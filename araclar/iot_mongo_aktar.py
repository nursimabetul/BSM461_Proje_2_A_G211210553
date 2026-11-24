"""Hazir toplu ozetleri MongoDB'ye yazar; ayni icerigi ikinci kez yazarak
belge sayisinin artmadigini ve tum alanlarin ayni kaldigini kontrol eder.

Calistirma: docker compose exec araclar python araclar/iot_mongo_aktar.py
Girdi verilmezse en yeni iot_mongo_toplu_*.json secilir. Ham veriye yazmaz.
"""
import argparse
import copy
import csv
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
import pymongo
from pymongo import MongoClient, ReplaceOne
from pymongo.write_concern import WriteConcern

KEYS = {"model_anomali": ["model"], "model_sensor_ozet": ["model"],
        "yil_sensor_ozet": ["kurulum_yili"], "bolge_saatlik": ["bolge", "saat"],
        "bolge_gece_gunduz": ["bolge"]}
EVIDENCE = Path("/kit/kanit/olcumler")


def normalize_docs(docs):
    out = copy.deepcopy(docs)
    for doc in out:
        doc["uretim_zamani"] = datetime.fromisoformat(doc["uretim_zamani"])
    return out


def verify(collection, run_id, expected):
    found = list(collection.find({"kosu_id": run_id}))
    if sorted(found, key=lambda x: x["_id"]) != sorted(expected, key=lambda x: x["_id"]):
        raise RuntimeError("MongoDB kayitlari beklenen icerikle ayni degil: " + collection.name)
    return len(found)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--girdi")
    args = parser.parse_args()
    if args.girdi:
        path = Path(args.girdi)
    else:
        paths = sorted(EVIDENCE.glob("iot_mongo_toplu_*.json"))
        if not paths:
            raise RuntimeError("Once Spark ile iot_mongo_sonuclari.py calistirilmali.")
        path = paths[-1]
    raw = path.read_bytes()
    data = json.loads(raw.decode("utf-8-sig"))
    if data.get("veritabani") != "bsm461_iot" or data.get("sema_surumu") != 1:
        raise RuntimeError("Beklenmeyen veritabani veya girdi semasi.")
    if data.get("dogal_anahtarlar") != KEYS or set(data.get("koleksiyonlar", {})) != set(KEYS):
        raise RuntimeError("Beklenmeyen koleksiyon/anahtarlar.")
    run_id = data["kosu_id"]
    payload_hash = hashlib.sha256(raw).hexdigest()
    docs_by_name = {name: normalize_docs(rows) for name, rows in data["koleksiyonlar"].items()}
    for name, docs in docs_by_name.items():
        if not docs or len({d["_id"] for d in docs}) != len(docs):
            raise RuntimeError("Bos veya tekrar anahtarli sonuc: " + name)
        for doc in docs:
            natural = [doc[k] for k in KEYS[name]]
            identity = json.dumps([run_id, name, natural], ensure_ascii=False, sort_keys=True,
                                  separators=(",", ":"), allow_nan=False)
            if doc["kosu_id"] != run_id or doc["_id"] != hashlib.sha256(identity.encode()).hexdigest():
                raise RuntimeError("Girdi kimligi tutarsiz.")
    if data["kaynak_kayit"] != data["join_kayit"]:
        raise RuntimeError("Kaynak/join sayimlari tutarsiz.")
    # Yerel standalone: w=1 ve journal onayi; replica/quorum garantisi DEGIL.
    client = MongoClient("mongodb://mongo:27017", serverSelectionTimeoutMS=10000, tz_aware=True)
    db = client.get_database("bsm461_iot", write_concern=WriteConcern(w=1, j=True))
    lock_id = uuid.uuid4().hex
    acquired = False
    try:
        client.admin.command("ping")
        server_version = client.server_info()["version"]
        # Birden fazla aktaricinin aktif kosu isaretcisini yaristirmasini onle.
        # Sert kapanmada kalan kilit kendiliginden silinmez; once kontrol gerekir.
        db.servis_meta.insert_one({"_id": "aktarim_kilidi", "sahip": lock_id,
                                   "baslangic": datetime.now(timezone.utc)})
        acquired = True
        prior_run = db.servis_meta.find_one({"_id": "kosu:" + run_id})
        if prior_run and prior_run.get("girdi_sha256") != payload_hash:
            raise RuntimeError("Ayni kosu kimligi farkli bir icerikle kullanilamaz.")
        results = []
        for name, docs in docs_by_name.items():
            collection = db[name]
            natural_index = [("kosu_id", 1)] + [(key, 1) for key in KEYS[name]]
            collection.create_index(natural_index, name="kosu_dogal_anahtar", unique=True)
            if name == "model_anomali":
                collection.create_index([("kosu_id", 1), ("anomali_orani", -1), ("model", 1)], name="kosu_anomali_sira")
            before = collection.count_documents({"kosu_id": run_id})
            collection.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs], ordered=False)
            first = verify(collection, run_id, docs)
            # Gercek ikinci aktarim: sadece formulu veya kodu degil sonucu sinar.
            again = collection.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs], ordered=False)
            second = verify(collection, run_id, docs)
            if first != second or again.upserted_count != 0:
                raise RuntimeError("Tekrar aktarimda yeni belge olustu: " + name)
            result = {"koleksiyon": name, "beklenen": len(docs), "once": before,
                      "ilk_aktarim": first, "tekrar_aktarim": second,
                      "ikinci_aktarim_yeni_belge": again.upserted_count,
                      "icerik_ayni": True, "indeksler": list(collection.list_indexes())}
            results.append(result)
            print(f"{name}: {first} belge | tekrar: {second} | icerik: AYNI", flush=True)
        # Yayinlamadan once ozetlerin kaynak sayimina denk geldigini de kontrol et.
        if sum(d["toplam_kayit"] for d in docs_by_name["model_anomali"]) != data["join_kayit"]:
            raise RuntimeError("Model toplami kaynakla tutarsiz.")
        if sum(d["olay_sayisi"] for d in docs_by_name["bolge_saatlik"]) != data["join_kayit"]:
            raise RuntimeError("Saatlik toplam kaynakla tutarsiz.")
        published = datetime.now(timezone.utc)
        meta = {key: data[key] for key in (
            "kosu_id", "kaynak", "kaynak_surumu", "kaynak_kayit", "join_kayit",
            "veri_baslangici", "veri_bitisi", "uretim_zamani", "boyut_kayit",
            "boyut_semantik_sha256", "toplu_is_sha256", "saat_dilimi", "esikler")}
        meta.update(girdi_sha256=payload_hash, durum="tamamlandi",
                    sonuc_sayilari={r["koleksiyon"]: r["ilk_aktarim"] for r in results},
                    yayin_zamani=published)
        db.servis_meta.replace_one({"_id": "kosu:" + run_id}, dict(meta, _id="kosu:" + run_id), upsert=True)
        # Tek belge ile yayinlama; butun koleksiyonlari kapsayan transaction degil.
        db.servis_meta.replace_one({"_id": "aktif_toplu"}, dict(meta, _id="aktif_toplu"), upsert=True)
        if db.servis_meta.find_one({"_id": "aktif_toplu"})["kosu_id"] != run_id:
            raise RuntimeError("Aktif kosu dogrulanamadi.")
        output = {"deney": "iot_mongo_aktarim", "kosu_id": run_id, "veritabani": db.name,
                  "mongo_surumu": server_version, "pymongo_surumu": pymongo.version,
                  "girdi": path.name, "girdi_sha256": payload_hash,
                  "aktarici_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  "kaynak_kayit": data["kaynak_kayit"], "kaynak_surumu": data["kaynak_surumu"],
                  "koleksiyonlar": results, "aktif_kosu_dogrulandi": True,
                  "toplam_sonuc_belge": sum(r["ilk_aktarim"] for r in results),
                  "kontrol_zamani": published.isoformat(),
                  "not": "Kucuk toplu ozetler; ham veriler tasinmadi. Ayni kosu/icerik iki kez gercekten yazildi ve geri okundu. Gecikme veya yuk testi degil. Yerel standalone; w=1,j=True. Uc uca exactly-once iddiasi yok."}
        out_path = EVIDENCE / ("iot_mongo_aktarim_" + run_id + ".json")
        out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        with out_path.with_suffix(".csv").open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["deney", "kapsam", "metrik", "tekrar", "deger", "birim", "kaynak", "aciklama"])
            for row in results:
                for repeat, field in [(1, "ilk_aktarim"), (2, "tekrar_aktarim")]:
                    writer.writerow(["mongo_aktarim", row["koleksiyon"], "belge_sayisi", repeat,
                                     row[field], "belge", out_path.name, output["not"]])
        print("Aktif kosu:", run_id, flush=True)
        print("Toplam sonuc belge:", output["toplam_sonuc_belge"], flush=True)
        print("Ikinci aktarimda belge artisi: YOK", flush=True)
        print("Tum alanlar geri okunarak dogrulandi: BASARILI", flush=True)
        print("Kanit:", out_path, "ve .csv", flush=True)
        print("MONGODB TOPLU AKTARIMI TAMAMLANDI", flush=True)
    finally:
        if acquired:
            db.servis_meta.delete_one({"_id": "aktarim_kilidi", "sahip": lock_id})
        client.close()


if __name__ == "__main__":
    main()
