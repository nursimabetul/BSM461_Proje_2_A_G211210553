"""Z6: yerel servis sorgularinin istemci tarafinda sicak onbellek olcumu.

Salt okuma. Varsayilan: 7 sorgu x 3 tur x 1000 olcum; her turde sorgu
basina 20 isinma okuması. Tek istemci, ardisik ve donusumlu sorgular.
P99: nearest-rank, ceil(0.99*n). Soguk onbellek veya uretim yuk testi degil.
"""
import argparse
import csv
import hashlib
import json
import math
import statistics
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import pymongo
from pymongo import MongoClient


def build_queries(run_id, region):
    return [
        {"ad": "aktif_kosu", "koleksiyon": "servis_meta", "filtre": {"_id": "aktif_toplu"},
         "alanlar": {"_id": 0, "kosu_id": 1, "veri_baslangici": 1, "veri_bitisi": 1,
                      "uretim_zamani": 1, "yayin_zamani": 1}, "sirala": [], "limit": 1},
        {"ad": "model_anomali", "koleksiyon": "model_anomali", "filtre": {"kosu_id": run_id},
         "alanlar": {"_id": 0, "model": 1, "toplam_kayit": 1, "toplam_anomali": 1,
                      "anomali_orani": 1, "sicaklik_anomali_sayisi": 1, "titresim_anomali_sayisi": 1},
         "sirala": [("anomali_orani", -1), ("model", 1)]},
        {"ad": "model_sensor_ozet", "koleksiyon": "model_sensor_ozet", "filtre": {"kosu_id": run_id},
         "alanlar": {"_id": 0, "model": 1, "sensor_sayisi": 1, "ortalama_sensor_anomali_orani": 1},
         "sirala": [("model", 1)]},
        {"ad": "yil_sensor_ozet", "koleksiyon": "yil_sensor_ozet", "filtre": {"kosu_id": run_id},
         "alanlar": {"_id": 0, "kurulum_yili": 1, "sensor_sayisi": 1, "ortalama_sensor_anomali_orani": 1},
         "sirala": [("kurulum_yili", 1)]},
        {"ad": "secili_bolge_saatlik", "koleksiyon": "bolge_saatlik", "filtre": {"kosu_id": run_id, "bolge": region},
         "alanlar": {"_id": 0, "bolge": 1, "saat": 1, "ortalama_sicaklik": 1, "ortalama_nem": 1, "olay_sayisi": 1},
         "sirala": [("saat", 1)]},
        {"ad": "tum_bolgeler_saatlik", "koleksiyon": "bolge_saatlik", "filtre": {"kosu_id": run_id},
         "alanlar": {"_id": 0, "bolge": 1, "saat": 1, "ortalama_sicaklik": 1, "ortalama_nem": 1, "olay_sayisi": 1},
         "sirala": [("bolge", 1), ("saat", 1)]},
        {"ad": "gece_gunduz", "koleksiyon": "bolge_gece_gunduz", "filtre": {"kosu_id": run_id},
         "alanlar": {"_id": 0, "bolge": 1, "gunduz_ort_sicaklik": 1, "gece_ort_sicaklik": 1,
                      "sicaklik_farki": 1, "gunduz_ort_nem": 1, "gece_ort_nem": 1, "nem_farki": 1},
         "sirala": [("bolge", 1)]},
    ]


def read_query(db, query):
    cursor = db[query["koleksiyon"]].find(query["filtre"], query["alanlar"])
    if query["sirala"]:
        cursor = cursor.sort(query["sirala"])
    if query.get("limit"):
        cursor = cursor.limit(query["limit"])
    cursor = cursor.max_time_ms(10000)
    try:
        return list(cursor)  # Ilk paket degil, sonuclarin tamami olculur.
    finally:
        cursor.close()


def distribution(values, target):
    ordered = sorted(values)
    if not ordered:
        return {"ornek": 0, "hedef_saglandi": False}
    def rank(p):
        return ordered[max(0, math.ceil(p * len(ordered)) - 1)]
    return {"ornek": len(ordered), "ortalama_ms": statistics.mean(ordered),
            "min_ms": ordered[0], "maks_ms": ordered[-1], "p50_ms": rank(.5),
            "p95_ms": rank(.95), "p99_ms": rank(.99),
            "hedef_ustu_istek": sum(v > target for v in ordered),
            "hedef_saglandi": rank(.99) <= target}


def explain(db, query):
    command = {"find": query["koleksiyon"], "filter": query["filtre"], "projection": query["alanlar"]}
    if query["sirala"]:
        command["sort"] = dict(query["sirala"])
    if query.get("limit"):
        command["limit"] = query["limit"]
    return db.command("explain", command, verbosity="executionStats")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tekrar", type=int, default=1000, help="Tur ve sorgu basina zamanli tekrar")
    parser.add_argument("--tur", type=int, default=3)
    parser.add_argument("--isinma", type=int, default=20)
    args = parser.parse_args()
    if args.tekrar < 100 or args.tur < 1 or args.isinma < 0:
        raise RuntimeError("En az 100 olcum, en az 1 tur; isinma negatif olamaz.")
    target = 50.0  # G211210553 senaryo kartindaki servis p99 hedefi; olculen sonuc degil.
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    client = MongoClient("mongodb://mongo:27017", serverSelectionTimeoutMS=10000,
                         socketTimeoutMS=15000, tz_aware=True)
    evidence = Path("/kit/kanit/olcumler")
    evidence.mkdir(parents=True, exist_ok=True)
    stem = evidence / ("iot_mongo_sorgu_" + run_id)
    try:
        client.admin.command("ping")
        db = client["bsm461_iot"]
        meta = db.servis_meta.find_one({"_id": "aktif_toplu"})
        if not meta or meta.get("durum") != "tamamlandi":
            raise RuntimeError("Tamamlanmis aktif toplu kosu yok.")
        if db.servis_meta.find_one({"_id": "aktarim_kilidi"}):
            raise RuntimeError("Bir aktarim calisiyor veya kilidi kalmis; olcum baslatilmadi.")
        source_run = meta["kosu_id"]
        region_row = db.bolge_saatlik.find_one({"kosu_id": source_run}, sort=[("bolge", 1)])
        if not region_row:
            raise RuntimeError("Bolge ozetleri bulunamadi.")
        queries = build_queries(source_run, region_row["bolge"])
        expected = {q["ad"]: read_query(db, q) for q in queries}
        for q in queries:
            count = len(expected[q["ad"]])
            if count == 0:
                raise RuntimeError("Bos sorgu sonucu: " + q["ad"])
            if q["ad"] not in ("aktif_kosu", "secili_bolge_saatlik"):
                if count != meta["sonuc_sayilari"][q["koleksiyon"]]:
                    raise RuntimeError("Aktarim metaverisiyle sayim farkli: " + q["ad"])
        print("Aktif toplu kosu:", source_run, flush=True)
        print("Hedef p99 <=", target, "ms", flush=True)
        print(f"Tek istemci, sicak onbellek; {args.tur} tur x {args.tekrar} tekrar/sorgu.", flush=True)
        print("Secili bolge:", region_row["bolge"], flush=True)
        times = {q["ad"]: [] for q in queries}
        rounds = []
        failed = []
        samples_path = stem.with_name(stem.name + "_tekrarlar.csv")
        with samples_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["olcum_kosu", "toplu_kosu", "sorgu", "tur", "tekrar", "sure_ms", "sonuc_belge", "hata"])
            for r in range(1, args.tur + 1):
                for q in queries:
                    for _ in range(args.isinma):
                        if read_query(db, q) != expected[q["ad"]]:
                            raise RuntimeError("Isinmada icerik degisti: " + q["ad"])
                current = {q["ad"]: [] for q in queries}
                # Her sorguyu ardisik blokta olcmek yerine sira dondurulur.
                for i in range(args.tekrar):
                    offset = (i + r - 1) % len(queries)
                    order = queries[offset:] + queries[:offset]
                    for q in order:
                        start = time.perf_counter_ns()
                        try:
                            docs = read_query(db, q)
                            elapsed = (time.perf_counter_ns() - start) / 1e6
                            error = "" if docs == expected[q["ad"]] else "SONUC_ICERIGI_DEGISTI"
                        except Exception as exc:
                            elapsed = (time.perf_counter_ns() - start) / 1e6
                            docs = []
                            error = str(exc)[:800]
                        writer.writerow([run_id, source_run, q["ad"], r, i + 1, elapsed, len(docs), error])
                        if error:
                            failed.append({"sorgu": q["ad"], "tur": r, "tekrar": i + 1, "hata": error})
                            raise RuntimeError("Olcumde hata/tutarsizlik. Ham tekrar CSV korunuyor: " + error)
                        current[q["ad"]].append(elapsed)
                        times[q["ad"]].append(elapsed)
                for q in queries:
                    stats = distribution(current[q["ad"]], target)
                    rounds.append(dict(stats, sorgu=q["ad"], tur=r))
                print(f"Tur {r}/{args.tur} tamamlandi.", flush=True)
        after = db.servis_meta.find_one({"_id": "aktif_toplu"})
        if after != meta or db.servis_meta.find_one({"_id": "aktarim_kilidi"}):
            raise RuntimeError("Olcum sirasinda aktif kaynak/aktarim durumu degisti; bu kosuyu hedef kaniti saymayin.")
        combined = [dict(distribution(times[q["ad"]], target), sorgu=q["ad"],
                         sonuc_belge=len(expected[q["ad"]])) for q in queries]
        plans = {}
        for q in queries:
            plans[q["ad"]] = explain(db, q)
        plans_path = stem.with_name(stem.name + "_explain.json")
        plans_path.write_text(json.dumps(plans, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        result = {
            "deney": "iot_mongo_sorgu", "kosu_id": run_id, "toplu_kosu": source_run,
            "kaynak_surumu": meta["kaynak_surumu"], "veritabani": db.name,
            "mongo_surumu": client.server_info()["version"], "pymongo_surumu": pymongo.version,
            "olcum_betigi_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "kosullar": {"istemci": 1, "eszamanli_istek": 1, "tur": args.tur,
                         "tur_basina_sorgu_tekrari": args.tekrar, "tur_basina_isinma": args.isinma,
                         "onbellek": "sicak; OS/MongoDB cache temizlenmedi",
                         "olcum": "perf_counter_ns; sorgu olusturma, ag, sunucu, tum cursor sonuclarini alma ve BSON cozme dahil; dogruluk karsilastirmasi zaman araligi disinda",
                         "p99_yontemi": "nearest-rank: sirali surelerde ceil(0.99*N)",
                         "hedef_p99_ms": target, "secili_bolge": region_row["bolge"]},
            "sorgular": queries, "turlar": rounds, "birlesik": combined,
            "hata_sayisi": len(failed), "aktif_kosu_degismedi": True,
            "hedef_tum_turlarda_saglandi": all(row["hedef_saglandi"] for row in rounds),
            "hedef_birlesik_saglandi": all(row["hedef_saglandi"] for row in combined),
            "ham_tekrarlar": samples_path.name, "explain": plans_path.name,
            "not": "Kucuk toplu ozetlerde tek istemciyle yerel okuma testi. Pano render suresi ve uretim veri girisi dahil degil. 60.000/480.000 olay/sn kapasitesi, cok istemci, cold-cache veya uretim SLA garantisi kanitlanmaz. Explain sureleri istemci p99'u yerine kullanilmaz.",
        }
        stem.with_suffix(".json").write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        with stem.with_suffix(".csv").open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["deney", "kapsam", "metrik", "tekrar", "deger", "birim", "kaynak", "aciklama"])
            for scope, rows in [("birlesik", combined), ("tur", rounds)]:
                for row in rows:
                    context = f"{row['sorgu']}/{scope}" + (str(row["tur"]) if "tur" in row else "")
                    for metric in ("ortalama_ms", "min_ms", "maks_ms", "p50_ms", "p95_ms", "p99_ms"):
                        writer.writerow(["mongo_sorgu", context, metric, "turetilmis", row[metric], "ms",
                                         samples_path.name, f"{row['ornek']} zamanli tekrar; {args.isinma} isinma/tur. {result['not']}"])
        print("\nMONGODB SORGU OLCUM SONUCLARI", flush=True)
        print("Sorgu | belge | olcum | p99 ms | hedef <= 50 ms", flush=True)
        for row in combined:
            print(f"{row['sorgu']} | {row['sonuc_belge']} | {row['ornek']} | {row['p99_ms']:.4f} | " +
                  ("SAGLANDI" if row["hedef_saglandi"] else "SAGLANMADI"), flush=True)
        print("Tum turlarda hedef:", result["hedef_tum_turlarda_saglandi"], flush=True)
        print("Hata sayisi:", result["hata_sayisi"], flush=True)
        print("Kanit:", str(stem) + ".json ve .csv", flush=True)
        print("Ham tekrarlar:", samples_path, flush=True)
        print("Sorgu planlari:", plans_path, flush=True)
        print("MONGODB SORGU OLCUMU TAMAMLANDI", flush=True)
    finally:
        client.close()


if __name__ == "__main__":
    main()
