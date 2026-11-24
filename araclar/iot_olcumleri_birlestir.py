"""Bes tamamlanmis deneyin ozet CSV'lerini ortak olcumler.csv'ye ekler.

Mevcut satirlari (ve baytlari) degistirmez; ayni yeni satiri ikinci kez
eklemez. Ham 21.000 tekrar dosyasi yerine ozet CSV kullanilir. Herhangi
bir kaynak eksik veya bozuksa ortak tabloya hicbir sey yazilmaz.

Calistirma: docker compose exec araclar python araclar/iot_olcumleri_birlestir.py
"""
import csv
import hashlib
import io
import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

HEADERS = ["deney", "kapsam", "metrik", "tekrar", "deger", "birim", "kaynak", "aciklama"]
SOURCES = [
    "iot_delta_zaman_yolculugu_20261124T175718Z_36011672.csv",
    "iot_atomik_20261124T181936Z_3ca91060_delta_kontrol.csv",
    "iot_atomik_20261124T181936Z_3ca91060_parquet_kontrol.csv",
    "iot_mongo_aktarim_20261124T185013Z_a62ee8dc.csv",
    "iot_mongo_sorgu_20261124T185641Z_25024778.csv",
]


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read_csv(raw, name):
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig"), newline=""))
    if reader.fieldnames != HEADERS:
        raise RuntimeError("CSV basliklari beklenenden farkli: " + name)
    rows = list(reader)
    if any(None in row or any(value is None for value in row.values()) for row in rows):
        raise RuntimeError("Eksik/fazla sutunlu CSV: " + name)
    return rows


def main():
    root = Path(__file__).resolve().parents[1]
    common = root / "kanit" / "olcumler.csv"
    folder = root / "kanit" / "olcumler"
    original = common.read_bytes()
    old_rows = read_csv(original, str(common))
    # Eski tekrarlar ve metadata degerleri oldugu gibi korunur.
    seen = {tuple(row[h] for h in HEADERS) for row in old_rows}
    added = []
    audit_sources = []
    for name in SOURCES:
        path = folder / name
        raw = path.read_bytes()  # Eksik kaynakta mutasyondan once durur.
        rows = read_csv(raw, name)
        if not rows:
            raise RuntimeError("Bos ozet CSV: " + name)
        count = 0
        skipped = 0
        for row in rows:
            # Genel tablonun kaynagi, kanit/ klasorune gore gorelidir.
            source = Path(row["kaynak"].replace("\\", "/"))
            if source.is_absolute() or ".." in source.parts:
                raise RuntimeError("Beklenmeyen kaynak yolu: " + row["kaynak"])
            if len(source.parts) == 1:
                source = Path("olcumler") / source
            if not (root / "kanit" / source).is_file():
                raise RuntimeError("Kanitta gosterilen kaynak bulunamadi: " + str(source))
            row["kaynak"] = source.as_posix()
            try:
                if not Decimal(row["deger"]).is_finite():
                    raise InvalidOperation()
            except (InvalidOperation, ValueError):
                raise RuntimeError("Sayisal olmayan olcum: " + name + " / " + row["metrik"])
            if not row["deney"] or not row["metrik"]:
                raise RuntimeError("Eksik deney/metrik alani: " + name)
            identity = tuple(row[h] for h in HEADERS)
            if identity in seen:
                skipped += 1
                continue
            seen.add(identity)
            added.append(row)
            count += 1
        audit_sources.append({"dosya": "olcumler/" + name, "sha256": sha(raw),
                              "kaynak_satir": len(rows), "eklenen": count, "zaten_var": skipped})

    if not added:
        print("Olcumlerin hepsi ortak tabloda zaten var. Dosya degistirilmedi.")
        print("Ortak olcum satiri:", len(old_rows))
        return
    newline = "\r\n" if b"\r\n" in original else "\n"
    text = io.StringIO(newline="")
    writer = csv.DictWriter(text, fieldnames=HEADERS, lineterminator=newline)
    writer.writerows(added)
    # Ilk bolum, baslik ve eski satirlar bayt bayt ayni kalir.
    separator = b"" if original.endswith((b"\r", b"\n")) else newline.encode("ascii")
    merged = original + separator + text.getvalue().encode("utf-8")
    verified = read_csv(merged, "birlestirilmis tablo")
    if verified[:len(old_rows)] != old_rows or len(verified) != len(old_rows) + len(added):
        raise RuntimeError("Koruma kontrolu basarisiz; ortak tablo degistirilmedi.")
    if common.read_bytes() != original:
        raise RuntimeError("Islem sirasinda ortak tablo baska bir islemle degisti; yeniden deneyin.")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    temp = common.with_name("olcumler.csv." + stamp + ".tmp")
    try:
        with temp.open("xb") as handle:
            handle.write(merged)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, common)
    finally:
        if temp.exists():
            temp.unlink()
    audit = {
        "islem": "ortak_olcum_tablosuna_ekleme", "kosu": stamp,
        "onceki_satir": len(old_rows), "eklenen_satir": len(added), "son_satir": len(verified),
        "eski_satirlar_aynen_korundu": True,
        "onceki_sha256": sha(original), "son_sha256": sha(merged),
        "kaynaklar": audit_sources,
        "betik_sha256": sha(Path(__file__).read_bytes()),
        "not": "Olcum yapilmadi; mevcut bes deneyin ozetleri birlestirildi. Eski satirlar degistirilmedi. Yeni kaynak yollari kanit/ klasorune gore normalize edildi.",
    }
    audit_path = folder / ("iot_ortak_tablo_birlestirme_" + stamp + ".json")
    audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    for entry in audit_sources:
        print(entry["dosya"], "=> eklenen:", entry["eklenen"], "zaten var:", entry["zaten_var"])
    print("Onceki satir:", len(old_rows))
    print("Eklenen satir:", len(added))
    print("Toplam satir:", len(verified))
    print("Eski satirlar aynen korundu: BASARILI")
    print("Ortak tablo:", common)
    print("Birlestirme kaydi:", audit_path)
    print("ORTAK OLCUM TABLOSU GUNCELLENDI")


if __name__ == "__main__":
    main()
