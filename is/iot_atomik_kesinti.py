"""Z7: kontrollu overwrite kesintisi ve sonrasinda salt-okunur kontrol.

Kullanim: --bicim delta (veya parquet); ardindan --bicim delta --kontrol.
Once iot_atomik_hazirlik.py calistirilmalidir. Yalnizca onun benzersiz
deney kopyalarina yazar. Kaynak temiz tabloya veya ham verilere yazmaz.
Yavaslatma kesinti penceresi icindir; performans olcumu olarak kullanilmaz.
"""
import argparse
import csv
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

EVIDENCE = Path("/kit/kanit/olcumler")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def slow_partition(index, rows):
    # Ilk gorev normal hizda biter: gercek bir cikti dosyasi olusur.
    # Diger gorevler devam ederken kullanici yazmayi kesebilir.
    for number, row in enumerate(rows):
        if index != 0 and number % 1000 == 0:
            time.sleep(0.25)
        yield row


def load_preparation(path_arg):
    if path_arg:
        path = Path(path_arg)
    else:
        paths = sorted(EVIDENCE.glob("iot_atomik_hazirlik_*.json"))
        if not paths:
            raise RuntimeError("Once atomik hazirlik betigini calistirin.")
        path = paths[-1]
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not data.get("kopyalar_kaynakla_ayni"):
        raise RuntimeError("Hazirlik kontrolleri basarili degil.")
    for fmt, bucket in [("delta", "tablolar"), ("parquet", "islenmis")]:
        required = f"s3a://{bucket}/iot_z7_deney/atomiklik/{data['kosu']}/{fmt}"
        if data.get(fmt + "_yolu") != required:
            raise RuntimeError("Deney hedefi beklenen guvenli yol ile uyusmuyor.")
    return path, data


def data_files(spark, path):
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(jpath):
        return set()
    files = fs.listFiles(jpath, True)
    result = set()
    while files.hasNext():
        item = files.next()
        if item.getPath().getName().endswith(".parquet"):
            result.add(item.getPath().toString())
    return result


def source_data(spark, prep):
    return spark.read.format("delta").option(
        "versionAsOf", int(prep["kaynak_surumu"])
    ).load(prep["kaynak"])


def write_interrupted(spark, prep_path, prep, fmt, record_path):
    target = prep[fmt + "_yolu"]
    before_files = data_files(spark, target)
    if not before_files:
        raise RuntimeError("Baslangic kopyasinin dosyalari bulunamadi.")
    source = source_data(spark, prep)
    # Her satirin sicakligi +1000: eski/yeni/karisik veri ayirt edilebilir.
    # Bu sentetik degisiklik yalnizca deney hedeflerine yazilir.
    replacement = source.withColumn("sicaklik", F.col("sicaklik") + F.lit(1000.0))
    delayed = spark.createDataFrame(
        replacement.repartition(8).rdd.mapPartitionsWithIndex(slow_partition),
        schema=replacement.schema,
    )
    record = {
        "kosu": prep["kosu"], "bicim": fmt, "hedef": target,
        "hazirlik_dosyasi": str(prep_path), "baslangic": utc_now(),
        "python_pid": os.getpid(),
        "java_pid": int(spark._jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0]),
        "durum": "yaziliyor", "yeni_dosya_gozlendi": False,
        "not": "Overwrite; sicaklik +1000 sentetik degisim. 8 gorev, ilk gorev hizli, digerlerinde 1000 satirda 0.25 sn bekleme. Performans testi degil.",
    }
    # Tek deneme: onceki kesinti kanitini ustune yazarak kaybetme.
    with record_path.open("x", encoding="utf-8") as handle:
        json.dump(record, handle, ensure_ascii=False, indent=2)
    lock = threading.Lock()
    done = threading.Event()

    def update(**values):
        with lock:
            record.update(values)
            temp = record_path.with_suffix(".tmp")
            temp.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            temp.replace(record_path)

    def watch():
        while not done.wait(1):
            try:
                added = data_files(spark, target) - before_files
                if added and not done.is_set():
                    update(yeni_dosya_gozlendi=True, gozlem_zamani=utc_now(),
                           yeni_dosya_ornekleri=sorted(added)[:4])
                    print("\nYENI VERI DOSYASI OLUSTU - YAZMA SURUYOR.", flush=True)
                    print("SIMDI CTRL+C TUSLARINA BASIN.", flush=True)
                    return
            except Exception:
                # Overwrite sirasinda listelenen bir dosya kaldirilabilir.
                continue

    watcher = threading.Thread(target=watch, daemon=True)
    print("Bicim:", fmt, "Deney:", prep["kosu"], flush=True)
    print("Yazma basliyor. SIMDI CTRL+C mesajini bekleyin.", flush=True)
    watcher.start()
    try:
        writer = delayed.write.mode("overwrite").option("parquet.block.size", str(32 * 1024 * 1024))
        if fmt == "delta":
            writer.format("delta").save(target)
        else:
            writer.parquet(target)
        update(durum="tamamlandi", bitis=utc_now())
        print("YAZMA TAMAMLANDI; bu kosuda kesinti uygulanamadi. Kontrol bu durumu raporlayacak.", flush=True)
    except KeyboardInterrupt:
        update(durum="kullanici_kesti", bitis=utc_now())
        print("\nKullanici kesintisi kaydedildi. Spark kapatiliyor...", flush=True)
        raise
    except Exception as exc:
        update(durum="hata_ile_durdu", bitis=utc_now(), hata=str(exc)[:1500])
        raise
    finally:
        done.set()


def live_writer(record):
    live = []
    for key in ("python_pid", "java_pid"):
        pid = record.get(key)
        if not pid:
            continue
        try:
            command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", b" ").decode(errors="replace")
            if "iot_atomik_kesinti.py" in command and "--kontrol" not in command:
                live.append(pid)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    return live


def check(spark, prep, fmt, record_path):
    record = json.loads(record_path.read_text(encoding="utf-8"))
    live = live_writer(record)
    if live:
        raise RuntimeError(f"Yazici halen calisiyor, PID: {live}. Henuz sonuc degerlendirilmedi.")
    source = source_data(spark, prep)
    expected = int(prep["kaynak_kayit"])
    target = prep[fmt + "_yolu"]
    result = {
        "kosu": prep["kosu"], "bicim": fmt, "hedef": target,
        "beklenen_kayit": expected, "yazici_durumu": record["durum"],
        "yazma_sirasinda_yeni_dosya_gozlendi": record["yeni_dosya_gozlendi"],
        "kesinti_penceresi_dogrulandi": bool(record["yeni_dosya_gozlendi"] and record["durum"] != "tamamlandi"),
        "kontrol_zamani": utc_now(),
        "not": "Mantiksal tablo butunlugu kontrolu. Parquet dosyalarinin fiziksel olarak bozuk oldugunu varsaymaz. Tek yazici, yerel SeaweedFS deneyi; tum ariza turlerini kanitlamaz.",
    }
    try:
        actual = spark.read.format(fmt).load(target)
        count = actual.count()
        same_old = count == expected and not source.exceptAll(actual.select(*source.columns)).limit(1).count()
        same_new = count == expected and not source.withColumn(
            "sicaklik", F.col("sicaklik") + F.lit(1000.0)
        ).exceptAll(actual.select(*source.columns)).limit(1).count()
        result.update(okunabilir=True, okunan_kayit=count,
                      eski_icerik_korundu=bool(same_old), yeni_icerik_tam=bool(same_new))
    except Exception as exc:
        result.update(okunabilir=False, okunan_kayit=None,
                      eski_icerik_korundu=False, yeni_icerik_tam=False, okuma_hatasi=str(exc)[:2500])
    if fmt == "delta":
        history = spark.sql(f"DESCRIBE HISTORY delta.`{target}`")
        result["surumler"] = [int(r["version"]) for r in history.select("version").collect()]
        print("\nDelta gecmisi:", flush=True)
        history.select("version", "timestamp", "operation").show(truncate=False)
    path = EVIDENCE / f"iot_atomik_{prep['kosu']}_{fmt}_kontrol.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    with path.with_suffix(".csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["deney", "kapsam", "metrik", "tekrar", "deger", "birim", "kaynak", "aciklama"])
        writer.writerow(["atomik_yazma", fmt, "beklenen_kayit", 1, expected, "kayit", path.name, result["not"]])
        if result["okunan_kayit"] is not None:
            writer.writerow(["atomik_yazma", fmt, "okunan_kayit", 1, result["okunan_kayit"], "kayit", path.name, result["not"]])
        for name in ("okunabilir", "eski_icerik_korundu", "yeni_icerik_tam", "kesinti_penceresi_dogrulandi"):
            writer.writerow(["atomik_yazma", fmt, name, 1, int(result[name]), "0_hayir_1_evet", path.name, result["not"]])
    print("\nBICIM:", fmt, flush=True)
    for key in ("yazici_durumu", "kesinti_penceresi_dogrulandi", "beklenen_kayit", "okunabilir", "okunan_kayit", "eski_icerik_korundu", "yeni_icerik_tam"):
        print(key + ":", result[key], flush=True)
    print("Kanit:", path, "ve .csv", flush=True)
    if not result["kesinti_penceresi_dogrulandi"]:
        print("KESINTI DENEYI HENUZ DOGRULANMADI. Sonucu atomiklik kaniti saymayin.", flush=True)
    else:
        print("KESINTI SONRASI KONTROL TAMAMLANDI", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bicim", required=True, choices=["delta", "parquet"])
    parser.add_argument("--kontrol", action="store_true")
    parser.add_argument("--hazirlik", help="Hazirlik JSON yolu; verilmezse en yeni hazirlik secilir.")
    args = parser.parse_args()
    prep_path, prep = load_preparation(args.hazirlik)
    record_path = EVIDENCE / f"iot_atomik_{prep['kosu']}_{args.bicim}_yazma.json"
    if not args.kontrol and record_path.exists():
        raise RuntimeError("Bu kopyada yazma zaten denendi. Once --kontrol ile sonucu okuyun. Yeniden denemek icin yeni hazirlik gerekir.")
    spark = SparkSession.builder.appName("iot-atomik-" + args.bicim).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    try:
        if args.kontrol:
            check(spark, prep, args.bicim, record_path)
        else:
            write_interrupted(spark, prep_path, prep, args.bicim, record_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
