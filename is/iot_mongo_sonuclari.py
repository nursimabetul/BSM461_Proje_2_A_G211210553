"""Temiz Delta'dan ekibin bes toplu ozetini cikartir; MongoDB'ye yazmaz.

Calistirma: spark-submit is/iot_mongo_sonuclari.py
Kaynak tablolar degismez. Cikti, araclar/iot_mongo_aktar.py girdisidir.
"""
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

EXPECTED_BATCH_HASH = "4f25b1f43f91525fc20f656dfb9401d6f97fe391ac793f851ee5d22dedf0b466"
KEYS = {
    "model_anomali": ["model"],
    "model_sensor_ozet": ["model"],
    "yil_sensor_ozet": ["kurulum_yili"],
    "bolge_saatlik": ["bolge", "saat"],
    "bolge_gece_gunduz": ["bolge"],
}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def main():
    batch_path = Path("/kit/is/toplu_is.py")
    batch_code = batch_path.read_text(encoding="utf-8-sig").replace("\r\n", "\n")
    batch_hash = hashlib.sha256(batch_code.encode("utf-8")).hexdigest()
    if batch_hash != EXPECTED_BATCH_HASH:
        raise RuntimeError("toplu_is.py incelenen surumden farkli. Analiz mantigini tekrar karsilastirmadan aktarim yapilmadi.")
    thresholds = {}
    for name in ("SICAKLIK_ESIGI", "TITRESIM_ESIGI"):
        match = re.search(r"^\s*" + name + r"\s*=\s*([0-9.]+)\s*$", batch_code, re.MULTILINE)
        if not match:
            raise RuntimeError("Toplu is esigi okunamadi: " + name)
        thresholds[name] = float(match.group(1))
    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    # MongoDB BSON tarihleri milisaniye hassasiyetindedir.
    produced = now.isoformat(timespec="milliseconds")
    spark = SparkSession.builder.appName("iot-mongo-toplu-sonuclar").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    joined = None
    try:
        path = "s3a://tablolar/iot"
        version = int(spark.sql(f"DESCRIBE HISTORY delta.`{path}`").agg(F.max("version")).first()[0])
        clean = spark.read.format("delta").option("versionAsOf", version).load(path)
        info = clean.agg(F.count("*").alias("n"), F.min("tarih").alias("ilk"), F.max("tarih").alias("son")).first()
        if not info["n"]:
            raise RuntimeError("Temiz kaynak tablo bos.")
        dimension_path = "s3a://ham/boyut/iot/sensorler.csv"
        dim = spark.read.option("header", True).option("inferSchema", True).csv(dimension_path).select(
            "sensor_id", "model", "kurulum_yili", F.col("bolge").alias("sensor_bolge")
        )
        dim_rows = [r.asDict() for r in dim.orderBy("sensor_id").collect()]
        ids = [r["sensor_id"] for r in dim_rows]
        if len(ids) != len(set(ids)) or not ids or any(any(v is None for v in r.values()) for r in dim_rows):
            raise RuntimeError("Boyut tablosu bos, eksik veya sensor_id tekrari var.")
        dim_hash = hashlib.sha256(canonical(dim_rows).encode("utf-8")).hexdigest()
        joined = clean.join(dim, on="sensor_id", how="inner").withColumn(
            "sicaklik_anomali", F.when(F.col("sicaklik") > thresholds["SICAKLIK_ESIGI"], 1).otherwise(0)
        ).withColumn(
            "titresim_anomali", F.when(F.col("titresim") > thresholds["TITRESIM_ESIGI"], 1).otherwise(0)
        ).withColumn(
            "anomali", F.when((F.col("sicaklik_anomali") == 1) | (F.col("titresim_anomali") == 1), 1).otherwise(0)
        ).persist(StorageLevel.MEMORY_AND_DISK)
        joined_count = joined.count()
        if joined_count != info["n"]:
            raise RuntimeError("Sensor boyut join'i kayit kaybettirdi/artirdi; aktarim durduruldu.")
        print("Temiz kaynak:", info["n"], "Join sonrasi:", joined_count, flush=True)
        model = joined.groupBy("model").agg(
            F.count("*").alias("toplam_kayit"),
            F.sum("sicaklik_anomali").alias("sicaklik_anomali_sayisi"),
            F.sum("titresim_anomali").alias("titresim_anomali_sayisi"),
            F.sum("anomali").alias("toplam_anomali"),
        ).withColumn("anomali_orani", F.round(100 * F.col("toplam_anomali") / F.col("toplam_kayit"), 2))
        sensors = joined.groupBy("sensor_id", "model", "kurulum_yili").agg(
            F.count("*").alias("sensor_toplam_kayit"), F.sum("anomali").alias("sensor_anomali_sayisi")
        ).withColumn("sensor_anomali_orani", 100 * F.col("sensor_anomali_sayisi") / F.col("sensor_toplam_kayit"))
        model_sensor = sensors.groupBy("model").agg(
            F.countDistinct("sensor_id").alias("sensor_sayisi"),
            F.round(F.avg("sensor_anomali_orani"), 2).alias("ortalama_sensor_anomali_orani"),
        )
        year_sensor = sensors.groupBy("kurulum_yili").agg(
            F.countDistinct("sensor_id").alias("sensor_sayisi"),
            F.round(F.avg("sensor_anomali_orani"), 2).alias("ortalama_sensor_anomali_orani"),
        )
        hourly = joined.select(F.col("sensor_bolge").alias("bolge"), F.hour("olay_zamani").alias("saat"), "sicaklik", "nem")
        hour_summary = hourly.groupBy("bolge", "saat").agg(
            F.round(F.avg("sicaklik"), 2).alias("ortalama_sicaklik"),
            F.round(F.avg("nem"), 2).alias("ortalama_nem"), F.count("*").alias("olay_sayisi"),
        )
        day = hourly.filter((F.col("saat") >= 6) & (F.col("saat") < 18)).groupBy("bolge").agg(
            F.avg("sicaklik").alias("gunduz_sicaklik"), F.avg("nem").alias("gunduz_nem")
        )
        night = hourly.filter((F.col("saat") < 6) | (F.col("saat") >= 18)).groupBy("bolge").agg(
            F.avg("sicaklik").alias("gece_sicaklik"), F.avg("nem").alias("gece_nem")
        )
        periods = day.join(night, "bolge", "inner").select(
            "bolge", F.round("gunduz_sicaklik", 2).alias("gunduz_ort_sicaklik"),
            F.round("gece_sicaklik", 2).alias("gece_ort_sicaklik"),
            F.round(F.abs(F.col("gunduz_sicaklik") - F.col("gece_sicaklik")), 2).alias("sicaklik_farki"),
            F.round("gunduz_nem", 2).alias("gunduz_ort_nem"),
            F.round("gece_nem", 2).alias("gece_ort_nem"),
            F.round(F.abs(F.col("gunduz_nem") - F.col("gece_nem")), 2).alias("nem_farki"),
        )
        frames = {"model_anomali": model, "model_sensor_ozet": model_sensor,
                  "yil_sensor_ozet": year_sensor, "bolge_saatlik": hour_summary, "bolge_gece_gunduz": periods}
        collections = {}
        for name, frame in frames.items():
            rows = [r.asDict() for r in frame.orderBy(*KEYS[name]).collect()]
            if not rows:
                raise RuntimeError("Bos analiz sonucu: " + name)
            docs = []
            for row in rows:
                natural = [row[k] for k in KEYS[name]]
                doc_id = hashlib.sha256(canonical([run_id, name, natural]).encode("utf-8")).hexdigest()
                docs.append(dict(row, _id=doc_id, kosu_id=run_id, kaynak_surumu=version, uretim_zamani=produced))
            collections[name] = docs
            print(name + ":", len(docs), "sonuc", flush=True)
        if sum(d["toplam_kayit"] for d in collections["model_anomali"]) != joined_count:
            raise RuntimeError("Model ozetindeki toplam kaynak sayisina esit degil.")
        if sum(d["olay_sayisi"] for d in collections["bolge_saatlik"]) != joined_count:
            raise RuntimeError("Saatlik ozetteki toplam kaynak sayisina esit degil.")
        data = {
            "sema_surumu": 1, "veritabani": "bsm461_iot", "kosu_id": run_id,
            "uretim_zamani": produced, "kaynak": path, "kaynak_surumu": version,
            "veri_baslangici": str(info["ilk"]), "veri_bitisi": str(info["son"]),
            "kaynak_kayit": int(info["n"]), "join_kayit": joined_count,
            "boyut_kayit": len(dim_rows), "boyut_yolu": dimension_path,
            "boyut_semantik_sha256": dim_hash, "toplu_is_sha256": batch_hash,
            "aktarima_hazirlayan_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "spark_surumu": spark.version, "saat_dilimi": "UTC", "esikler": thresholds,
            "dogal_anahtarlar": KEYS, "koleksiyonlar": collections,
            "not": "Temiz ana Delta surumunden toplu ozetler. Ham veri veya Z7 sentetik deney verisi kullanilmadi. Henuz MongoDB'ye yazilmadi.",
        }
        dest = Path("/kit/kanit/olcumler") / ("iot_mongo_toplu_" + run_id + ".json")
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("x", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2, allow_nan=False)
        print("Toplam ozet belge:", sum(len(v) for v in collections.values()), flush=True)
        print("Cikti:", dest, flush=True)
        print("MONGODB ICIN TOPLU SONUCLAR HAZIR - HENUZ AKTARILMADI", flush=True)
    finally:
        if joined is not None:
            joined.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
