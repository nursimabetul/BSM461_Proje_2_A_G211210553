"""Z7 kesinti deneyinin baslangic kopyalari. Kaynak tabloya yazmaz.

Bu asama kesinti testi DEGILDIR; iki ayri hedefin saglam baslangicini
olusturur, sayim ve satir coklugu esitligini dogrular.
"""
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F


def verify_copy(source, target, expected):
    actual = target.count()
    if actual != expected:
        raise RuntimeError(f"Kopya sayimi farkli: {actual} != {expected}")
    # Esit sayim + EXCEPT ALL boslugu, tekrar/null dahil cokluk esitligi.
    if source.exceptAll(target.select(*source.columns)).limit(1).count():
        raise RuntimeError("Kopya icerigi kaynakla ayni degil.")
    return actual


def main():
    spark = SparkSession.builder.appName("iot-atomik-hazirlik").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    source_path = "s3a://tablolar/iot"
    delta_path = "s3a://tablolar/iot_z7_deney/atomiklik/" + run_id + "/delta"
    parquet_path = "s3a://islenmis/iot_z7_deney/atomiklik/" + run_id + "/parquet"
    evidence = Path("/kit/kanit/olcumler")
    evidence.mkdir(parents=True, exist_ok=True)
    try:
        version = int(spark.sql(f"DESCRIBE HISTORY delta.`{source_path}`").agg(
            F.max("version")
        ).first()[0])
        source = spark.read.format("delta").option("versionAsOf", version).load(source_path)
        expected = source.count()
        if expected < 1000000:
            raise RuntimeError("Bu deney icin kaynakta en az 1 milyon satir bekleniyor.")
        print("Kaynak surumu:", version, "kayit:", expected, flush=True)
        print("Delta deney yolu:", delta_path, flush=True)
        print("Parquet deney yolu:", parquet_path, flush=True)
        print("Baslangic Delta kopyasi yaziliyor...", flush=True)
        source.repartition(4).write.format("delta").mode("errorifexists").option(
            "parquet.block.size", str(32 * 1024 * 1024)
        ).save(delta_path)
        print("Baslangic Parquet kopyasi yaziliyor...", flush=True)
        source.repartition(4).write.mode("errorifexists").option(
            "parquet.block.size", str(32 * 1024 * 1024)
        ).parquet(parquet_path)
        print("Kopyalar kaynakla karsilastiriliyor...", flush=True)
        delta_count = verify_copy(source, spark.read.format("delta").load(delta_path), expected)
        parquet_count = verify_copy(source, spark.read.parquet(parquet_path), expected)
        result = {
            "deney": "iot_atomik_yazma", "asama": "hazirlik", "kosu": run_id,
            "kaynak": source_path, "kaynak_surumu": version,
            "delta_yolu": delta_path, "parquet_yolu": parquet_path,
            "kaynak_kayit": expected, "delta_baslangic_kayit": delta_count,
            "parquet_baslangic_kayit": parquet_count,
            "delta_baslangic_surumu": 0,
            "kopyalar_kaynakla_ayni": True,
            "not": "Henuz kesinti uygulanmadi. Kaynak tablo degistirilmedi. Iki ayri deney kopyasi.",
        }
        path = evidence / ("iot_atomik_hazirlik_" + run_id + ".json")
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Delta baslangic kayit:", delta_count, flush=True)
        print("Parquet baslangic kayit:", parquet_count, flush=True)
        print("Iki kopyanin icerigi kaynakla ayni: BASARILI", flush=True)
        print("Kanit:", path, flush=True)
        print("ATOMIK DENEY HAZIRLIGI TAMAMLANDI - HENUZ KESINTI YAPILMADI", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
