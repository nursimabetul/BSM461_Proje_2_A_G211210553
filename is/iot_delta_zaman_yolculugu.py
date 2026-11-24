"""Z7: Ayri deney tablosunda ikinci yazma ve Delta surumlerini okuma.

Kaynak tabloya yazmaz. Yeni gun, son gunun olay zamanlari 1 gun ileri
alinan SENTETIK deney verisidir; yeni gercek sensor olcumu degildir.
Her calistirma benzersiz bir deney yolu ve kanit dosyalari olusturur.
"""
import csv
import json
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from pyspark.sql import SparkSession, functions as F


def main():
    spark = SparkSession.builder.appName("iot-delta-zaman-yolculugu").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    source_path = "s3a://tablolar/iot"
    target_path = "s3a://tablolar/iot_z7_deney/zaman_yolculugu/" + run_id
    evidence = Path("/kit/kanit/olcumler")
    evidence.mkdir(parents=True, exist_ok=True)
    stem = evidence / ("iot_delta_zaman_yolculugu_" + run_id)
    try:
        # Kaynak surumune sabitle: deney sirasindaki baska yazmalari dahil etme.
        source_version = int(spark.sql(
            f"DESCRIBE HISTORY delta.`{source_path}`"
        ).agg(F.max("version")).first()[0])
        source = spark.read.format("delta").option(
            "versionAsOf", source_version
        ).load(source_path)
        source_count = source.count()
        last_day = source.agg(F.max("tarih")).first()[0]
        if not source_count or last_day is None:
            raise RuntimeError("Kaynak temiz tablo bos veya tarih bilgisi yok.")

        print("Kaynak:", source_path, "surum:", source_version, flush=True)
        print("Deney tablosu:", target_path, flush=True)
        print("Ilk yazma basliyor; kaynak kayit:", source_count, flush=True)
        source.repartition(4).write.format("delta").mode("errorifexists").option(
            "parquet.block.size", str(32 * 1024 * 1024)
        ).partitionBy("tarih").save(target_path)

        new_day = last_day + timedelta(days=1)
        extra = source.filter(F.col("tarih") == F.lit(last_day)).withColumn(
            "olay_zamani", F.col("olay_zamani") + F.expr("INTERVAL 1 DAY")
        ).withColumn("tarih", F.to_date("olay_zamani"))
        extra_count = extra.count()
        if extra_count == 0:
            raise RuntimeError("Yeni gun deney verisi olusturulamadi.")
        print("SENTETIK yeni gun:", new_day, "eklenecek kayit:", extra_count, flush=True)
        extra.repartition(4).write.format("delta").mode("append").option(
            "parquet.block.size", str(32 * 1024 * 1024)
        ).save(target_path)

        history = spark.sql(f"DESCRIBE HISTORY delta.`{target_path}`")
        print("\nDESCRIBE HISTORY:", flush=True)
        history.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)
        history_rows = [row.asDict(recursive=True) for row in history.collect()]
        old = spark.read.format("delta").option("versionAsOf", 0).load(target_path)
        current = spark.read.format("delta").option("versionAsOf", 1).load(target_path)
        old_count = old.count()
        current_count = current.count()
        old_new_day_count = old.filter(F.col("tarih") == F.lit(new_day)).count()
        new_day_count = current.filter(F.col("tarih") == F.lit(new_day)).count()
        # Esit sayim + tek yonlu EXCEPT ALL: null ve tekrarlar dahil ayni cokluk.
        original_preserved = old_count == source_count and not source.exceptAll(
            old.select(*source.columns)
        ).limit(1).count()
        checks = {
            "surumler_0_ve_1": sorted(int(r["version"]) for r in history_rows) == [0, 1],
            "surum_0_kaynakla_ayni": bool(original_preserved),
            "surum_1_sayimi": current_count == source_count + extra_count,
            "surum_0_yeni_gun_yok": old_new_day_count == 0,
            "surum_1_yeni_gun_var": new_day_count == extra_count,
        }
        metrics = {
            "kaynak_kayit": source_count,
            "surum_0_kayit": old_count,
            "eklenen_sentetik_kayit": extra_count,
            "surum_1_kayit": current_count,
            "surum_0_yeni_gun_kayit": old_new_day_count,
            "surum_1_yeni_gun_kayit": new_day_count,
        }
        result = {
            "deney": "iot_delta_zaman_yolculugu", "kosu": run_id,
            "kaynak": source_path, "kaynak_surumu": source_version,
            "hedef": target_path, "yeni_gun": str(new_day),
            "not": "Son gunun zamanlari +1 gun kaydirildi; sentetik deney verisi. Tek deney kosusu; performans testi degil.",
            "surum_gecmisi": history_rows, "metrikler": metrics,
            "kontroller": checks, "basarili": all(checks.values()),
        }
        stem.with_suffix(".json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )
        with stem.with_suffix(".csv").open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["deney", "kapsam", "metrik", "tekrar", "deger", "birim", "kaynak", "aciklama"])
            for name, value in metrics.items():
                writer.writerow(["delta_zaman_yolculugu", run_id, name, 1, value,
                                 "kayit", stem.name + ".json", result["not"]])
        for name, value in metrics.items():
            print(name + ":", value, flush=True)
        for name, ok in checks.items():
            print(name + ":", "BASARILI" if ok else "BASARISIZ", flush=True)
        print("Kanit:", str(stem) + ".json ve .csv", flush=True)
        if not all(checks.values()):
            raise RuntimeError("Deney kontrollerinden en az biri basarisiz. Kanit JSON dosyasini inceleyin.")
        print("ZAMAN YOLCULUGU DENEYI TAMAMLANDI", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
