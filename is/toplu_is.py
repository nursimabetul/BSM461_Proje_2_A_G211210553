# -*- coding: utf-8 -*-

import argparse, time, os, statistics, io
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
from datetime import timedelta
from contextlib import redirect_stdout
from pyspark import StorageLevel

ap = argparse.ArgumentParser()
ap.add_argument("--sablon", required=True, choices=["log", "satis", "iot", "haber"])
a = ap.parse_args()

spark = SparkSession.builder.appName(f"toplu-{a.sablon}").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "8")

"""print("\n--- SPARK PERFORMANS AYARLARI ---")

print(
    "Shuffle Partitions:",
    spark.conf.get("spark.sql.shuffle.partitions")
)

print(
    "Adaptive Query Execution:",
    spark.conf.get("spark.sql.adaptive.enabled")
)

print(
    "Default Parallelism:",
    spark.sparkContext.defaultParallelism
)
"""

spark.sparkContext.setLogLevel("WARN")

HAM = f"s3a://ham/{a.sablon}/"
PARQUET = f"s3a://islenmis/{a.sablon}/"
TABLO = f"s3a://tablolar/{a.sablon}"
KIRLI_TABLO = f"s3a://tablolar/{a.sablon}_kirli"

# ── 1. Ham veriyi oku ──────────────────────────────────────────────────────

t0 = time.time()

# IoT verisi için açık şema
iot_sema = StructType([
    StructField("olay_zamani", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("bolge", StringType(), True),
    StructField("sicaklik", StringType(), True),
    StructField("nem", StringType(), True),
    StructField("titresim", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

ham = (
    spark.read
    .schema(iot_sema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(HAM)
)

# ── 2. Temizle ─────────────────────────────────────────────────────────────


# Zorunlu alan kontrolleri
eksik_alan = (
    F.col("olay_zamani").isNull() |
    (F.trim(F.col("olay_zamani")) == "") |
    F.col("sensor_id").isNull() |
    (F.trim(F.col("sensor_id")) == "") |
    F.col("bolge").isNull() |
    (F.trim(F.col("bolge")) == "") |
    F.col("sicaklik").isNull() |
    (F.trim(F.col("sicaklik")) == "") |
    F.col("nem").isNull() |
    (F.trim(F.col("nem")) == "") |
    F.col("titresim").isNull() |
    (F.trim(F.col("titresim")) == "")
)

# Sayısal dönüşüm hataları
gecersiz_sicaklik = (
    F.col("sicaklik").isNotNull() &
    (F.trim(F.col("sicaklik")) != "") &
    F.col("sicaklik").cast("double").isNull()
)

gecersiz_nem = (
    F.col("nem").isNotNull() &
    (F.trim(F.col("nem")) != "") &
    F.col("nem").cast("double").isNull()
)

gecersiz_titresim = (
    F.col("titresim").isNotNull() &
    (F.trim(F.col("titresim")) != "") &
    F.col("titresim").cast("double").isNull()
)

gecersiz_zaman = (
    F.col("olay_zamani").isNotNull() &
    (F.trim(F.col("olay_zamani")) != "") &
    F.to_timestamp("olay_zamani").isNull()
)

# Tekrar kayıt kontrolü
tekrar_penceresi = Window.partitionBy(
    "olay_zamani",
    "sensor_id",
    "bolge",
    "sicaklik",
    "nem",
    "titresim"
).orderBy(F.lit(1))

kontrollu = ham.withColumn(
    "_sira",
    F.when(
        eksik_alan,
        F.lit(1)
    ).otherwise(
        F.row_number().over(tekrar_penceresi)
    )
)

# Her kayıt için hata nedenlerini belirle
kontrollu = kontrollu.withColumn(
    "hata_nedenleri",
    F.concat_ws(
        ", ",
        F.when(F.col("_corrupt_record").isNotNull(), "BOZUK_JSON"),
        F.when(eksik_alan, "EKSIK_ALAN"),
        F.when(gecersiz_sicaklik, "GECERSIZ_SICAKLIK"),
        F.when(gecersiz_nem, "GECERSIZ_NEM"),
        F.when(gecersiz_titresim, "GECERSIZ_TITRESIM"),
        F.when(gecersiz_zaman, "GECERSIZ_ZAMAN"),
        F.when(F.col("_sira") > 1, "TEKRAR_KAYIT")
    )
).withColumn(
    "islem_zamani",
    F.current_timestamp()
)

# Hatalı kayıtları karantinaya ayır
kirli = (
    kontrollu
    .filter(F.col("hata_nedenleri") != "")
    .drop("_sira")
    .withColumn("tarih", F.to_date(F.to_timestamp("olay_zamani")))
)

print("\nKARANTİNA NEDENLERİ:")

neden_sayilari = (
    kirli
    .select(F.explode(F.split("hata_nedenleri", ", ")).alias("neden"))
    .groupBy("neden")
    .count()
    .orderBy(F.desc("count"))
)

neden_sayilari.show(truncate=False)

"""
print("\nBİRDEN FAZLA HATA NEDENİ BULUNAN KAYITLAR:")

birden_fazla_hata = kirli.filter(
    F.size(F.split("hata_nedenleri", ", ")) > 1
).count()

print(f"Birden fazla hata nedeni: {birden_fazla_hata:,}")
"""

# Geçerli kayıtları oluştur ve veri tiplerini dönüştür
temiz = (
    kontrollu
    .filter(F.col("hata_nedenleri") == "")
    .withColumn("olay_zamani", F.to_timestamp("olay_zamani"))
    .withColumn("sicaklik", F.col("sicaklik").cast("double"))
    .withColumn("nem", F.col("nem").cast("double"))
    .withColumn("titresim", F.col("titresim").cast("double"))
    .withColumn("tarih", F.to_date("olay_zamani"))
    .drop("_sira", "hata_nedenleri", "islem_zamani", "_corrupt_record")
)

print("\nVERİ KALİTESİ İŞLEMİ TAMAMLANDI")
print(f"Geçerli kayıt: {temiz.count():,}")
print(f"Karantina kaydı: {kirli.count():,}\n")



# ── 3. Parquet olarak yaz ──────────────────────────────────────────────────
# Veriler tarih sütununa göre bölümlenmektedir.
# Böylece tarih filtreli sorgularda partition pruning uygulanabilir.

t0 = time.time()
temiz.write.mode("overwrite").partitionBy("tarih").parquet(PARQUET)
print(f"Parquet yazıldı: {PARQUET}   ({time.time()-t0:.1f} s)")

# Bölüm ve dosya sayılarını kontrol et

jvm = spark._jvm
hadoop_conf = spark._jsc.hadoopConfiguration()
parquet_yolu = jvm.org.apache.hadoop.fs.Path(PARQUET)
fs = parquet_yolu.getFileSystem(hadoop_conf)

durum = fs.listStatus(parquet_yolu)

bolum_sayisi = 0
dosya_sayisi = 0

for oge in durum:
    if oge.isDirectory() and oge.getPath().getName().startswith("tarih="):
        bolum_sayisi += 1
        alt_durum = fs.listStatus(oge.getPath())

        for alt_oge in alt_durum:
            if alt_oge.isFile() and alt_oge.getPath().getName().endswith(".parquet"):
                dosya_sayisi += 1

""""
print("\nPARQUET BÖLÜMLEME KONTROLÜ")
print(f"Tarih bölümü sayısı: {bolum_sayisi}")
print(f"Parquet dosya sayısı: {dosya_sayisi}")

if bolum_sayisi > 0:
    print(f"Bölüm başına ortalama dosya: {dosya_sayisi / bolum_sayisi:.2f}")

"""
# Parquet dosya boyutlarını ve bölüm dağılımını incele

""""
durum = fs.listStatus(parquet_yolu)

bolum_sayisi = 0
dosya_boyutlari = []
bolum_istatistikleri = []

for oge in durum:
    if oge.isDirectory() and oge.getPath().getName().startswith("tarih="):

        bolum_sayisi += 1
        bolum_adi = oge.getPath().getName()

        bolum_dosyalari = []

        for alt_oge in fs.listStatus(oge.getPath()):
            if (
                alt_oge.isFile()
                and alt_oge.getPath().getName().endswith(".parquet")
            ):
                boyut = alt_oge.getLen()
                bolum_dosyalari.append(boyut)
                dosya_boyutlari.append(boyut)

        bolum_toplam = sum(bolum_dosyalari)

        bolum_istatistikleri.append({
            "bolum": bolum_adi,
            "dosya_sayisi": len(bolum_dosyalari),
            "toplam_mb": bolum_toplam / (1024 ** 2),
            "ortalama_mb": (
                bolum_toplam / len(bolum_dosyalari) / (1024 ** 2)
                if bolum_dosyalari else 0
            )
        })

print("\nPARQUET BÖLÜMLEME VE DOSYA BOYUTU ANALİZİ")

print(f"Tarih bölümü sayısı: {bolum_sayisi}")
print(f"Parquet dosya sayısı: {len(dosya_boyutlari)}")

if dosya_boyutlari:
    toplam_boyut = sum(dosya_boyutlari)

    print(f"Toplam veri boyutu: {toplam_boyut / (1024 ** 2):.2f} MB")
    print(f"Ortalama dosya boyutu: {statistics.mean(dosya_boyutlari) / (1024 ** 2):.2f} MB")
    print(f"En küçük dosya: {min(dosya_boyutlari) / (1024 ** 2):.2f} MB")
    print(f"En büyük dosya: {max(dosya_boyutlari) / (1024 ** 2):.2f} MB")
    print(f"Medyan dosya boyutu: {statistics.median(dosya_boyutlari) / (1024 ** 2):.2f} MB")

print("\nGÜN BAZINDA DOSYA DAĞILIMI")
print(f"{'Tarih bölümü':<20} {'Dosya':>8} {'Toplam MB':>14} {'Ort. MB':>12}")

for b in sorted(bolum_istatistikleri, key=lambda x: x["bolum"]):
    print(
        f"{b['bolum']:<20} "
        f"{b['dosya_sayisi']:>8} "
        f"{b['toplam_mb']:>14.2f} "
        f"{b['ortalama_mb']:>12.2f}"
    )
"""

# ── 4. Delta tablosu ───────────────────────────────────────────────────────

# Geçerli ve tekilleştirilmiş kayıtlar
temiz.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("tarih") \
    .save(TABLO)

print(f"Temiz Delta tablosu yazıldı: {TABLO}")

# ── BATCH 1: Sensör bazlı anomali analizi ─────────────────────────────

baslangic = time.perf_counter()

# İstatistiksel anomali eşikleri
SICAKLIK_ESIGI = 35.19
TITRESIM_ESIGI = 0.325

# Sensör boyut tablosunu yükle
BOYUT_YOLU = "s3a://ham/boyut/iot/sensorler.csv"

# Temiz Delta tablosunu yeniden oku
temiz_tablo = spark.read.format("delta").load(TABLO)

sensorler = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(BOYUT_YOLU)
    .select(
        "sensor_id",
        "model",
        "kurulum_yili",
        F.col("bolge").alias("sensor_bolge")
    )
)

birlesik = temiz_tablo.join(
    sensorler,
    on="sensor_id",
    how="inner"
)

print("\n--- BATCH 1: JOIN KONTROLÜ ---")
print("Temiz tablo kayıt sayısı:", temiz.count())
print("Boyut tablosu kayıt sayısı:", sensorler.count())
print("Birleşim sonrası kayıt sayısı:", birlesik.count())

birlesik.printSchema()

# Sıcaklık ve titreşim anomalilerini işaretle
birlesik = (
    birlesik
    .withColumn(
        "sicaklik_anomali",
        F.when(
            F.col("sicaklik") > SICAKLIK_ESIGI, 1
        ).otherwise(0)
    )
    .withColumn(
        "titresim_anomali",
        F.when(
            F.col("titresim") > TITRESIM_ESIGI, 1
        ).otherwise(0)
    )
    .withColumn(
        "anomali",
        F.when(
            (F.col("sicaklik") > SICAKLIK_ESIGI) |
            (F.col("titresim") > TITRESIM_ESIGI),
            1
        ).otherwise(0)
    )
)

"""
# ── CACHE OPTİMİZASYONU ─────────────────────────────────────────

print("\n--- CACHE OPTİMİZASYONU ---")

cache_baslangic = time.perf_counter()

# Ortak veri kümesini bellekte veya gerekirse diskte sakla
birlesik = birlesik.persist(StorageLevel.MEMORY_AND_DISK)

# Cache'i gerçekten doldurmak için bir action çalıştır
cache_kayit_sayisi = birlesik.count()

cache_hazirlama_suresi = time.perf_counter() - cache_baslangic

print("Cache'e alınan kayıt sayısı:", cache_kayit_sayisi)
print(f"Cache hazırlama süresi: {cache_hazirlama_suresi:.2f} saniye")
print("Cache Storage Level:", birlesik.storageLevel)
print("------------------------------------------------")
"""

# Sensör modeline göre anomali analizi
model_analizi = (
    birlesik
    .groupBy("model")
    .agg(
        F.count("*").alias("toplam_kayit"),
        F.sum("sicaklik_anomali").alias("sicaklik_anomali_sayisi"),
        F.sum("titresim_anomali").alias("titresim_anomali_sayisi"),
        F.sum("anomali").alias("toplam_anomali")
    )
    .withColumn(
        "anomali_orani",
        F.round(
            100 * F.col("toplam_anomali") / F.col("toplam_kayit"),
            2
        )
    )
    .orderBy(F.desc("anomali_orani"))
)

print("\n" + "=" * 65)
print("BATCH 1: SENSÖR MODELİ VE YAŞINA GÖRE ARIZA/ANOMALİ SIKLIĞI")
print("=" * 65 + "\n")

print("\n--- MODEL BAZINDA ANOMALİ ANALİZİ ---")

model_analizi.show(truncate=False)

# ── Sensör başına anomali oranları ────────────────────────────────

sensor_oranlari = (
    birlesik
    .groupBy("sensor_id", "model", "kurulum_yili")
    .agg(
        F.count("*").alias("sensor_toplam_kayit"),
        F.sum("anomali").alias("sensor_anomali_sayisi")
    )
    .withColumn(
        "sensor_anomali_orani",
        100 * F.col("sensor_anomali_sayisi") /
        F.col("sensor_toplam_kayit")
    )
)

# Model bazında sensörlerin ortalama anomali oranı
model_sensor_analizi = (
    sensor_oranlari
    .groupBy("model")
    .agg(
        F.countDistinct("sensor_id").alias("sensor_sayisi"),
        F.round(
            F.avg("sensor_anomali_orani"), 2
        ).alias("ortalama_sensor_anomali_orani")
    )
    .orderBy(F.desc("ortalama_sensor_anomali_orani"))
)

print("\n--- MODEL BAZINDA SENSÖR ORTALAMASI ---")
model_sensor_analizi.show(truncate=False)


# Kurulum yılı bazında sensörlerin ortalama anomali oranı
yil_sensor_analizi = (
    sensor_oranlari
    .groupBy("kurulum_yili")
    .agg(
        F.countDistinct("sensor_id").alias("sensor_sayisi"),
        F.round(
            F.avg("sensor_anomali_orani"), 2
        ).alias("ortalama_sensor_anomali_orani")
    )
    .orderBy("kurulum_yili")
)

print("\n--- KURULUM YILI BAZINDA SENSÖR ORTALAMASI ---")
yil_sensor_analizi.show(truncate=False)

# BATCH 2: Bölge ve Saat Bazında Sıcaklık / Nem Analizi ────────────────────────────────

print("\n" + "=" * 65)
print("BATCH 2: BOLGE VE SAAT BAZINDA SICAKLIK / NEM ANALIZI")
print("=" * 65)

# 1. Saat bilgisini ve bölge bilgisini hazırla
# Boyut tablosundaki bolge bilgisini kullanıyoruz.
saatlik_veri = (
    birlesik
    .select(
        F.col("sensor_bolge").alias("bolge"),
        F.hour("olay_zamani").alias("saat"),
        "sicaklik",
        "nem"
    )
)

# 2. Bölgelerin 24 saatlik ortalama sıcaklık ve nem değerleri
saatlik_analiz = (
    saatlik_veri
    .groupBy("bolge", "saat")
    .agg(
        F.round(F.avg("sicaklik"), 2).alias("ortalama_sicaklik"),
        F.round(F.avg("nem"), 2).alias("ortalama_nem"),
        F.count("*").alias("olay_sayisi")
    )
    .orderBy("bolge", "saat")
)

# 3. Gündüz ve gece dönemlerini belirle
donemli_veri = (
    saatlik_veri
    .withColumn(
        "donem",
        F.when(
            (F.col("saat") >= 6) & (F.col("saat") < 18),
            "Gunduz"
        ).otherwise("Gece")
    )
)

# 4. Gündüz ortalamalarını hesapla
gunduz_analiz = (
    donemli_veri
    .filter(F.col("donem") == "Gunduz")
    .groupBy("bolge")
    .agg(
        F.avg("sicaklik").alias("gunduz_sicaklik"),
        F.avg("nem").alias("gunduz_nem")
    )
)

# 5. Gece ortalamalarını hesapla
gece_analiz = (
    donemli_veri
    .filter(F.col("donem") == "Gece")
    .groupBy("bolge")
    .agg(
        F.avg("sicaklik").alias("gece_sicaklik"),
        F.avg("nem").alias("gece_nem")
    )
)

# 6. Gündüz ve gece sonuçlarını birleştir
gece_gunduz_fark = (
    gunduz_analiz
    .join(gece_analiz, on="bolge", how="inner")
    .withColumn(
        "sicaklik_farki",
        F.round(
            F.abs(F.col("gunduz_sicaklik") - F.col("gece_sicaklik")),
            2
        )
    )
    .withColumn(
        "nem_farki",
        F.round(
            F.abs(F.col("gunduz_nem") - F.col("gece_nem")),
            2
        )
    )
    .select(
        "bolge",
        F.round("gunduz_sicaklik", 2).alias("gunduz_ort_sicaklik"),
        F.round("gece_sicaklik", 2).alias("gece_ort_sicaklik"),
        "sicaklik_farki",
        F.round("gunduz_nem", 2).alias("gunduz_ort_nem"),
        F.round("gece_nem", 2).alias("gece_ort_nem"),
        "nem_farki"
    )
)

print("\n--- BOLGELERE GORE GECE-GUNDUZ FARKLARI ---")
gece_gunduz_fark.orderBy(
    F.desc("sicaklik_farki")
).show(100, truncate=False)

# 7. En yüksek sıcaklık ve nem farkına sahip bölgeleri bul
print("\n--- EN YUKSEK SICAKLIK FARKI ---")
gece_gunduz_fark.orderBy(
    F.desc("sicaklik_farki")
).show(1, truncate=False)

print("\n--- EN YUKSEK NEM FARKI ---")
gece_gunduz_fark.orderBy(
    F.desc("nem_farki")
).show(1, truncate=False)

bitis = time.perf_counter()

# ── BATCH PERFORMANS ÖLÇÜMÜ ──

print("\n--- BATCH PERFORMANS ÖLÇÜMÜ ---")

# Batch 1
t0 = time.perf_counter()

model_sonuclari = model_analizi.collect()
yil_sonuclari = yil_sensor_analizi.collect()

# ── BATCH 1: Fiziksel plan kanıtı ────────────────────────────────

os.makedirs("kanit", exist_ok=True)

plan_ciktisi = io.StringIO()

with redirect_stdout(plan_ciktisi):
    model_analizi.explain(mode="formatted")

with open(
    "kanit/explain-batch1.txt",
    "w",
    encoding="utf-8"
) as dosya:
    dosya.write(plan_ciktisi.getvalue())

print("Batch 1 fiziksel planı kanit/explain-batch1.txt dosyasına kaydedildi.")

batch1_suresi = time.perf_counter() - t0

print(f"Batch 1 Süresi: {batch1_suresi:.2f} saniye")
print(f"Model sonucu: {len(model_sonuclari)} kayıt")
print(f"Yıl sonucu: {len(yil_sonuclari)} kayıt")


# Batch 2
t0 = time.perf_counter()

saatlik_sonuclari = saatlik_analiz.collect()
gece_gunduz_sonuclari = gece_gunduz_fark.collect()

batch2_suresi = time.perf_counter() - t0

print(f"Batch 2 Süresi: {batch2_suresi:.2f} saniye")
print(f"Saatlik sonuç: {len(saatlik_sonuclari)} kayıt")
print(f"Gece-gündüz sonucu: {len(gece_gunduz_sonuclari)} kayıt")

print("--------------------------------")

# Karantinaya alınan kayıtlar
kirli.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("tarih") \
    .save(KIRLI_TABLO)

print(f"Karantina Delta tablosu yazıldı: {KIRLI_TABLO}")

"""
# Temiz Delta tablosunun sürüm geçmişi
print("Delta sürümleri:")
spark.sql(
    f"DESCRIBE HISTORY delta.`{TABLO}`"
).select(
    "version", "timestamp", "operation"
).show(truncate=False)
"""

# Yazılan Delta tablolarını yeniden oku
temiz_kontrol = spark.read.format("delta").load(TABLO)
kirli_kontrol = spark.read.format("delta").load(KIRLI_TABLO)

"""
print("DELTA TABLO KONTROLÜ")
print("Temiz tablo:", temiz_kontrol.count())
print("Karantina tablosu:", kirli_kontrol.count())
print("Toplam:", temiz_kontrol.count() + kirli_kontrol.count())
"""

# ── 5. Bir analiz sorgusu ve fiziksel planı ────────────────────────────────

tablo = spark.read.format("delta").load(TABLO)

ANAHTAR = {
    "log": "ip",
    "satis": "urun_id",
    "iot": "sensor_id",
    "haber": "kaynak"
}[a.sablon]

# Veri setindeki en son tarihi bul
son_tarih = tablo.agg(
    F.max("tarih").alias("son_tarih")
).first()["son_tarih"]

# Son üç günlük veri aralığını belirle
baslangic_tarihi = son_tarih - timedelta(days=2)

print(f"Analiz başlangıcı: {baslangic_tarihi}")
print(f"Analiz bitişi: {son_tarih}")

# Analiz sorgusu
sorgu = (
    tablo
    .filter(F.col("tarih") >= F.lit(baslangic_tarihi))
    .groupBy(ANAHTAR)
    .count()
    .orderBy(F.desc("count"))
)

# Sorguyu çalıştır
t0 = time.time()
sorgu.show(10, truncate=False)
print(f"Sorgu Süresi: {time.time()-t0:.2f} s")
print(f"Batchs Analiz Süresi: {bitis - baslangic:.2f} saniye")

# Fiziksel planı göster
sorgu.explain(mode="formatted")

os.makedirs("kanit", exist_ok=True)

plan_ciktisi = io.StringIO()

with redirect_stdout(plan_ciktisi):
    sorgu.explain(mode="formatted")

with open("kanit/explain-1.txt", "w", encoding="utf-8") as dosya:
    dosya.write(plan_ciktisi.getvalue())

# ── 6. Çarpıklık kontrolü (H3, H7) ─────────────────────────────────────────

# Sensör bazında kayıt sayılarını hesapla
anahtar_sayim = (
    tablo
    .groupBy(ANAHTAR)
    .count()
)

# Toplam kayıt sayısı
toplam = tablo.count()

# En fazla kayıt gönderen sensörün kayıt sayısı
en_buyuk_satir = (
    anahtar_sayim
    .orderBy(F.desc("count"))
    .first()
)

if en_buyuk_satir is not None and toplam > 0:
    en_buyuk = en_buyuk_satir["count"]
    en_buyuk_pay = 100 * en_buyuk / toplam

    print(f"Toplam kayıt: {toplam:,}")
    print(f"En büyük anahtarın kayıt sayısı: {en_buyuk:,}")
    print(f"En büyük anahtarın payı: %{en_buyuk_pay:.3f}")
else:
    print("Çarpıklık kontrolü yapılamadı: Veri bulunamadı.")

# TODO (H7): Bu anahtara göre JOIN veya groupBy yapan bir işte ne olur?
# Spark UI'da görev sürelerini inceleyin; gerekiyorsa tuzlama uygulayarak
# ÖNCE/SONRA sürelerini karşılaştırın.
spark.stop()
