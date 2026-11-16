# -*- coding: utf-8 -*-
"""Zstd deney verisini ayri bir konumda iki Parquet dosyasina birlestirir.
Ham veri ve mevcut bicim deneyi degismez. Yalniz asagidaki HEDEF yeniden yazilir.
"""
import csv
import json
import math
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

KAYNAK = 's3a://islenmis/iot_bicim_deneyi/zstd/'
HEDEF = 's3a://islenmis/iot_dosya_birlestirme/iki_dosya_zstd/'
ALANLAR = ['olay_zamani', 'sensor_id', 'bolge', 'sicaklik', 'nem', 'titresim']
CIKTI = Path('/kit/kanit/olcumler')


def main():
    spark = SparkSession.builder.appName('iot-dosya-birlestirme').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set('spark.sql.shuffle.partitions', '8')

    def oku(yol):
        return spark.read.parquet(yol).select(*ALANLAR)

    def dosyalar(yol):
        p = spark._jvm.org.apache.hadoop.fs.Path(yol)
        fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
        iterator = fs.listFiles(p, True)
        sizes = []
        while iterator.hasNext():
            item = iterator.next()
            if item.getPath().getName().endswith('.parquet'):
                sizes.append(int(item.getLen()))
        if not sizes:
            raise RuntimeError('Parquet dosyasi bulunamadi: ' + yol)
        return dict(dosya_sayisi=len(sizes), toplam_bayt=sum(sizes),
                    ortalama_dosya_bayti=sum(sizes)/len(sizes),
                    en_kucuk_dosya_bayti=min(sizes), en_buyuk_dosya_bayti=max(sizes))

    def sorgu(yol):
        return (oku(yol).filter(F.col('sicaklik').isNotNull()).groupBy('bolge')
                .agg(F.count('*').alias('adet'),
                     F.avg('sicaklik').alias('ortalama_sicaklik'),
                     F.avg('nem').alias('ortalama_nem')))

    def sonuc(rows):
        return {r['bolge']: (r['adet'], r['ortalama_sicaklik'], r['ortalama_nem']) for r in rows}

    def kontrol(beklenen, gercek):
        if set(beklenen) != set(gercek):
            raise RuntimeError('Sorgu gruplari farkli')
        for key in beklenen:
            for a, b in zip(beklenen[key], gercek[key]):
                if a is None or b is None:
                    if a != b:
                        raise RuntimeError('Null sorgu sonucu farkli')
                elif not math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-8):
                    raise RuntimeError('Sorgu sonucu farkli: ' + str(key))

    try:
        baslangic = datetime.now(timezone.utc).isoformat()
        once = oku(KAYNAK)
        sayi = once.count()
        if sayi == 0:
            raise RuntimeError('Kaynak bos; islem durduruldu')
        once_meta = dosyalar(KAYNAK)
        print('Kaynak:', sayi, 'kayit,', once_meta['dosya_sayisi'], 'dosya', flush=True)
        print('Ayri deney klasorune 2 dosya yaziliyor...', flush=True)
        t = time.perf_counter()
        once.repartition(2).write.mode('overwrite').option('compression', 'zstd').parquet(HEDEF)
        yazma_s = time.perf_counter() - t
        sonra = oku(HEDEF)
        sonra_sayi = sonra.count()
        sonra_meta = dosyalar(HEDEF)
        if sonra_sayi != sayi or sonra_meta['dosya_sayisi'] != 2:
            raise RuntimeError('Hedef kayit veya dosya sayisi beklenenden farkli')
        print('Tekrarlar ve null degerler dahil tum satirlar karsilastiriliyor...', flush=True)
        # EXCEPT ALL tekrar adetlerini de korur. Esit toplam sayi ve tek yonlu
        # coklu-kume farkinin bos olmasi iki veri kumesinin esitligini saglar.
        fark_var = once.exceptAll(sonra).limit(1).count()
        if fark_var:
            raise RuntimeError('Satir icerigi veya tekrar adetleri degisti')
        spark.catalog.clearCache()
        referans = sonuc(sorgu(KAYNAK).collect())
        kontrol(referans, sonuc(sorgu(HEDEF).collect()))
        sureler = {'once': [], 'sonra': []}
        yollar = {'once': KAYNAK, 'sonra': HEDEF}
        for tur in range(3):
            sira = ['once', 'sonra'] if tur % 2 == 0 else ['sonra', 'once']
            for ad in sira:
                spark.catalog.clearCache()
                t = time.perf_counter()
                cevap = sonuc(sorgu(yollar[ad]).collect())
                sure = time.perf_counter() - t
                kontrol(referans, cevap)
                sureler[ad].append(sure)
                print(f'{ad} sorgu {tur+1}: {sure:.4f} s', flush=True)
        ozet = []
        for ad, meta, adet in [('once', once_meta, sayi), ('sonra', sonra_meta, sonra_sayi)]:
            ozet.append(dict(durum=ad, yol=yollar[ad], kayit_sayisi=adet, **meta,
                             sorgu_saniyeleri=sureler[ad],
                             ortalama_sorgu_s=statistics.mean(sureler[ad])))
        rapor = dict(baslangic_utc=baslangic, spark_surumu=spark.version,
                     kodek='zstd', hedef_dosya_sayisi=2, yazma_suresi_s=yazma_s,
                     kayitlar_ve_tekrar_adetleri_esit=True, ozet=ozet,
                     notlar=[
                         'Kaynak ve ham veri degismedi; yalniz ayri deney hedefi yeniden yazildi.',
                         'Temizleme veya tekillestirme yok; tum alti alan ve null degerler korundu.',
                         'Esit satir sayisi ve EXCEPT ALL ile satir icerigi/tekrar adetleri dogrulandi.',
                         'Her durumda bir isinma ve uc zamanli ayni sorgu; surelere dogrulama dahil degil.',
                         'Spark sorgu onbellegi kullanilmadi; OS/S3 onbellekleri temizlenmedi.',
                         'Dosya boyutu yalniz Parquet dosyalarini kapsar.',
                         'Iki dosya yerel deney tercihidir; uretim icin hedef dosya boyutu olcege gore belirlenir.',
                         'Dosya sayisinin azalmasi sorgunun mutlaka hizlanacagi anlamina gelmez.',
                         'Bu hedef tarih klasorlerine ayrilmamis ayri bir birlestirme deneyidir.'
                     ])
        CIKTI.mkdir(parents=True, exist_ok=True)
        (CIKTI/'iot_dosya_birlestirme.json').write_text(
            json.dumps(rapor, ensure_ascii=False, indent=2), encoding='utf-8')
        keys = ['durum', 'kayit_sayisi', 'dosya_sayisi', 'toplam_bayt',
                'ortalama_dosya_bayti', 'ortalama_sorgu_s']
        with (CIKTI/'iot_dosya_birlestirme.csv').open('w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(ozet)
        print('\nDOSYA BIRLESTIRME DENEYI TAMAMLANDI', flush=True)
        print('Kayitlar ve tekrar adetleri ayni:', sayi, flush=True)
        for r in ozet:
            print(f"{r['durum']}: {r['dosya_sayisi']} dosya; {r['toplam_bayt']} bayt; "
                  f"ortalama dosya {r['ortalama_dosya_bayti']/1e6:.2f} MB; "
                  f"sorgu {r['ortalama_sorgu_s']:.4f} s", flush=True)
        print('Sonuclar: kanit/olcumler/iot_dosya_birlestirme.json ve CSV', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
