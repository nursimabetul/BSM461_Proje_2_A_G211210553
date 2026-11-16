# -*- coding: utf-8 -*-
"""Ayni ham IoT kayitlarini JSON ve uc Parquet kodekinde karsilastirir.
Ham veri ve arkadasinizin toplu_is.py dosyasi degistirilmez.
Yalniz islenmis/iot_bicim_deneyi/ altindaki deney ciktilari yeniden yazilir.
"""
import csv
import json
import math
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark import StorageLevel

FIELDS = ['olay_zamani', 'sensor_id', 'bolge', 'sicaklik', 'nem', 'titresim']
SCHEMA = T.StructType([T.StructField(k, T.StringType() if k in FIELDS[:3] else T.DoubleType(), True) for k in FIELDS])
HAM = 's3a://ham/iot/'
BASE = 's3a://islenmis/iot_bicim_deneyi/'
OUT = Path('/kit/kanit')

def main():
    spark = SparkSession.builder.appName('iot-bicim-olcumu').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set('spark.sql.shuffle.partitions', '8')
    outrows, summary = [], []
    OUT.mkdir(parents=True, exist_ok=True)
    def read(kind, path):
        if kind == 'json':
            return spark.read.schema(SCHEMA).option('mode', 'FAILFAST').json(path).select(*FIELDS)
        return spark.read.parquet(path).select(*FIELDS)
    def files(path, suffix):
        jp = spark._jvm.org.apache.hadoop.fs.Path(path)
        fs = jp.getFileSystem(spark._jsc.hadoopConfiguration())
        it = fs.listFiles(jp, True)
        sizes = []
        while it.hasNext():
            st = it.next()
            if st.getPath().getName().endswith(suffix):
                sizes.append(int(st.getLen()))
        if not sizes:
            raise RuntimeError('Veri dosyasi bulunamadi: ' + path)
        return sizes
    def query(df):
        # Ayni filtre, sutunlar ve toplama her bicimde kullanilir.
        return (df.filter(F.col('sicaklik').isNotNull())
                .groupBy('bolge').agg(F.count('*').alias('adet'),
                F.avg('sicaklik').alias('ortalama_sicaklik'),
                F.avg('nem').alias('ortalama_nem')))
    def result_dict(rows):
        return {r['bolge']: (r['adet'], r['ortalama_sicaklik'], r['ortalama_nem']) for r in rows}
    def check(expected, actual):
        if set(expected) != set(actual):
            raise RuntimeError('Sorgu gruplari eslesmiyor')
        for key in expected:
            for a,b in zip(expected[key], actual[key]):
                if a is None or b is None:
                    if a != b: raise RuntimeError('Null sonuclari eslesmiyor')
                elif not math.isclose(a,b,rel_tol=1e-9,abs_tol=1e-8):
                    raise RuntimeError('Sorgu sonuclari eslesmiyor: ' + str(key))

    try:
        start = datetime.now(timezone.utc).isoformat()
        raw = read('json', HAM).persist(StorageLevel.DISK_ONLY)
        n = raw.count()
        if n == 0: raise RuntimeError('Ham veri bos')
        print('Ham kayit sayisi:', n, flush=True)
        paths = [('JSON', 'json', HAM)]
        for codec in ['snappy', 'zstd', 'gzip']:
            path = BASE + codec + '/'
            print('Parquet yaziliyor:', codec, flush=True)
            t = time.perf_counter()
            raw.repartition(8).write.mode('overwrite').option('compression', codec).parquet(path)
            elapsed = time.perf_counter()-t
            paths.append(('Parquet-'+codec, 'parquet', path))
            outrows.append(['yazma', 'Parquet-'+codec, 1, elapsed, 's'])
        raw.unpersist(blocking=True)
        spark.catalog.clearCache()
        baseline = None
        metadata = {}
        for label, kind, path in paths:
            df = read(kind,path)
            actual_n = df.count()
            if actual_n != n:
                raise RuntimeError('Kayit sayisi eslesmiyor: ' + label)
            sizes = files(path, '.jsonl' if kind == 'json' else '.parquet')
            metadata[label] = dict(kayit=actual_n, dosya=len(sizes), bayt=sum(sizes),
                                   ortalama_dosya_bayti=sum(sizes)/len(sizes), yol=path)
            (OUT/(label.lower()+'-explain.txt')).write_text(
                query(df)._jdf.queryExecution().toString(), encoding='utf-8')
            print('Isinma sorgusu:', label, flush=True)
            res = result_dict(query(df).collect())
            if baseline is None: baseline = res
            else: check(baseline,res)
        timings = {label:[] for label,_,_ in paths}
        for run in range(3):
            # Her turda baslangic bicimini degistir; siraya bagli etkiyi azalt.
            order = paths[run:] + paths[:run]
            for label,kind,path in order:
                spark.catalog.clearCache()
                t = time.perf_counter()
                result = result_dict(query(read(kind,path)).collect())
                elapsed = time.perf_counter()-t
                check(baseline,result)
                timings[label].append(elapsed)
                outrows.append(['sorgu', label, run+1, elapsed, 's'])
                print(f'{label} tekrar {run+1}: {elapsed:.3f} s', flush=True)
        for label,_,_ in paths:
            summary.append(dict(bicim=label, **metadata[label],
                sorgu_saniyeleri=timings[label], ortalama_sorgu_s=statistics.mean(timings[label]),
                ham_boyuta_oran=metadata[label]['bayt']/metadata['JSON']['bayt']))
        report = dict(baslangic_utc=start, spark_surumu=spark.version,
            varsayilan_paralellik=spark.sparkContext.defaultParallelism,
            shuffle_bolum_sayisi=8, yazma_bolum_sayisi=8, ozet=summary,
            notlar=['Temizleme veya tekillestirme uygulanmadi; tum bicimler ayni ham kayitlari icerir.',
            'Olay zamani metin olarak korundu; bicim karsilastirmasinda tipler ayni tutuldu.',
            'Boyut yalniz veri dosyalarini kapsar; _SUCCESS gibi yardimci nesneler dahil degil.',
            'Her bicimde bir isinma ve uc zamanli sorgu var. Spark sorgu onbellegi kullanilmadi.',
            'Isletim sistemi ve S3 onbellekleri temizlenmedi; sonuclar soguk disk testi degildir.',
            'Sorgu: sicaklik bos olmayanlari bolgeye gore say, ortalama sicaklik ve nem hesapla.',
            'Sorgu sonuclari ve satir sayilari her bicimde dogrulandi.',
            'Yazma suresi her kodekte tek kosudur; kesin performans karari icin tek basina yeterli degildir.',
            'Makine RAM/CPU ve Docker kaynak ayarlari ayrica rapora yazilmalidir.'])
        (OUT/'iot_bicim_olcumu.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
        with (OUT/'iot_bicim_tekrarlar.csv').open('w',encoding='utf-8-sig',newline='') as f:
            w=csv.writer(f);w.writerow(['olcum','bicim','tekrar','deger','birim']);w.writerows(outrows)
        with (OUT/'iot_bicim_ozet.csv').open('w',encoding='utf-8-sig',newline='') as f:
            w=csv.writer(f);w.writerow(['bicim','kayit','dosya','bayt','ortalama_dosya_bayti','ortalama_sorgu_s'])
            for row in summary:
                w.writerow([row[k] for k in ['bicim','kayit','dosya','bayt','ortalama_dosya_bayti','ortalama_sorgu_s']])
        print('\nBICIM OLCUMU TAMAMLANDI',flush=True)
        for row in summary:
            print(f"{row['bicim']}: {row['bayt']} bayt, {row['dosya']} dosya, sorgu ortalamasi {row['ortalama_sorgu_s']:.3f} s",flush=True)
        print('Sonuclar kanit/iot_bicim_olcumu.json ve CSV dosyalarina kaydedildi.',flush=True)
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
