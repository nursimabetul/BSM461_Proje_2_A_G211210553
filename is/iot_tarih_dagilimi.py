# -*- coding: utf-8 -*-
"""Olay zamaninin UTC tarihini 8 Spark hash bolumunde degerlendirir.
Ham veriyi degistirmez; tarih anahtari ile S3 klasor duzeni farklidir.
"""
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark import StorageLevel

def main():
    spark=SparkSession.builder.appName('iot-tarih-dagilimi').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set('spark.sql.session.timeZone','UTC')
    spark.conf.set('spark.sql.shuffle.partitions','8')
    data=None
    try:
        schema=T.StructType([T.StructField('olay_zamani',T.StringType(),True)])
        data=(spark.read.schema(schema).json('s3a://ham/iot/')
            .select(F.to_date(F.to_timestamp('olay_zamani')).alias('olay_tarihi'))
            .persist(StorageLevel.DISK_ONLY))
        total=data.count()
        if total==0:raise RuntimeError('Ham veri bos')
        counts=data.groupBy('olay_tarihi').count().orderBy(F.col('olay_tarihi').asc_nulls_first()).collect()
        buckets=data.select(F.pmod(F.hash('olay_tarihi'),F.lit(8)).alias('bolum')).groupBy('bolum').count().collect()
        observed={int(r['bolum']):int(r['count']) for r in buckets}
        sizes=[observed.get(i,0) for i in range(8)]
        assert sum(sizes)==total
        maxrow=max(counts,key=lambda r:r['count'])
        result=dict(zaman_utc=datetime.now(timezone.utc).isoformat(),spark_surumu=spark.version,
            kaynak='s3a://ham/iot/',anahtar='olay_tarihi',saat_dilimi='UTC',toplam_kayit=total,
            tarih_sayisi=sum(r['olay_tarihi'] is not None for r in counts),
            null_tarih_kaydi=sum(r['count'] for r in counts if r['olay_tarihi'] is None),
            tarih_sayimlari=[dict(tarih=None if r['olay_tarihi'] is None else str(r['olay_tarihi']),kayit=r['count']) for r in counts],
            en_buyuk_anahtar=None if maxrow['olay_tarihi'] is None else str(maxrow['olay_tarihi']),
            en_buyuk_anahtar_payi_yuzde=100*maxrow['count']/total,
            bolum_sayilari=sizes,ortalama_bolum=total/8,
            en_dolu_bolum_bolu_ortalama=max(sizes)/(total/8),bos_bolum_sayisi=sum(n==0 for n in sizes),
            notlar=['Anahtar olay_zamani alanindan UTC takvim gunu olarak turetildi.',
            '8 bolum icin pozitif mod(Spark hash(tarih), 8) kullanildi; bos bolumler ortalamaya dahildir.',
            'Ayni veri kullanildi; tekrarlar ve eksik degerler korunur.',
            'Bu Kafka bolumleme deneyi veya gorev suresi olcumu degildir.',
            'Spark hash bolumlemesi ile S3 tarih klasorleri farklidir. Tarihin hash dagilimi zayif ciksa bile tarih araligi sorgulari icin S3 tarih klasorleri faydali olabilir.'])
        out=Path('/kit/kanit/olcumler');out.mkdir(parents=True,exist_ok=True)
        (out/'iot_tarih_dagilimi.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
        with (out/'iot_tarih_dagilimi.csv').open('w',encoding='utf-8-sig',newline='') as f:
            w=csv.writer(f);w.writerow(['tur','anahtar','kayit'])
            for r in result['tarih_sayimlari']:w.writerow(['tarih',r['tarih'],r['kayit']])
            for i,n in enumerate(sizes):w.writerow(['hash_bolumu',i,n])
        print('\nTARIH DAGILIMI OLCUMU TAMAMLANDI',flush=True)
        print('Toplam:',total,'Tarih sayisi:',result['tarih_sayisi'],'Null tarih:',result['null_tarih_kaydi'],flush=True)
        print('En buyuk tarih payi (%):',result['en_buyuk_anahtar_payi_yuzde'],flush=True)
        print('Bolumler:',sizes,flush=True)
        print('En dolu / ortalama:',result['en_dolu_bolum_bolu_ortalama'],flush=True)
        print('Sonuclar: kanit/olcumler/iot_tarih_dagilimi.json ve CSV',flush=True)
    finally:
        if data is not None:data.unpersist()
        spark.stop()

if __name__=='__main__':main()
