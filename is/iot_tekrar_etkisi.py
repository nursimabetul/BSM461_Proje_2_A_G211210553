# -*- coding: utf-8 -*-
"""Z3: tekrar kayitlarin sayim, toplam ve ortalamaya etkisi.
Ham S3 verisini degistirmez. Sadece kanit/olcumler/ altina sonuc yazar.
Bu deney tam satir tekrari icindir; tum temizleme politikasini uygulamaz.
"""
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

NUMERIC=['sicaklik','nem','titresim']
OUT=Path('/kit/kanit/olcumler')

def change(before,after):
    if before is None or after is None:
        return {'once':before,'sonra':after,'fark_once_eksi_sonra':None,'fark_yuzde':None}
    diff=before-after
    return {'once':before,'sonra':after,'fark_once_eksi_sonra':diff,
            'fark_yuzde':None if after==0 else 100*diff/after}

def main():
    spark=SparkSession.builder.appName('iot-tekrar-etkisi').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set('spark.sql.shuffle.partitions','8')
    raw=None;unique=None
    try:
        OUT.mkdir(parents=True,exist_ok=True)
        raw=(spark.read.text('s3a://ham/iot/').select('value')
             .filter(F.length(F.trim('value'))>0).persist(StorageLevel.DISK_ONLY))
        n=raw.count()
        print('Ham satir:',n,flush=True)
        unique=raw.dropDuplicates(['value']).persist(StorageLevel.DISK_ONLY)
        u=unique.count()
        print('Benzersiz satir:',u,'Fazladan tekrar:',n-u,flush=True)
        def parse(df):
            return df.select(F.get_json_object('value','$.bolge').alias('bolge'),
                *[F.get_json_object('value','$.'+name).cast('double').alias(name) for name in NUMERIC])
        def aggregates():
            expr=[F.count('*').alias('kayit_sayisi')]
            for name in NUMERIC:
                expr.extend([F.count(name).alias(name+'_gecerli_sayi'),
                             F.sum(name).alias(name+'_toplam'),F.avg(name).alias(name+'_ortalama')])
            return expr
        def summarize(df):
            return dict(genel=df.agg(*aggregates()).first().asDict(),
                        bolgeler={row['bolge']:{k:v for k,v in row.asDict().items() if k!='bolge'}
                                   for row in df.groupBy('bolge').agg(*aggregates()).collect()})
        print('Tekrarlar varken hesaplar aliniyor...',flush=True)
        before=summarize(parse(raw))
        print('Tekrarlar cikarilinca hesaplar aliniyor...',flush=True)
        after=summarize(parse(unique))
        global_change={k:change(before['genel'][k],after['genel'][k]) for k in before['genel']}
        regions=[]
        for region,vals in before['bolgeler'].items():
            regions.append(dict(bolge=region,olcumler={k:change(v,after['bolgeler'][region][k]) for k,v in vals.items()}))
        report=dict(tarih_utc=datetime.now(timezone.utc).isoformat(),spark_surumu=spark.version,
            kaynak='s3a://ham/iot/',ham_satir=n,benzersiz_satir=u,fazladan_tekrar=n-u,
            genel=global_change,bolgeler=regions,
            notlar=[
                'Yalniz birebir ayni ham JSON satirlari tekillestirildi. Kaynak veri degistirilmedi.',
                'Uretici tekrar olayi ayni JSON satirini kopyalayarak olusturur. Alan sirasi farkli olan esdeger JSON satirlari bu deneyde birlestirilmez.',
                'Eksik ve null alanlar korunur. Bolge null olanlar ayri grup olarak raporlanir.',
                'Ortalama ve toplamlar ilgili alani gecerli olan kayitlardan hesaplanir; gecerli sayilar da verilir.',
                'Fark once eksi sonra; yuzde fark tekillestirme sonrasi deger payda alinarak hesaplandi.',
                'Sicaklik/nem/titresim toplamlari yalniz toplama isleminin tekrara duyarliligini gosterir; fiziksel bir toplam yorumlanmamistir.',
                'Ortalama mutlaka artmaz; tekrarlarin degerlerine gore artabilir, azalabilir veya ayni kalabilir.',
                'Cok kucuk farklarda kayan nokta toplama sirasi da etkili olabilir.',
                'Bu deney tum temizleme, Kafka yeniden baslatma veya uctan uca exactly-once testi degildir.'
            ])
        (OUT/'iot_tekrar_etkisi.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
        with (OUT/'iot_tekrar_etkisi.csv').open('w',encoding='utf-8-sig',newline='') as f:
            w=csv.writer(f);w.writerow(['kapsam','bolge','olcum','once','sonra','fark_once_eksi_sonra','fark_yuzde'])
            for key,row in global_change.items():w.writerow(['genel','',key,*row.values()])
            for region in regions:
                for key,row in region['olcumler'].items():
                    w.writerow(['bolge','NULL' if region['bolge'] is None else region['bolge'],key,*row.values()])
        print('\nTEKRAR ETKISI OLCUMU TAMAMLANDI',flush=True)
        print('Once:',n,'Sonra:',u,'Tekrar:',n-u,flush=True)
        for key,row in global_change.items():
            print(f"{key}: once={row['once']} sonra={row['sonra']} fark={row['fark_once_eksi_sonra']}",flush=True)
        print('Sonuclar: kanit/olcumler/iot_tekrar_etkisi.json ve CSV',flush=True)
    finally:
        if unique is not None:unique.unpersist()
        if raw is not None:raw.unpersist()
        spark.stop()

if __name__=='__main__':main()
