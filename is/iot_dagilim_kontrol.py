# -*- coding: utf-8 -*-
"""Z3: ayni 3 milyon kayitta anahtar ve 8 Spark hash bolumu karsilastirmasi.
Kaynak veriyi veya mevcut ciktilari degistirmez; kanit dosyalarini yazar.
Kafka ureticisinin kullandigi bolumleyiciyi olcmez.
"""
import csv
import json
from pathlib import Path
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel

OUT = Path('/kit/kanit')
INPUT = 's3a://islenmis/iot_bicim_deneyi/zstd/'

def main():
    spark=SparkSession.builder.appName('iot-dagilim-kontrol').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set('spark.sql.shuffle.partitions','8')
    data=None
    try:
        OUT.mkdir(parents=True,exist_ok=True)
        data=spark.read.parquet(INPUT).select('sensor_id','bolge').persist(StorageLevel.DISK_ONLY)
        total=data.count()
        if total == 0: raise RuntimeError('Veri bos')
        candidates=[('sensor_id',['sensor_id']),('bolge',['bolge']),
                    ('bolge_ve_sensor_id',['bolge','sensor_id'])]
        reports=[]
        for name, cols in candidates:
            print('Olculuyor:',name,flush=True)
            null_cond=F.col(cols[0]).isNull()
            for col in cols[1:]: null_cond=null_cond | F.col(col).isNull()
            null_count=data.filter(null_cond).count()
            counts=data.groupBy(*cols).count()
            top=counts.orderBy(F.desc('count'),*[F.col(c).asc_nulls_first() for c in cols]).limit(10).collect()
            unique_count=counts.count()
            # Spark hash repartition ifadesi: pozitif mod(hash(anahtarlar), 8).
            # Bos bolumler de ortalamaya dahil edilir.
            hashed=data.select(F.pmod(F.hash(*[F.col(c) for c in cols]),F.lit(8)).alias('bolum'))
            observed={int(r['bolum']):int(r['count']) for r in hashed.groupBy('bolum').count().collect()}
            partitions=[observed.get(i,0) for i in range(8)]
            if sum(partitions)!=total: raise RuntimeError('Bolum toplami eslesmiyor')
            avg=total/8
            report=dict(anahtar=name,sutunlar=cols,toplam_kayit=total,
                        anahtarinda_null_olan_kayit=null_count,
                        farkli_anahtar_grubu=unique_count,
                        en_buyuk_anahtar=top[0].asDict(),
                        en_buyuk_anahtar_payi_yuzde=100*top[0]['count']/total,
                        ilk_10_anahtar=[r.asDict() for r in top],
                        bolum_sayilari=partitions,ortalama_bolum=avg,
                        en_dolu_bolum=max(partitions),
                        en_dolu_bolum_bolu_ortalama=max(partitions)/avg,
                        bos_bolum_sayisi=sum(n==0 for n in partitions))
            reports.append(report)
            print(f"{name}: en buyuk anahtar %{report['en_buyuk_anahtar_payi_yuzde']:.3f}; bolumler {partitions}; en dolu/ortalama {report['en_dolu_bolum_bolu_ortalama']:.3f}",flush=True)
        by={r['anahtar']:r for r in reports}
        comparison=dict(once='bolge',sonra='bolge_ve_sensor_id',
            once_oran=by['bolge']['en_dolu_bolum_bolu_ortalama'],
            sonra_oran=by['bolge_ve_sensor_id']['en_dolu_bolum_bolu_ortalama'])
        result=dict(zaman_utc=datetime.now(timezone.utc).isoformat(),
            kaynak=INPUT,spark_surumu=spark.version,adaylar=reports,
            bilesik_anahtar_karsilastirmasi=comparison,
            notlar=['Ayni ham kayitlar kullanildi; tekrarlar ve null degerler korunmustur.',
            'Eksik alanlar Parquet okununca null olarak temsil edilir.',
            '8 bolum icin Spark hash ve pozitif mod kullanildi. Bos bolumler ortalamaya dahildir.',
            'Bilesik anahtar deneyi bolge anahtarina sensor_id eklenmesini olcer; iyilesme varsayilmaz.',
            'En buyuk anahtar payi ile hash bolum dolulugu farkli olculerdir.',
            'Bu Kafka bolumleyicisinin veya gorev surelerinin olcumu degildir.',
            'Ham veri, temiz veri, Kafka ve S3 tarih bolumlemesi icin kararlar ayri gerekcelendirilmelidir.',
            'Daha dengeli dagilim her sorguyu hizlandirmaz; bolgeye gore toplama yine shuffle gerektirebilir.',
            'Bir sensorun tek anahtarda kalmasi gerektiginde bolge+sensor anahtarinin semantigi ayrica degerlendirilir.'])
        (OUT/'iot_dagilim_kontrol.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
        with (OUT/'iot_dagilim_ozet.csv').open('w',encoding='utf-8-sig',newline='') as f:
            w=csv.writer(f);w.writerow(['anahtar','toplam','null_anahtar_kaydi','en_buyuk_anahtar_payi_yuzde','en_dolu_bolu_ortalama','bos_bolum'])
            for r in reports:w.writerow([r[k] for k in ['anahtar','toplam_kayit','anahtarinda_null_olan_kayit','en_buyuk_anahtar_payi_yuzde','en_dolu_bolum_bolu_ortalama','bos_bolum_sayisi']])
        with (OUT/'iot_dagilim_bolumler.csv').open('w',encoding='utf-8-sig',newline='') as f:
            w=csv.writer(f);w.writerow(['anahtar','bolum','kayit'])
            for r in reports:
                for i,n in enumerate(r['bolum_sayilari']):w.writerow([r['anahtar'],i,n])
        print('\nDAGILIM KONTROLU TAMAMLANDI',flush=True)
        print('Sonuclar: kanit/iot_dagilim_kontrol.json ve iki CSV dosyasi',flush=True)
    finally:
        if data is not None:data.unpersist()
        spark.stop()

if __name__=='__main__':main()
