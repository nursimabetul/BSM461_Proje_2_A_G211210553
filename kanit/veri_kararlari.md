# Veri ve depolama karar notu

Bu dosya rapor degildir. Olculen sonuclari ve henuz uygulanmamis tasarim tercihlerini ayirir. Nihai rapor, butun proje tamamlandiktan sonra yazilacaktir.

## Deneylerde olculenler

- Kaynak: `s3://ham/iot/tarih=.../`; 14 JSONL dosyasi, 3.000.000 kayit, 430.702.406 bayt. Ham veri korunur.
- Kalite: 6.073 fazladan tekrar; 2.983 eksik alanli, 2.947 null degerli kayit; en az bir eksik/null/bos iceren 5.930 kayit. Kategoriler genel olarak ortusebilir, sayilari toplayarak temiz kayit sayisi bulunmaz. Anormal sensor degerleri otomatik olarak kirli veri sayilmaz.
- Boyut deneyi: JSON 430.702.406; Snappy 46.307.832; Zstd 34.631.624; Gzip 33.812.981 bayt. Her bicimde ayni ham kayitlar kullanildi. Zstd deney icin dengeli bir boyut/sure tercihidir; uc yerel sorgu kosusu kodekler arasinda kesin hiz ustunlugu kanitlamaz.
- Zstd veri yolu `s3://islenmis/iot_bicim_deneyi/zstd/` bir bicim deneyidir; temizlenmis veri olarak adlandirilmaz.

## Bolum anahtari ve dosya duzeni

- Spark hash ve pozitif mod ile 8 bolumde en dolu bolum / ortalama: sensor_id 1,33996; bolge 3,43627; bolge+sensor_id 1,23249; UTC olay tarihi 2,28573. Bolgede 3, tarihte 1 bos bolum var.
- Toplu hash dagilimi acisindan bolge+sensor_id, bolgeye gore daha dengeli aday olarak secildi. Bu, sorgu hizlanmasi veya Kafka bolumleyicisi testi degildir; sorgu anlamina ve temiz veri sonucuna gore yeniden degerlendirilir.
- Sensor bazinda olay sirasi gereken Kafka akisinda sensor_id tercih edilir. Bu not mevcut uretici anahtarini degistirmez. Birlesik anahtar ayni sensoru farkli bolumlere ayirabileceginden Kafka'ya otomatik aktarilmaz.
- Ham katmanda tarih klasorleri korunur. Tarih hash dengesizligi, S3 tarih klasorlerinin tarih araligi filtrelerine faydasiz oldugu anlamina gelmez. Bilesik hash deneyi S3 klasor duzeni degildir.
- Kucuk dosya deneyi ayri `s3://islenmis/iot_dosya_birlestirme/iki_dosya_zstd/` yolunda yapildi: 8 dosya -> 2 dosya. Esit satir sayisi ve EXCEPT ALL ile tum alti alan ve tekrar adetleri kontrol edildi; 3.000.000 kayit korundu.
- Birlesmeden once/sonra: 34.631.624 / 37.830.244 bayt; ortalama dosya 4.328.953 / 18.915.122 bayt; ortalama sorgu 0,5098266883 / 0,6448129130 saniye. Her durumda bir isinma ve uc zamanli sorgu; OS/S3 onbellekleri temizlenmedi. Bu kosuda dosya azalmasi hiz kazandirmadi. Mevcut 8 dosyali deney korunur; iki dosyali sonuc karsilastirma kaniti olarak tutulur. Uretim birlestirme esigi bu kucuk deneyden sabitlenmez.
- 6.073 tekrarin sayim, toplam ve ortalamalara etkisi ayri olculdu. Ham veri degistirilmedi; bu uctan uca exactly-once garantisi degildir.

## Replikasyon ve tutarlilik karari

Yerel durum, paylasilan `docker-compose.yml` dosyasina gore:

- Kafka tek broker/controller. Ic offset/transaction konularinin replikasyon ayari 1. Varsayilan 3 partition, 3 kopya demek degildir. Kullanici konularinin canli ayarlari bu dosyadan tek tek dogrulanmis sayilmaz.
- SeaweedFS tek servis ve tek Docker veri volume'u. Bagimsiz makineler arasinda replikasyon yapilandirilmamis. Docker volume kaliciliktir, yedek kopya degildir.
- Spark isleme bilesenidir; Spark cache veya dosya sayisi, kalici verinin replikasyon sayisi degildir. Elasticsearch ve MongoDB istege bagli profillerdir; cekirdek deney sonuclarindan bunlara garanti atfedilmez.

Uretim icin onerilen, yerelde uygulanmamis hedef:

- Kafka: en az 3 broker uzerinde konu RF=3, min.insync.replicas=2, uretici acks=all; unclean leader election kapali; KRaft metadata icin 3 controller. Senkron kopya kosulu saglanmadiginda yaziyi basarili saymamak, partition gunlugu yazma yolunda C tercihidir. Normal durumda kopya onayi gecikmesi kabul edilir: bu kapsamdaki tasarim tercihi CP/EC'dir. Bu ifade tum pipeline'in lineer tutarli oldugunu veya kopya sayisinin her ag bolunmesinde hizmeti surdurdugunu iddia etmez.
- SeaweedFS: bagimsiz volume sunucularinda toplam 3 kopya ve hata alanlarina uygun yerlestirme hedeflenir; ornegin farkli raflar icin `020`, bu topoloji gercekten varsa. Master quorum ve filer metadata deposunun da ayrica cogaltilmasi gerekir. Veri kopyalamak tek basina S3 metadata tutarliligini kanitlamaz.
- Ham nesne yazilarinda C oncelikli hedef: gerekli kopya ve metadata onayi olmadan basari donulmemesi; normal durumda senkron onay gecikmesinin kabul edilmesi. Bu bir CP/EC tasarim hedefidir; secilecek filer metadata deposu ve hata testleriyle dogrulanmadan mevcut SeaweedFS servisinin olculmus sinifi diye sunulmaz.
- Yerel cok dugumlu ag bolunmesi/failover testi yapilmadi. Servis deposunun nihai secimi ve okuma tutarliligi karari tum projenin mimarisiyle birlikte kesinlesmelidir.

## Kanitlar ve kaynaklar

Sayisal olcumler: `kanit/olcumler.csv`; ayrintilar: `kanit/olcumler/` altindaki bicim, kalite, dagilim, tarih, tekrar ve dosya birlestirme JSON/CSV dosyalari. Birim ve deney sinirlari ortak CSV'de belirtilmistir.

- Kafka 4.0 konu ayarlari: https://kafka.apache.org/40/generated/topic_config.html
- Kafka 4.0 broker ayarlari: https://kafka.apache.org/40/generated/kafka_config.html
- SeaweedFS replikasyon: https://github.com/seaweedfs/seaweedfs/wiki/Replication

Bu not diger grup uyelerinin akis, join, servis, mimari veya hata deneyi gorevlerinin tamamlandigi anlamina gelmez.
