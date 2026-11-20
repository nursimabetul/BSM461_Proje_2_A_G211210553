TOPLU





1-Sensör Modeli ve Yaşına Göre Arıza/Anomali Sıklığı (Boyut Tablosu Join):

Soru:

"14 günlük geçmiş veride, sensörlerin üretim modeline ve kurulum yılına (sensorler.csv) göre kritik sıcaklık/titreşim eşiği aşım sıklığı (anomali oranı) nasıl değişmektedir? Eski kurulum yıllı cihazlar daha mı sık anomali üretiyor?"

Neden İyi? sensorler.csv boyut tablosuyla join yapmayı zorunlu kılar; explain() çıktısında BroadcastHashJoin optimizasyonunu göstermek ve çubuk/ısı grafiği çizmek için idealdir.





2-Bölgelere Göre Günün Saatlik İklim/Çalışma Deseni (Zaman Serisi Eğilimi):



Soru: "Bölgeler bazında günün 24 saatine göre ortalama sıcaklık ve nem değişimi nasıl bir desen izlemektedir? Gece-gündüz farkının en yüksek olduğu bölge hangisidir?"



Neden İyi? Üreticinin veriye gizlediği "saate ve güne bağlı değişim" desenini ortaya çıkarır; Python/R'da çok çizgili zaman serisi grafiği çizmek için çok net sonuç verir.





AKIŞ



1-Sessizleşen / Bağlantısı Kopan Sensör Tespiti (Heartbeat / Event-Time Gap):

Soru:

&#x20;"Son 10 dakikalık olay zamanı (event-time) penceresinde normal veri gönderim sıklığının (ör. dakikada en az $X$ mesaj) %80 altında kalan veya hiç veri göndermeyen (susan) sensörler hangileridir?"Neden İyi? Yönergedeki "Hangi sensör sustu?" örneğinin ölçüme dayalı profesyonel hâlidir; geç gelen veriler (--gec-orani) artırıldığında watermark süresinin yanlış alarmları nasıl etkilediğini ölçmek için mükemmeldir.





**2-Bölge Bazlı Veri Akış Hızı ve Geç Veri Oranı İzleme:**



**Soru: "Her 1 dakikalık pencerede bölgelere göre gelen geçerli olay sayısı ve olay zamanı ile işleme zamanı (processing-time) arasındaki ortalama gecikme süresi (saniye) nedir?"**



\*\*Neden İyi? Hem pano (Kibana/Grafana - Adım 12) üzerinde canlı sistem sağlığını izlemek hem de Adım 14'teki "sistemi kırma / yüksek gecikme" deneyini doğrudan gözlemlemek için çok uygundur.





