# Mimari taslak ve garanti tablosu (Adım 5 · Z0) — İLK TASLAK

Grup **G211210553** · Şablon 3 (IoT). Hazırlayan: Kişi 2 (Abdurrahman Doğan).
Şema: `kanit/mimari/omurga_taslak.svg` / `.png` (kaynak `omurga_taslak.dot`, betik `analiz/kisi2_sekiller.py`).
Bu tablo dönem boyunca dolacak (yönerge Adım 5.3) ve son hâlini Adım 15'te alacak. **?** = ilgili hafta (H8–H11) işlenmeden
kesinleşmeyecek hücre. Raporun 3. bölümü ve sunumun ilk slaytı bu şemadan yapılacak.

## 1. Kutular — hangi bileşen

Sürümler `docker-compose.yml` ve `spark-defaults.conf` dosyalarından alındı.

| # | Kutu (H1 §12 omurgası) | Bizim sistemde | Sürüm | Durum |
|---|---|---|---|---|
| ① | Olay kaynağı | 200 sensör, 60.000 ölçüm/s (kart). Kitte `araclar/uretici.py --sablon iot` | kit `bsm461-araclar:2` | ✅ |
| ② | Toplama | Kafka üreticisi: `acks=all`, idempotent (kit varsayılanı) | — | ✅ (ayarın gerekçesi Z8'de) |
| ③ | Günlük / Kuyruk | Apache Kafka, KRaft (ZooKeeper'sız), konu `sensorler`, anahtar `sensor_id` | 4.0.0 | 🟨 bölüm sayısı ? (Z8) |
| ④ | Akış işleme | Spark Structured Streaming. Sorular: susan sensör (10 dk), bölge başına 1 dk olay sayısı + gecikme | Spark 3.5.3 | ⬜ pencere/watermark ? (H11) |
| ⑤ | Ham depolama | SeaweedFS, S3 uyumlu. `s3://ham/iot/tarih=…/` JSON Lines; işlenmiş Parquet-zstd (`s3://islenmis/`) | 4.48 | ✅ (Kişi 1) |
| ⑥ | Toplu işleme | Spark (yerel kip), Delta Lake. Temizle → `sensorler.csv` ile join → topla | Spark 3.5.3 · Delta 3.2.1 | 🟨 (Kişi 3) |
| ⑦ | Servis deposu | ? Aday: MongoDB (kitte hazır). Kart: p99 ≤ 50 ms | 7.0 | ⬜ (H8, Adım 9) |
| ⑧ | Sunum | ? Pano: Python grafikleri ya da Grafana (Şablon 3'te Elasticsearch gerekmez) | — | ⬜ (Adım 12) |

## 2. Garanti tablosu — her ok için üç soru (H3 §10)

Okların harfleri şemadaki etiketlerdir. "Kit" sütunu bugünkü tek makine kurulumunu, "Üretim" sütunu gerçek ölçekte olması
gerekeni anlatır (yönerge Z0: sadeleştirmeler açıkça yazılır).

| Ok | ① Kaç kopya? | ② Sıra garantisi var mı? | ③ Ağ bölünürse ne olur? |
|---|---|---|---|
| **A** Kaynak → Toplama | 1. Sensör olayı bir kez üretir. Üretici yeniden dener. | ❌ Sensörler arası sıra yok. Bağlantı kopunca olaylar **geç** gelir (kart: %15'i, en fazla 30 dk). | Sensör tarafında tampon yoksa olay kaybolur; yeniden deneme **tekrar** üretebilir → alıcı idempotent olmalı (H3). |
| **B** Toplama → Kuyruk | **Kit: 1** (tek broker). Üretim: RF=3, `min.insync.replicas=2`, `acks=all`. | ✅ **Yalnız bölüm içinde.** Anahtar `sensor_id` → bir sensörün olayları aynı bölümde, sıralı. Bölümler arasında genel sıra yok (örnek mesajlarda 5–7 s'lik karışma, `veri_sozlugu.md`). | Üretim ayarıyla C tarafında: yeterli senkron kopya yoksa yazma reddedilir, üretici bekler (Kişi 1'in notu: CP/EC). Kitte tek düğüm: broker giderse yazma durur. |
| **C** Kuyruk → Akış | — (okuma; kopya üretmez) | ✅ Bölüm içinde, ofset sırasıyla. Olay zamanı sırası **değil**: geç olay sonradan gelir → watermark (H11). | Tüketici ofsetini kaydettiği yerden geri sarar; ofset ile sonuç birlikte kaydedilmezse çift sayım ya da kayıp olabilir → teslim garantisi Z8'de **deneyle** gösterilecek. |
| **D** Akış → Servis | ? (H8) | ? | ? Aday: A tarafı (eski sonuç servis edilir, uyarı gecikir) — H8 sonrası. |
| **E** Servis → Sunum | ? | ❌ Pano son değeri okur | ? |
| **F** Toplama → Ham depolama | **Kit: 1** (tek SeaweedFS düğümü, tek Docker hacmi; hacim kalıcılıktır, kopya değildir). Üretim: 3 kopya (farklı raflarda) ya da silme kodlaması. | ❌ Gerekmez: dosyalar `tarih=` bölümüne yazılır, sıra olay zamanından yeniden kurulur. | Kitte depo erişilemezse yazma durur. Nesne deposunda atomik rename yok (H4): yarım yazılmış dosya okuyucuya görünebilir → tablo biçimi (Z7, Delta). |
| **G** Ham depolama → Toplu | — (okuma) | ❌ Gerekmez; toplu iş tüm tarihi tarar. | İş başarısız olur ama **ham veri durur → baştan hesaplanır** (H1 §12: soğuk yolun garantisi). Kayıp bölüm soy ağacından yeniden hesaplanır (H6); dönüşümler belirlenimci olmalı. |
| **H** Toplu → Servis | ? (H8) | ❌ | ? Toplu iş servise yazarken yarıda kalırsa ikinci koşu **çift yazabilir** (H6: yan etkili kod iki kez çalışabilir) → idempotent yazma (anahtarla üzerine yaz). |

## 3. Kitin tek bilgisayar sadeleştirmeleri ve üretimde olması gereken

| Sadeleştirme | Kitte | Üretimde | Neden önemli (bu sistem için) |
|---|---|---|---|
| Kafka düğüm sayısı | 1 broker + controller aynı süreçte | ≥ 3 broker, 3 controller (KRaft çoğunluğu) | Tek broker giderse hem akış hem uyarı durur |
| Kafka kopya | `OFFSETS/TRANSACTION…_REPLICATION_FACTOR: 1` | RF=3, `min.insync.replicas=2` | Kopya yoksa disk arızası = veri kaybı |
| Kafka bölüm sayısı | varsayılan 3 | tepe 480.000 olay/s'ye göre hesaplanacak (Z8) | Bölüm sayısı tüketici paralelliğinin üst sınırı |
| Nesne deposu | tek SeaweedFS süreci, tek hacim, kopya yok | çok düğüm, 3 kopya / silme kodlaması, ayrı metadata çoğaltması | Z1: 65,5 TB arşiv tek diske sığmaz; ilk tıkanan bileşen |
| Spark | yerel kip, tek makine | küme (YARN/Kubernetes), dinamik yürütücü | Tam arşiv taraması tek makinede saatler sürer (Z1 §4) |
| Güvenlik | kimlik doğrulama ve şifreleme kapalı (PLAINTEXT) | TLS + kimlik doğrulama, S3 erişim anahtarları gizli | Sensör verisi kişisel değil (KVKK notu) ama altyapı yine de açık bırakılmaz |
| Yedek | yok (Docker hacmi) | kopyadan ayrı, geri yüklemesi denenmiş yedek | H3 vakası (GitLab 2017): "üç kopya" yanlışlıkla silmeye karşı korumaz |

## 4. Açık sorular (Adım 15'e kadar)

- Kafka bölüm sayısı ve tüketici sayısı: Z8'de 480.000 olay/s tepeye göre hesap.
- Watermark: kart %15 geç veri / 30 dk / kayıp ≤ %1 / uyarı ≤ 120 s. Tek bir watermark dördünü birden sağlayamaz;
  hangisinden ödün verildiği H11'den sonra gerekçelendirilecek.
- Servis deposu ve D/H oklarının garantileri: H8'den sonra.
- Toplu işin aşamaları ve aradaki Exchange'ler şemaya eklenecek (H6 §8: "toplu işinizin aşamalarını ve aradaki shuffle'ları
  çizin"). Kişi 3'ün `explain` çıktısından alınacak.
