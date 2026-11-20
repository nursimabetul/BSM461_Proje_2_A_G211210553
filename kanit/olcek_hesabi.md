# Ölçek ve eşik kararı (Adım 4 · Z1)

Grup **G211210553** · Şablon 3 (IoT). Hazırlayan: Kişi 2 (Abdurrahman Doğan).
Hesap betiği: `analiz/kisi2_veri_ve_olcek.py` → `kanit/olcumler/kisi2_olcek_hesabi.csv` (her satırda formül ve kaynak).
Şekil: `kanit/grafikler/kisi2_z1_esik.png` (betik: `analiz/kisi2_sekiller.py`). Raporun 4. bölümü buradan yazılacak.

**Sonuç:** Bir günlük veri Parquet-zstd ile 60 GB'tır ve tek makinede işlenebilir. Kartın istediği 1.095 günlük ham arşiv
ise JSON olarak 815 TB, Parquet-zstd ile 65,5 TB eder ve sürekli büyür. H1 eşik tablosuna göre depolama **dağıtık olmak
zorunda**. İşleme ise sorgunun kaç günü taradığına bağlıdır.

> Birimler onluktur: 1 GB = 10⁹ bayt, 1 TB = 10¹² bayt (H1 §4). Sayılar tam hâliyle `kisi2_olcek_hesabi.csv` içinde;
> burada yuvarlanmış yazıldı.

## 1. Girdiler

| Girdi | Değer | Birim | Nereden |
|---|---|---|---|
| Ortalama hız | 60.000 | olay/s | senaryo kartı (`kanit/senaryo.txt`) |
| Tepe çarpanı | 8 | kat | senaryo kartı |
| Saklama | 1.095 | gün | senaryo kartı ("ham veri 1095 gün saklanır") |
| **Olay başına bayt** | **143,57** | bayt/olay | **ölçüm:** 430.702.406 bayt ÷ 3.000.000 kayıt, toplu JSONL, satır sonu dahil (`iot_veri_kalitesi.json`) |
| Olay başına bayt, sağlama | 142,10 (137–147) | bayt/olay | **ölçüm:** Kafka'dan okunan 20 mesaj, satır sonu hariç (`iot_ornek_20.jsonl`). Fark ≈ 1 bayt = satır sonu |
| Parquet-zstd ÷ JSON | 0,0804 | oran | Kişi 1 ölçümü: 34.631.624 ÷ 430.702.406 bayt (`iot_bicim_ozet.csv`) |
| Parquet-snappy ÷ JSON | 0,1075 | oran | Kişi 1 ölçümü: 46.307.832 ÷ 430.702.406 bayt |

## 2. Hacim — zarf arkası hesap

Formül (yönerge Adım 4.3): `olay/s × bayt/olay × 86.400 × saklama günü = saklanan ham veri`

| | Değer | Birim | Hesap |
|---|---|---|---|
| Günlük olay | 5,184 milyar | olay/gün | 60.000 × 86.400 |
| Günlük ham veri (JSON) | 744 | GB/gün | 5,184 × 10⁹ × 143,57 bayt |
| Günlük, Parquet-zstd | 60 | GB/gün | 744 × 0,0804 |
| **Saklanan ham veri (JSON, 1.095 gün)** | **815** | **TB** | 744 GB × 1.095 |
| Saklanan, Parquet-zstd | 65,5 | TB | 815 × 0,0804 |
| Saklanan, Parquet-snappy | 87,6 | TB | 815 × 0,1075 |
| Ortalama giriş (JSON) | 8,6 | MB/s | 60.000 × 143,57 bayt |
| Tepe giriş (JSON) | 68,9 | MB/s | 480.000 × 143,57 bayt |

Kitteki 3.000.000 kayıt (0,43 GB) gerçek sistemin yalnız **50 saniyelik** verisidir (3.000.000 ÷ 60.000).
Kitte yapılan her ölçüm, gerçek ölçeğin çok küçük bir örneği üzerindedir. Raporda bu ayrım açıkça yazılmalı.

## 3. Ortalama mı, tepe mi?

İki ayrı karar var, çünkü bileşenler farklı şeye duyarlı:

| Bileşen | Neye göre | Gerekçe |
|---|---|---|
| Kafka girişi + akış işleme (③④) | **Tepeye göre: 480.000 olay/s, 68,9 MB/s** | Kart, uyarının en geç 120 s'de çıkmasını istiyor. Kapasite ortalamaya göre kurulursa, tepe saatte fazla gelen 420.000 olay/s kuyrukta birikir ve uyarı gecikmesi büyür. Kuyruk birikimi emebilir ama tepe bir saat sürüyorsa birikim 120 s'yi aşar. |
| Ham depolama (⑤) | **Ortalamaya göre: 744 GB/gün** | Disk kapasitesi günün toplamına bağlıdır; tepe yük günlük toplamı değiştirmez. Tepe yalnız yazma bant genişliğini zorlar: 68,9 MB/s, H1 §5'teki 1 Gb/s ağın (~125 MB/s) altındadır. |
| Toplu işleme (⑥) | **Ortalamaya göre** | Gece işi dünün toplamını tarar; süre veri boyutuyla ölçeklenir, anlık hızla değil. |

## 4. Tek makine mi, dağıtık mı? (H1 §7 eşik tablosu ve karar ağacı)

| Veri | Boyut | H1 §7 bandı | Karar |
|---|---|---|---|
| Kit verisi (3 M kayıt, JSON) | 0,43 GB | < 1 GB — belleğe sığar | pandas bile yeterdi. Kitte Spark kullanmamız öğretim amaçlı |
| Bir gün, Parquet-zstd | 60 GB | 10–100 GB — diskten akıtarak | **Tek makine** (DuckDB/Polars ya da yerel Spark). NVMe'den (~5.000 MB/s, H1 §5) bir günü okumak ≈ 12 s |
| Bir gün, JSON | 744 GB | 500 GB – birkaç TB — tek sunucu zorlanır | JSON'da tutmak tek makineyi zorlar. Parquet'e geçiş (Z2) bu yüzden ölçek kararıdır |
| 1.095 gün, Parquet-zstd | 65,5 TB | > birkaç TB, sürekli büyüyor | **Dağıtık depolama zorunlu.** Bütün arşivi tarayan bir sorgu da dağıtık işleme ister: 65,5 TB tek NVMe'den ≈ 3,6 saat, 10 Gb/s ağdan ≈ 14,6 saat |

Karar ağacındaki yerimiz "> birkaç TB → sürekli büyüyor mu? → evet → dağıtık sistem (şimdi gerekli)" dalıdır.
Ancak bu yalnız **depolama ve tam tarama** için geçerli. Günlük ve haftalık toplu sorgular (`tarih=` bölüm budamasıyla
birkaç güne inen sorgular) tek güçlü makinede çalışabilir. H1 vakası (McSherry vd., COST) tam olarak bu ayrımı uyarıyor:
küme yalnız gerçekten gerektiği yerde kurulmalı.

## 5. Gerçek ölçekte ilk tıkanan bileşen

Kit tek makinede (MacBook, Docker'a ayrılan bellekle) çalışıyor. Kart ölçeğinde sırasıyla:

1. **Ham depolama: SeaweedFS, tek düğüm, tek disk (⑤).** İlk tıkanan bileşen. Günde 744 GB JSON gelir; 1 TB'lık bir disk
   ~1,3 günde, Parquet-zstd'ye çevrilse bile ~17 günde (1.000 ÷ 59,8) dolar. 1.095 günlük hedefe tek diskle ulaşılamaz.
   Kitte kopya da yok (`-volume.max=0`, tek `s3-veri` hacmi).
2. **Kafka, tek broker (③).** Tepe girişi 68,9 MB/s. Tek bir NVMe diskin yazma hızı bunu karşılar, ama üretimde RF=3
   ile her bayt iki kopya daha olarak ağdan geçer. Küme genelinde giriş + çoğaltma ≈ 207 MB/s (68,9 × 3) eder.
   Bu, tek bir 1 Gb/s bağlantıya (~125 MB/s) sığmaz; yük birden çok broker'a dağıtılmalı ya da 10 Gb/s ağ kullanılmalı. Ayrıca tek broker tek arıza
   noktasıdır. Bölüm sayısı kitte 3 (`KAFKA_NUM_PARTITIONS: 3`); 480.000 olay/s için yetersiz olup olmadığı Z8'de hesaplanacak.
3. **Spark, tek makine yerel kip (④⑥).** Bir günlük veride sorun değil. Tam arşiv taramasında ve akışta tepe yükte
   yürütücü sayısı sınırı ortaya çıkar.

## 6. Verinin 10 ve 100 katı

| | 1× (kart) | 10× | 100× |
|---|---|---|---|
| Ortalama / tepe hız | 60 bin / 480 bin olay/s | 600 bin / 4,8 milyon olay/s | 6 milyon / 48 milyon olay/s |
| Tepe giriş (JSON) | 68,9 MB/s | 689 MB/s | 6,9 GB/s |
| Bir gün, Parquet-zstd | 60 GB | 598 GB | 6,0 TB |
| 1.095 gün, Parquet-zstd | 65,5 TB | 655 TB | 6,6 PB |
| Bir günün eşik bandı | 10–100 GB → tek makine | 500 GB – birkaç TB → tek sunucu zorlanır | > birkaç TB → dağıtık |
| Mimari değişikliği | Dağıtık nesne deposu (çok düğüm, 3 kopya ya da silme kodlaması); Kafka 3 broker, RF=3 | Günlük toplu iş de kümeye taşınır (bir günü tek makinede işlemek zorlaşır). Kafka'da bölüm sayısı ve broker sayısı artar; giriş ağı 10 Gb/s olmalı | Her katman yatay ölçeklenir. Ham JSON saklamak yerine girişte Parquet/tablo biçimine yazmak ve **ham saklamayı kısaltmak ya da örneklemek** tartışılmalı. 6,6 PB'lık arşiv, kartın 1.095 gün koşulunun yeniden pazarlık konusu olması demektir |

Not: 10× ve 100× satırları kartın oranları (bayt/olay, sıkıştırma) aynı kalırsa diye hesaplandı. Gerçekte hız arttıkça
toplu iş Amdahl sınırına (H1 §10) takılır: makine eklemek seri kalan kısmı (ör. tek sürücüde toplanan sonuç) hızlandırmaz.
