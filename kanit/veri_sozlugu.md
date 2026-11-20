# Veriyi tanıma — veri sözlüğü, dört kusur, KVKK notu (Adım 3 · Z10)

Grup **G211210553** · Şablon 3 (IoT) · Kafka konusu `sensorler`, anahtar `sensor_id`.
Hazırlayan: Kişi 2 (Abdurrahman Doğan). Sayılar `analiz/kisi2_veri_ve_olcek.py` ile depodaki kanıtlardan üretildi;
her sayının kaynağı yanında yazılı. Bu not rapor değildir; raporun Ek'i (veri sözlüğü) ve 10. bölümü (KVKK) buradan yazılacak.

**Kullanılan kanıtlar**

| Dosya | Ne | Kim |
|---|---|---|
| `kanit/senaryo.txt` | senaryo kartı | Nursima |
| `kanit/olcumler/iot_ornek_20.jsonl` | `sensorler` konusundan okunan 20 mesaj (okunma sırasıyla) | Nursima |
| `kanit/olcumler/iot_veri_kalitesi.json` | 3.000.000 kayıtlık toplu verinin kalite sayımı | Nursima |
| `kanit/olcumler/iot_dagilim_ozet.csv` | aday anahtarların 8 bölüme dağılımı | Nursima |
| `kanit/olcumler/sensorler.csv` | boyut tablosu, 200 satır | üretici |
| `kanit/olcumler/kisi2_kusur_ornekleri.json` | aşağıdaki kusur sayıları ve örnekleri | bu not |

## 1. Veri sözlüğü

### Olay: `sensorler` konusu ve `s3://ham/iot/tarih=…/` (JSON Lines)

| Alan | Tip | Anlam | Örnek (20 mesajlık örnekten) | Gözlenen aralık (20 mesaj) |
|---|---|---|---|---|
| `olay_zamani` | metin, ISO-8601 UTC (`…Z`) | Ölçümün sensörde alındığı an (olay zamanı, işlem zamanı değil) | `2026-11-14T19:04:24.941Z` | — |
| `sensor_id` | metin, `sensor-NNN` | Sensör kimliği; Kafka mesaj anahtarı; `sensorler.csv` ile join anahtarı | `sensor-062` | 200 farklı değer (toplu verinin tamamında) |
| `bolge` | metin, 7 değer | Sensörün coğrafi bölgesi | `Güneydoğu` | Ege, Akdeniz, Karadeniz, Marmara, İç Anadolu, Doğu Anadolu, Güneydoğu |
| `sicaklik` | ondalık (2 basamak) | Sıcaklık ölçümü | `30.14` | 11,35 – 31,44 |
| `nem` | ondalık (1 basamak) | Nem ölçümü | `44.5` | 40,7 – 63,3 |
| `titresim` | ondalık (3 basamak) | Titreşim ölçümü | `0.229` | 0,134 – 0,273 |

> Kit alanların birimini tanımlamıyor. Değer aralığına göre `sicaklik` °C, `nem` % bağıl nem olarak yorumlanıyor;
> `titresim` birimsiz bir genlik olarak ele alınıyor. Raporda bu bir varsayım olarak yazılmalı.

Mesaj boyutu: 137–147 bayt, ortalama 142,1 bayt (20 mesaj, satır sonu hariç). Toplu dosyada 143,57 bayt/kayıt
(430.702.406 bayt ÷ 3.000.000 kayıt, satır sonu dahil). İki bağımsız ölçüm arasındaki yaklaşık 1 baytlık fark satır sonudur.
Z1 hesabında toplu değer kullanıldı (`kanit/olcek_hesabi.md`).

### Boyut tablosu: `s3://ham/boyut/iot/sensorler.csv` (200 satır)

| Alan | Tip | Anlam | Örnek | Değerler |
|---|---|---|---|---|
| `sensor_id` | metin | Join anahtarı | `sensor-001` | 200 benzersiz |
| `bolge` | metin | Sensörün bölgesi (olaydaki `bolge` ile aynı) | `Ege` | 7 bölge |
| `model` | metin | Sensör modeli | `SN-12` | KM-5 (61), TX-100 (44), SN-10 (36), SN-12 (35), TX-200 (24) |
| `kurulum_yili` | tam sayı | Kurulum yılı | `2018` | 2018–2025 (yıl başına 22–29 sensör) |

> Olaydaki `bolge` boyut tablosunda da var. Olayda `bolge` boş geldiğinde (1.196 kayıt, aşağıda) `sensor_id` üzerinden
> boyut tablosundan doldurulabilir. Bu bir temizleme seçeneği olarak toplu işe (Kişi 3) önerilir.

## 2. Dört kusur — verimizden örnekler

| Kusur | Verimizde ne gördük | Sayı | Kaynak |
|---|---|---|---|
| **Çarpık anahtar** | `sensor_id` düzeyinde **sıcak anahtar yok**. En çok olay gönderen `sensor-091`: 15.724 olay; sensör başına ortalama 14.994. En fazla ÷ ortalama = **1,05** (H3 karar rehberi: > 1,5 ise sıcak nokta). En büyük sensörün payı %0,52 (eşit dağılımda %0,50); en büyük bölgenin payı %14,59 (eşit dağılımda %14,29). Çarpıklık anahtar düzeyinde değil, **bölümleme yönteminde** ortaya çıkıyor: `bolge` (7 değer) 8 bölüme hash'lenince en dolu ÷ ortalama **3,44**, 3 bölüm boş (Kişi 1'in ölçümü). | 1,05 · 3,44 | `kisi2_kusur_ornekleri.json`, `iot_dagilim_ozet.csv` |
| **Geç olay** | Örnek mesajların 9.'su `sensor-193`, `olay_zamani` = `18:57:07.966Z`. Kendisinden önce okunan mesajlar 19:04 civarında; o ana kadarki en yeni olaydan **441 s (7,4 dk) geride**. Kart: olayların %15'i en fazla 30 dk geç. | 1 / 20 mesaj | `iot_ornek_20.jsonl` |
| **Tekrar eden olay** | Toplu veride aynı içerikli kayıttan fazladan kopya: ör. `sensor-166`, `2026-10-31T21:54:10.587Z`, Doğu Anadolu, 10,27 / 63,9 / 0,195. | 6.073 kayıt (%0,20) | `iot_veri_kalitesi.json` |
| **Kirli kayıt** | Eksik alan: `sensor-190` kaydında `bolge` alanı hiç yok. Boş değer: `sensor-066` kaydında `"bolge": null`. | eksik 2.983 · null 2.947 · en az biri 5.930 (%0,20) | `iot_veri_kalitesi.json` |

Notlar:
- **Geç olay ölçümü sınırı:** Örnekte 5–7 s geride görünen beş mesaj daha var. Bunlar, birden çok Kafka bölümünden okurken
  oluşan sıra karışmasıyla açıklanabilir: bölümler arasında genel sıra yoktur. Bu yüzden 60 s altı farklar geç olay sayılmadı.
  Gerçek geç veri oranı ulaşma zamanı gerektirir; Z8 watermark deneyinde ölçülecek.
- **Kusur kategorileri örtüşebilir.** 2.983 + 2.947 ≠ 5.930 olmasının sebebi budur (Kişi 1'in notu).
- **Çarpık anahtar dışında bir desen.** 11 sensör, ortalamanın %80'inden az olay göndermiş: `sensor-017, 034, 051, 068, 085,
  102, 119, 136, 153, 170, 187`. Hepsinin numarası 17'nin katı ve olay sayıları 9.852–10.111 (ortalama 14.994). Bu bir kusur değil,
  **veri kaybı yaşayan sensör** deseni. Akış sorusu 1 (susan sensör) ve örnek toplu soru "sensör başına veri kaybı oranı" için
  doğrudan kanıt. Bulgu olarak Z10'a taşınmalı.

## 3. KVKK notu (Z10)

**Doğrudan kişisel veri yok.** Olayda ad, T.C. kimlik no, e-posta, telefon, IP adresi, cihaz sahibinin kimliği ya da kesin konum
bulunmuyor. Alanlar bir makine/ortam ölçümü (sıcaklık, nem, titreşim), sensör kimliği ve 7 değerli kaba bir bölgeden ibaret.

**Dolaylı risk değerlendirmesi.**

| Alan | Kişiyi tanımlayabilir mi? | Karar |
|---|---|---|
| `sensor_id` | Tek başına hayır. Ancak bir sensör bir konuta ya da tek kişilik bir işyerine bağlanırsa, eşleme tablosuyla birlikte o kişinin ortamı hakkında bilgi verir (ör. ortam sıcaklığı ve titreşimden kullanım saatleri çıkarılabilir). | **Saklanır, maskelenmez.** Join, uyarı ve bölümleme anahtarı olduğu için gereklidir. Sensör → sahip/adres eşlemesi **bu sistemde tutulmaz**. |
| `bolge` | Hayır. 7 coğrafi bölge, kişiyi ayırt edemeyecek kadar kaba. | Saklanır. |
| `olay_zamani` + ölçümler | Ancak `sensor_id` bir kişiye bağlanırsa (yukarıdaki durum). | Saklanır. |
| `sensorler.csv` (`model`, `kurulum_yili`) | Hayır. | Saklanır. |

**Gelecek için kural:** Sistem ileride sensörü bir müşteriye ya da adrese bağlarsa (ör. servis deposunda "bu sensör kimin?"),
bu eşleme ayrı, erişimi kısıtlı bir tabloda tutulur. Analitik katmana yalnız takma ad (`sensor_id`) taşınır. Kartın gerektirdiği
1.095 günlük ham veri saklaması yalnız kişisel veri içermeyen bu alanlar için uygundur. Kişisel veri eklenirse saklama süresi
amaçla sınırlanmalıdır.
