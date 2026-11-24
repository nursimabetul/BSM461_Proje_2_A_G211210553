# IoT servis deposu: veri modeli ve erisim desenleri (Z6)

Bu dosya uygulama tasarim notudur; calistirma veya gecikme olcumu kaniti
degildir. Son rapor, uygulama ve ekip entegrasyonu bittikten sonra derlenecek.

## Kapsam ve kaynak

- Veritabani: `bsm461_iot`.
- Yerel servis: Docker Compose `mongo`, MongoDB `7.0` imaj ailesi.
- Konteynerlerden baglanti: `mongodb://mongo:27017`; bilgisayardan:
  `mongodb://localhost:27017`.
- Kaynak: `s3a://tablolar/iot` temiz Delta tablosunun sabitlenmis bir surumu
  ve `s3a://ham/boyut/iot/sensorler.csv` boyut tablosu.
- Analiz mantigi: ekipteki `is/toplu_is.py` dosyasinin iki toplu sorusu.
  Sicaklik esigi `35.19`, titresim esigi `0.325`; anomali, bu iki
  kosuldan en az birinin saglanmasidir. Bunlar toplu isten alinan
  analiz esikleridir; senaryo kartinin gecikme hedefiyle karistirilmaz.
- 3 milyon ham olay MongoDB'ye kopyalanmaz. Hazir analiz sonuclari aktarilir.
  Ham, temiz ve karantina verileri S3/Delta katmaninda kalir.
- Z7 deneyindeki `sicaklik +1000` veya sentetik yeni gun verileri kullanilmaz.

## Koleksiyonlar

Her sonuc belgesi ortak `kosu_id`, `kaynak_surumu` ve UTC `uretim_zamani`
alanlarini tasir. `_id`, kosu kimligi ile asagidaki dogal anahtarin
birlesiminden belirlenir. Anahtarlarin birlestirilmesinde belirsiz ayiraclar
yerine kararlı JSON kodlamasi veya esdeger cakismasiz kodlama kullanilir.

| Koleksiyon | Dogal anahtar | Sonuc alanlari |
|---|---|---|
| `model_anomali` | `model` | `toplam_kayit`, `sicaklik_anomali_sayisi`, `titresim_anomali_sayisi`, `toplam_anomali`, `anomali_orani` |
| `model_sensor_ozet` | `model` | `sensor_sayisi`, `ortalama_sensor_anomali_orani` |
| `yil_sensor_ozet` | `kurulum_yili` | `sensor_sayisi`, `ortalama_sensor_anomali_orani` |
| `bolge_saatlik` | `bolge`, `saat` | `ortalama_sicaklik`, `ortalama_nem`, `olay_sayisi` |
| `bolge_gece_gunduz` | `bolge` | `gunduz_ort_sicaklik`, `gece_ort_sicaklik`, `sicaklik_farki`, `gunduz_ort_nem`, `gece_ort_nem`, `nem_farki` |

Saatlik sonuc, ilk fazdaki gibi veri araliginin tum gunleri icin saat
bazinda birlesik ortalamadir; ayri ayri tarih-saat serisi degildir.
Model bazinda toplam olaylardan bulunan anomali yuzdesi ile sensor
yuzdelerinin esit agirlikli ortalamasi ayri koleksiyonlarda tutulur;
bu iki istatistik birbirinin yerine kullanilmaz.

Ek `servis_meta` koleksiyonunda tamamlanmis toplu kosunun kimligi,
kaynak Delta surumu, veri tarih araligi, kaynak/birlesim sayimlari,
sonuc sayimlari, UTC zaman bilgisi ve aktarici kodunun SHA-256 ozeti
tutulur. `_id: aktif_toplu` belgesi panonun okuyacagi kosuyu gosterir.

## Erisim desenleri ve indeksler

Senaryo kartindaki servis okuma hedefi **p99 <= 50 ms**'dir. Asagidaki
okuma sikliklari ekip tarafindan degistirilebilen pano tasarim kararidir;
senaryo kartindan alinmis veya gercekte olculmus kullanici trafigi degildir.

| Sorgu | Filtre ve siralama | Onerilen siklik | p99 hedefi | Indeks tasarimi |
|---|---|---|---|---|
| Aktif toplu kosu | `_id: aktif_toplu` | Pano yenilemesinde, 30 sn'de bir/istemci | <= 50 ms | Yerlesik `_id` |
| Model anomali siralamasi | `kosu_id`; `anomali_orani` azalan, `model` artan | 30 sn'de bir/istemci | <= 50 ms | `(kosu_id, anomali_orani:-1, model)` |
| Model sensor ortalamasi | `kosu_id`; `model` | 30 sn'de bir/istemci | <= 50 ms | `(kosu_id, model)` benzersiz |
| Kurulum yili karsilastirmasi | `kosu_id`; `kurulum_yili` artan | 30 sn'de bir/istemci | <= 50 ms | `(kosu_id, kurulum_yili)` benzersiz |
| Secili bolgenin 24 saatlik deseni | `kosu_id`, `bolge`; `saat` artan | Filtre degisiminde ve 30 sn'de bir/istemci | <= 50 ms | `(kosu_id, bolge, saat)` benzersiz |
| Tum bolgelerin saatlik deseni | `kosu_id`; `bolge`, `saat` artan | Genel gorunumde 30 sn'de bir/istemci | <= 50 ms | Ayni `(kosu_id, bolge, saat)` |
| Gece-gunduz karsilastirmasi | `kosu_id`; `bolge` artan | 30 sn'de bir/istemci | <= 50 ms | `(kosu_id, bolge)` benzersiz |

`model_anomali` icin ayrica `(kosu_id, model)` benzersiz indeksi dogal
anahtari korur. Indekslerin gercek sorgu planlari ve istemci tarafindaki
okuma sureleri 6. adimda olculecek. Yerel kucuk ozet tablolarda elde edilen
gecikme, senaryonun 60.000 olay/sn ortalama veya 480.000 olay/sn tepe
uretim yukunun tasindigini kanitlamaz. Pano sorgu sikligi olay uretim
hiziyla ayni degildir. p99 hedefi ile olculen deger ayri kaydedilir.

## Gomme ve referans karari

Her sonuc grubunun anahtari ve birlikte okunan az sayidaki olcum alani
tek belgede tutulur (denormalizasyon). Sensor boyut tablosuyla join
Spark'ta yapilir; pano her istekte MongoDB `$lookup` veya S3 taramasi yapmaz.
Boyut tablosunun tum gecmisi veya sinirsiz olay listesi bir belgeye gomulmez.
Bu, belge boyutunun zamanla sinirsiz buyumesini onler.

Kosu metaverisi `kosu_id` ile referanslanir; kaynak yolu, kod ozeti ve
kontrol sayimlari her sonuc belgesine buyuk bir nesne olarak kopyalanmaz.
Secim, sorgularin birlikte okudugu alanlara gore yapilmistir.

## Tekrar aktarim ve tutarli yayinlama

1. Tek bir toplu aktarici ayni anda calisir; aktarim kosusuna bir kimlik verilir.
2. Kaynak Delta surumu sabitlenir; sensor boyut tablosunun icerik ozeti kaydedilir.
3. Sonuclar bu kosunun deterministik `_id` degerleriyle `replaceOne + upsert`
   benzeri bir yontemle yazilir. Ayni kosu ayni icerikle yeniden aktarilirsa
   belge sayisi artmaz. Yeni hesaplama yeni kosu kimligi alir.
4. Kaynak/join ve koleksiyon sayimlari, anahtarlar ve kayitli degerler dogrulanir.
5. Yalnizca tum koleksiyonlar hazirsa `aktif_toplu` belgesi tek yazmayla
   yeni kosuyu gosterecek sekilde degistirilir.
6. Pano bir yenilemede kosu kimligini bir kez alip tum sorgularini o kimlikle
   yapar. Eski kosular okuma devam ederken silinmez.

Bu, tek aktarici ve bu okuma protokolu icin uygulama duzeyinde yayinlama
kararidir. Tum koleksiyonlari kapsayan bir MongoDB transaction'i veya
uc uca exactly-once garantisi olarak sunulmaz. Aktif isaretci degismeden
kesilen aktarim eski tamamlanmis kosuyu etkilememelidir; sonraki asamada
uygulanmasi ve dogrulanmasi gerekir. Yerel servis standalone oldugundan
cok belgeli transaction/replika guvencesi varsayilmaz.

## Shard anahtari ve ilk fazdaki sicak nokta bulgulari

Yerel Compose MongoDB'si **tek standalone sunucudur**; replika seti ve
sharded cluster kurulmus degildir. Burada shard dagilimi olctugumuzu
soylemeyiz. Bu kucuk ozet koleksiyonlari icin shard kurmak gerekli degildir.

Ilk fazdaki ham veri, Spark `pmod(hash(anahtar), 8)` deneyi:

| Aday | En dolu bolum / ortalama | Bos bolum |
|---|---:|---:|
| `sensor_id` | 1.3399626667 | 0 |
| `bolge` | 3.4362693333 | 3 |
| `tarih` | 2.2857333333 | 1 |
| `bolge + sensor_id` | 1.232488 | 0 |

Kaynaklar: `kanit/olcumler/iot_dagilim_kontrol.json` ve
`kanit/olcumler/iot_tarih_dagilimi.json`. Bunlar MongoDB hash algoritmasi,
gercek shard dagilimi veya query gecikmesi olcumu degildir. Yedi bolgeli ve
14 gunluk adaylarin dusuk kardinalitesi riskini gosteren girdilerdir.

Uretimde sensor bazinda detay/son-durum koleksiyonu buyurse, aday
`{sensor_id: 'hashed'}` anahtaridir: sensor sorgularini hedeflemeye ve
tek basina monoton zaman anahtarinin son shard'a yazma yigmasini
onlemeye uygundur. Tek cok sicak sensorun yukunu kendi basina bolmez;
bolgeye gore sorgular birden fazla shard'a gidebilir. Mevcut 200 sensorun
kardinalitesi sinirlidir; buyume ve shard sayisiyla tekrar olculmelidir.

Ozet koleksiyonlari buyurse aday, saatlik seri icin sabit genislikte
kovalar yerine gercek `(bolge, tarih/saat penceresi)` dogal anahtarini
temsil eden `ozet_anahtari` alaninin hash'idir. Bunun yalnizca gunumuzdeki
7x24 ozetlere uygulanmasi kapasite gereksinimi olusturmaz; sorgu dagitma
maliyetiyle birlikte yeni bir shard deneyinde degerlendirilmelidir.
Tek `bolge` veya monoton `tarih` ranged shard anahtari secilmez.

## Akis baglantisi ve uygulama durumu

- MongoDB baslatildi ve `ping` sonucu `{ok: 1}` goruldu.
- Bu notta belirtilen koleksiyonlar, indeksler ve aktarim protokolu
  **tasarimdir**; veri aktarimi ve olcumler sonraki adimlardir.
- Akis sonuclari henuz bu koleksiyonlara karistirilmaz. Kisi 3 ile pencere
  anahtari, guncelleme/revizyon ve tekrar yazma semantigi belirlendikten
  sonra ayri akis koleksiyonlari ve metaverisi tasarlanacak.
- Panonun 30 saniyede bir MongoDB'yi okumasi, toplu isin de 30 saniyede
  bir yeniden hesaplandigi anlamina gelmez. Veri araligi, uretim zamani
  ve yenileme zamani panoda ayri gosterilecek.

## Teknik kaynaklar

- MongoDB gomme modeli: https://www.mongodb.com/docs/manual/data-modeling/embedding/
- MongoDB veri modeli iliskileri: https://www.mongodb.com/docs/manual/data-modeling/schema-design-process/map-relationships/
- MongoDB sharding: https://www.mongodb.com/docs/manual/sharding/
- Monoton anahtar ve hashed indeks: https://www.mongodb.com/docs/manual/core/indexes/index-types/index-hashed/
