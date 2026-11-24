# IoT toplu sonuc panosu (Z9)

## Dosyalar ve baslatma

- `araclar/iot_pano.py`: Python HTTP/API ve tarayicida cizilen SVG grafikler.
- `araclar/iot_mongo_sorgu_olcumu.py`: panonun kullandigi ortak sorgu tanimlari.
- Proje kokunde `compose.iot-pano.yml`: ana Compose dosyasina ek servis tanimi.
- MongoDB `bsm461_iot` veritabaninda tamamlanmis `aktif_toplu` kosusu bulunmali.
- Yeni bir bilgisayarda once temel kit ve Python araclar imaji kurulmalidir:
  `docker compose build araclar`. Gerekli PyMongo, araclar imajinda bulunmali.
- Yerel hazir kitte baslatma:

```powershell
docker compose -f docker-compose.yml -f compose.iot-pano.yml --profile servis --profile iot-pano up -d iot-pano
```

Adres: http://localhost:8050

Kontroller:

```powershell
Invoke-RestMethod http://localhost:8050/health
Invoke-RestMethod http://localhost:8050/api/toplu
docker compose -f docker-compose.yml -f compose.iot-pano.yml logs --tail 30 iot-pano
```

Yalnizca pano servisini durdurmak:

```powershell
docker compose -f docker-compose.yml -f compose.iot-pano.yml stop iot-pano
```

Ana Docker Compose, S3, Kafka ve Spark dosyalari degistirilmez. Pano,
bilgisayarin yalnizca loopback adresindeki 8050 portuna yayimlanir.
Bu Python HTTP sunucusu yerel ders projesi icindir; uretim web sunucusu,
kimlik dogrulama ve cok kullanicili yuk guvencesi olarak sunulmaz.

## Kaynak ve veri akisi

`s3a://tablolar/iot` temiz Delta surumu + `sensorler.csv` boyut tablosu
-> `is/iot_mongo_sonuclari.py` ile toplu ozetler
-> `araclar/iot_mongo_aktar.py` ile MongoDB
-> `servis_meta/aktif_toplu` kosu kimligi
-> bes sonuc koleksiyonu -> pano.

Pano ham olaylari, karantina kayitlarini veya Z7 deney verilerini okumaz.
Her yenilemede aktif kimligi bir kez alir ve tum grafikleri o kosu ile
filtreler. Yeni kosu tum koleksiyonlariyla hazir olmadan aktarici aktif
kimligi degistirmez. Onceki kosular pano okurken silinmez.

## Gosterilen bilgiler

- Kaynak temiz olay sayisi, analize giren sensor sayisi, anomali isaretli
  olay sayisi ve veri tarih araligi.
- Model bazinda anomali yuzdesi (olay agirlikli).
- Bolge secimiyle saatlik sicaklik ve nem ortalamalari; ayri birimler ve
  ayri grafikler. Nem grafiginde sabit yuzde 0-100 ekseni kullanilir;
  cok kucuk farklarin buyuk dalgalanma gibi gorunmesi onlenir.
  Saat UTC'dir; tum gunlerin ayni saatleri birlestirilir.
- Kurulum yili bazinda sensor yuzdelerinin esit agirlikli ortalamasi.
- Geceler/gunduzler arasindaki sicaklik karsilastirmasi.
- Kaynak Delta yolu/surumu, aktif kosu, sonuc uretim ve MongoDB yayin zamani.

Sicaklik/titresim esikleri MongoDB'deki aktif kosu metaverisinden alinir;
gosterilen anomali, dogrulanmis cihaz arizasi olarak adlandirilmaz.

## Yenileme ve guncellik siniri

Tarayici acilinca ve her 30 saniyede bir `/api/toplu` uzerinden MongoDB
tekrar okunur. `Simdi yenile` dugmesi elle ayni okumayi baslatir. Eszamanli
bir yenileme surerken ikinci yenileme baslatilmaz.

Bu otomatik **ekran yenilemesidir**. Spark toplu isi yeniden calistirilmaz.
Yeni tamamlanmis toplu kosu aktarilirsa sonraki yenilemede yeni sonuclar
gorunur. Veri tarih araligi, uretim zamani, son MongoDB okuma ve tarayicida
gosterim zamanlari birbirinden ayridir. Toplu veri, ekran yenileniyor diye
canli akis sayilmaz. Kisi 3'un akis hatti henuz bagli degildir.

MongoDB okunamazsa ekranda hata acikca gosterilir. Daha once goruntu varsa
son basarili goruntu tutulur ve bunun guncel kabul edilmemesi belirtilir.

6. adimdaki p99, MongoDB'den ayri ayri ozet okuma olcumudur. Pano API'sinin
birden fazla sorgu suresi, tarayici cizimi ve uretim trafigi icin gecikme
garantisi vermez. Pano bunlari olculmus gibi gostermez.

## Kaydedilecek kanitlar

1. Pano tarayicida acilinca kartlar ve model grafigi: `kanit/iot_pano_genel.png`.
2. Secili bolgenin saatlik grafik ve tabloları: `kanit/iot_pano_bolge.png`.
3. Kaynak/kosu/zaman bilgileri: `kanit/iot_pano_kaynak.png`.
4. Otomatik yenilemede aktif kosu ayniyken okuma zamaninin ilerledigini
   kaydetmek, ekranin yeniden veri istedigini gosterir. Kaynak olaylarin
   degistigini veya canli akis entegrasyonunu kanitlamaz.

## 24 Kasim 2026 yerel kontrol sonucu

- MongoDB ve `bsm461-iot-pano-1` konteynerleri baslatildi.
- `http://localhost:8050` tarayicida acildi; son sade tasarimin basligi
  `IoT Sensor Analizi` olarak goruldu.
- Genel gorunumde 2.988.010 temiz olay, 200 sensor, 50.021 anomali
  isaretli olay ve 2026-10-31 / 2026-11-13 veri araligi goruldu.
- Akdeniz'den Dogu Anadolu'ya gecildiginde sicaklik ve nem grafikleri
  degisti. Nem ekseninin yuzde 0-100 oldugu kontrol edildi.
- Kaynak bolumunde 193 ozet belge, 5 sonuc koleksiyonu,
  `s3a://tablolar/iot` surum 0 ve
  `20261124T185013Z_a62ee8dc` aktif kosusu goruldu.
- Hicbir dugmeye basmadan beklenince son MongoDB okuma zamaninin
  22:27:25'ten 22:28:25'e ilerledigi kullanici tarafindan gozlemlendi
  (Europe/Istanbul, GMT+3). Bu otomatik yenilemeyi dogrular; her istegin
  zamanlamasinin bagimsiz yuk altinda olculdugu anlamina gelmez.
- Genel, bolge ve kaynak ekran goruntulerinin yukaridaki adlarla
  `kanit/` klasorune kaydedildigi kullanici tarafindan bildirildi.
- Canli akis baglantisi, Kisi 3'un ciktilariyla sonraki entegrasyon
  adimina kaldi. Bu pano mevcut haliyle toplu sonuclari gosterir.
