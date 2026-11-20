#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kişi 2 — Adım 3 (veriyi tanı) ve Adım 4 (ölçek, Z1) hesapları.

Yeni bir Spark/Kafka koşusu YAPMAZ. Girdileri depodaki kanıtlardan okur:
  kanit/senaryo.txt                         senaryo kartı (G211210553)
  kanit/olcumler/iot_ornek_20.jsonl         Kafka 'sensorler' konusundan okunan 20 mesaj (Adım 3.2)
  kanit/olcumler/iot_veri_kalitesi.json     3.000.000 kayıtlık toplu verinin kalite sayımı (Kişi 1)
  kanit/olcumler/iot_bicim_ozet.csv         JSON ↔ Parquet boyutları (Kişi 1, Adım 6)
  kanit/olcumler/iot_dagilim_ozet.csv       bölüm anahtarı dağılımı (Kişi 1, Adım 7)
  kanit/olcumler/sensorler.csv              boyut tablosu

Üretir:
  kanit/olcumler/kisi2_olcek_hesabi.csv / .json   Z1 hesap tablosu (her satırda formül ve kaynak)
  kanit/olcumler/kisi2_kusur_ornekleri.json       dört kusurun veriden örnekleri ve sayıları
  kanit/olcumler.csv                              'olcek' ve 'kusur' deney satırları eklenir (varsa yenilenir)

Çalıştırma (depo kökünde, yalnız standart kütüphane):  python3 analiz/kisi2_veri_ve_olcek.py
"""
import csv, json, re, statistics as st
from collections import Counter
from datetime import datetime
from pathlib import Path

KOK = Path(__file__).resolve().parent.parent
K = KOK / "kanit"
O = K / "olcumler"
GRUP = "G211210553"


def oku_json(p):
    return json.loads(Path(p).read_text(encoding="utf-8-sig"))


def oku_csv(p):
    with open(p, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def sayi(metin):
    return int(metin.replace(".", ""))


# ── 1. Senaryo kartı ─────────────────────────────────────────────────────────
kart = (K / "senaryo.txt").read_text(encoding="utf-8-sig")
assert GRUP in kart, "senaryo kartı beklenen grup koduna ait değil"
hiz = sayi(re.search(r"saniyede ([\d.]+) ölçüm", kart).group(1))
tepe_kat = int(re.search(r"ortalamanın (\d+) katı", kart).group(1))
saklama_gun = int(re.search(r"ham veri (\d+) gün", kart).group(1))
gec_oran = int(re.search(r"olayların %(\d+)'i en fazla", kart).group(1)) / 100
gec_dk = int(re.search(r"en fazla (\d+) dakika geç", kart).group(1))
uyari_s = int(re.search(r"en geç (\d+) saniyede", kart).group(1))

# ── 2. Olay başına bayt — iki bağımsız ölçüm ────────────────────────────────
satirlar = [s for s in (O / "iot_ornek_20.jsonl").read_bytes().decode("utf-8-sig").splitlines() if s.strip()]
ornek = [json.loads(s) for s in satirlar]
akis_bayt = [len(s.encode("utf-8")) for s in satirlar]           # Kafka mesaj değeri (satır sonu yok)
kalite = oku_json(O / "iot_veri_kalitesi.json")
toplu_kayit = kalite["olcumler"]["toplam_dolu_satir"]
toplu_bayt = kalite["olcumler"]["toplam_dosya_bayti"]
bayt_olay = toplu_bayt / toplu_kayit                             # JSONL dosyası, satır sonu dahil

bicim = {r["bicim"]: r for r in oku_csv(O / "iot_bicim_ozet.csv")}
oran = {b: int(r["bayt"]) / int(bicim["JSON"]["bayt"]) for b, r in bicim.items()}

# ── 3. Ölçek hesabı (Z1) ─────────────────────────────────────────────────────
GB, TB, PB, MB = 1e9, 1e12, 1e15, 1e6        # onluk birimler (H1 §4: disk üreticisi birimi)
# H1 §5 aktarım hızları (MB/s) — 'tipik büyüklük mertebeleri'
HIZ_H1 = {"NVMe SSD (PCIe 4.0)": 5000, "SATA SSD": 550, "10 Gb/s ağ": 1250, "1 Gb/s ağ": 125}


def esik(bayt):
    """H1 §7 'Tek makine 2026'da nereye kadar gider?' tablosundaki bant."""
    if bayt < 1 * GB: return "< 1 GB — belleğe rahat sığar (pandas)"
    if bayt < 10 * GB: return "1–10 GB — dizüstü belleğine sığar (Polars/DuckDB + Parquet)"
    if bayt < 100 * GB: return "10–100 GB — diskten akıtarak tek makine (DuckDB/Polars)"
    if bayt < 500 * GB: return "100–500 GB — diske sığar, hâlâ tek makine"
    if bayt < 3 * TB: return "500 GB – birkaç TB — tek sunucu zorlanır: ölçün, karar verin"
    return "> birkaç TB, sürekli büyüyor — dağıtık sistem gerekli"


satir = []


def ekle(olcek, metrik, deger, birim, formul, kaynak):
    satir.append(dict(olcek=olcek, metrik=metrik, deger=deger, birim=birim, formul=formul, kaynak=kaynak))


for kat in (1, 10, 100):
    o = f"{kat}x"
    r = hiz * kat
    gun_olay = r * 86_400
    gun_json = gun_olay * bayt_olay
    top_json = gun_json * saklama_gun
    ekle(o, "ortalama_hiz", r, "olay/s", f"kart {hiz} × {kat}", "kanit/senaryo.txt")
    ekle(o, "tepe_hiz", r * tepe_kat, "olay/s", f"ortalama × {tepe_kat}", "kanit/senaryo.txt")
    ekle(o, "gunluk_olay", gun_olay, "olay/gün", "ortalama × 86.400", "hesap")
    ekle(o, "ortalama_giris_json", r * bayt_olay / MB, "MB/s", "ortalama × bayt/olay", "hesap")
    ekle(o, "tepe_giris_json", r * tepe_kat * bayt_olay / MB, "MB/s", "tepe × bayt/olay", "hesap")
    ekle(o, "gunluk_json", gun_json / GB, "GB/gün", "günlük olay × bayt/olay", "hesap")
    ekle(o, "saklanan_json", top_json / TB, "TB", f"günlük × {saklama_gun} gün", "hesap")
    for b in ("Parquet-zstd", "Parquet-snappy"):
        ekle(o, f"gunluk_{b}", gun_json * oran[b] / GB, "GB/gün", f"günlük JSON × {oran[b]:.4f}", "kanit/olcumler/iot_bicim_ozet.csv")
        ekle(o, f"saklanan_{b}", top_json * oran[b] / TB, "TB", f"saklanan JSON × {oran[b]:.4f}", "kanit/olcumler/iot_bicim_ozet.csv")
    zstd_gun = gun_json * oran["Parquet-zstd"]
    for ad, mbs in HIZ_H1.items():
        ekle(o, f"bir_gun_zstd_okuma_{ad}", zstd_gun / MB / mbs / 60, "dakika", f"günlük zstd ÷ {mbs} MB/s", "H1 §5")
    ekle(o, "esik_gunluk_zstd", esik(zstd_gun), "bant", "H1 §7", "H1 §7")
    ekle(o, "esik_gunluk_json", esik(gun_json), "bant", "H1 §7", "H1 §7")
    ekle(o, "esik_saklanan_zstd", esik(top_json * oran["Parquet-zstd"]), "bant", "H1 §7", "H1 §7")

# kit verisi (karşılaştırma için)
ekle("kit", "toplu_json", toplu_bayt / GB, "GB", "ölçülen dosya baytı", "kanit/olcumler/iot_veri_kalitesi.json")
ekle("kit", "esik_toplu_json", esik(toplu_bayt), "bant", "H1 §7", "H1 §7")
ekle("kit", "kit_kac_saniyelik_gercek_veri", toplu_kayit / hiz, "s", f"{toplu_kayit} ÷ {hiz}", "hesap")

with open(O / "kisi2_olcek_hesabi.csv", "w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(satir[0]))
    w.writeheader(); w.writerows(satir)
(O / "kisi2_olcek_hesabi.json").write_text(json.dumps({
    "girdiler": {"grup": GRUP, "ortalama_hiz_olay_s": hiz, "tepe_kat": tepe_kat, "saklama_gun": saklama_gun,
                 "bayt_olay_toplu": bayt_olay, "bayt_olay_akis_ortalama": st.mean(akis_bayt),
                 "bayt_olay_akis_min": min(akis_bayt), "bayt_olay_akis_max": max(akis_bayt),
                 "sikistirma_orani": oran},
    "notlar": "Onluk birimler (1 GB = 10^9 bayt). Bayt/olay toplu JSONL dosyasından (satır sonu dahil); "
              "akış örneği satır sonu hariçtir, iki ölçüm arasındaki ~1 bayt fark satır sonudur.",
    "satirlar": satir}, ensure_ascii=False, indent=1), encoding="utf-8")

# ── 4. Dört kusur — veriden örnekler ─────────────────────────────────────────
zaman = [datetime.fromisoformat(r["olay_zamani"].replace("Z", "+00:00")) for r in ornek]
en_yeni, gec = zaman[0], []
for i, (r, t) in enumerate(zip(ornek, zaman), 1):
    en_yeni = max(en_yeni, t)
    geride = (en_yeni - t).total_seconds()
    if geride > 60:                          # bölümler arası birkaç saniyelik karışma geç sayılmaz
        gec.append({"sira": i, "kayit": r, "en_yeni_olaydan_geride_s": round(geride, 1)})

sens = {k: v for k, v in kalite["sensor_sayimlari"].items()}
ort = st.mean(sens.values())
bolge = kalite["bolge_sayimlari"]
dagilim = {r["anahtar"]: r for r in oku_csv(O / "iot_dagilim_ozet.csv")}
kusur = {
    "carpik_anahtar": {
        "sensor_basina_olay_en_fazla": max(sens.values()), "en_fazla_sensor": max(sens, key=sens.get),
        "sensor_basina_olay_ortalama": ort, "en_fazla_bolu_ortalama": max(sens.values()) / ort,
        "en_buyuk_sensor_payi_yuzde": 100 * max(sens.values()) / sum(sens.values()),
        "esit_dagilimda_pay_yuzde": 100 / len(sens),
        "en_buyuk_bolge_payi_yuzde": 100 * max(bolge.values()) / sum(bolge.values()),
        "esit_dagilimda_bolge_payi_yuzde": 100 / len(bolge),
        "ortalamanin_yuzde_80_altindaki_sensor": sorted(k for k, v in sens.items() if v < 0.8 * ort),
        "hash_8_bolum_en_dolu_bolu_ortalama": {k: float(v["en_dolu_bolu_ortalama"]) for k, v in dagilim.items()},
        "kaynak": "kanit/olcumler/iot_veri_kalitesi.json (sensor_sayimlari, bolge_sayimlari), kanit/olcumler/iot_dagilim_ozet.csv",
    },
    "gec_olay": {"ornek_mesaj_sayisi": len(ornek), "60_s_den_fazla_geride": gec,
                 "kart": f"olayların %{round(gec_oran*100)}'i en fazla {gec_dk} dk geç",
                 "kaynak": "kanit/olcumler/iot_ornek_20.jsonl (Kafka 'sensorler', okunma sırası)"},
    "tekrar": {"fazladan_kopya": kalite["olcumler"]["tekrar_fazla_kopya"],
               "oran_yuzde": 100 * kalite["olcumler"]["tekrar_fazla_kopya"] / toplu_kayit,
               "ornek": kalite["ornekler"]["tekrar"], "kaynak": "kanit/olcumler/iot_veri_kalitesi.json"},
    "kirli_kayit": {"eksik_alanli": kalite["olcumler"]["eksik_alanli_kayit"],
                    "null_degerli": kalite["olcumler"]["null_degerli_kayit"],
                    "en_az_bir_kusurlu": kalite["olcumler"]["en_az_bir_eksik_null_bos_iceren"],
                    "oran_yuzde": 100 * kalite["olcumler"]["en_az_bir_eksik_null_bos_iceren"] / toplu_kayit,
                    "ornek_eksik": kalite["ornekler"]["eksik_alanli_kayit"],
                    "ornek_null": kalite["ornekler"]["null_degerli_kayit"],
                    "kaynak": "kanit/olcumler/iot_veri_kalitesi.json"},
}
(O / "kisi2_kusur_ornekleri.json").write_text(json.dumps(kusur, ensure_ascii=False, indent=1, default=str), encoding="utf-8")

# ── 5. Ortak ölçüm tablosuna satırlar (grubun biçimi) ────────────────────────
yeni = []
for i, b in enumerate(akis_bayt, 1):
    yeni.append(["olcek", "akis_ornek_20", "mesaj_bayti", i, b, "bayt",
                 "olcumler/iot_ornek_20.jsonl", "Kafka mesaj degeri UTF-8 bayt; satir sonu haric"])
yeni.append(["olcek", "akis_ornek_20", "ortalama_mesaj_bayti", len(akis_bayt), st.mean(akis_bayt), "bayt/olay",
             "olcumler/iot_ornek_20.jsonl", "20 mesajin ortalamasi"])
yeni.append(["olcek", "toplu_ham", "ortalama_kayit_bayti", 1, bayt_olay, "bayt/olay",
             "olcumler/iot_veri_kalitesi.json", "toplam_dosya_bayti / toplam_dolu_satir; satir sonu dahil; Z1 hesabinin girdisi"])
k = kusur["carpik_anahtar"]
yeni += [
    ["kusur", "carpik_anahtar", "sensor_en_fazla_bolu_ortalama", "", k["en_fazla_bolu_ortalama"], "oran",
     "olcumler/iot_veri_kalitesi.json", "H3 karar rehberi: > 1,5 ise sicak nokta"],
    ["kusur", "carpik_anahtar", "en_buyuk_sensor_payi", "", k["en_buyuk_sensor_payi_yuzde"], "yuzde",
     "olcumler/iot_veri_kalitesi.json", f"esit dagilimda {k['esit_dagilimda_pay_yuzde']:.2f}"],
    ["kusur", "carpik_anahtar", "ortalamanin_yuzde80_altindaki_sensor", "", len(k["ortalamanin_yuzde_80_altindaki_sensor"]),
     "sensor", "olcumler/iot_veri_kalitesi.json", "az olay gonderen sensorler (kayip/susma adayi)"],
    ["kusur", "gec_olay", "60s_den_fazla_geride_mesaj", len(ornek), len(gec), "mesaj",
     "olcumler/iot_ornek_20.jsonl", "okunma sirasinda o ana kadarki en yeni olay_zamani'ndan geride"],
]
yeni += [["kusur", "gec_olay", "en_yeni_olaydan_geride", g["sira"], g["en_yeni_olaydan_geride_s"], "saniye",
          "olcumler/iot_ornek_20.jsonl", g["kayit"]["sensor_id"]] for g in gec]

yol = K / "olcumler.csv"
with open(yol, encoding="utf-8-sig", newline="") as f:
    eski = list(csv.reader(f))
baslik, govde = eski[0], [r for r in eski[1:] if r and r[0] not in ("olcek", "kusur")]
with open(yol, "w", encoding="utf-8-sig", newline="") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(baslik); w.writerows(govde); w.writerows(yeni)

print(f"bayt/olay toplu {bayt_olay:.2f} · akış ort {st.mean(akis_bayt):.2f} ({min(akis_bayt)}–{max(akis_bayt)})")
for r in satir:
    if r["olcek"] in ("1x", "kit") or r["metrik"].startswith(("gunluk_json", "saklanan", "gunluk_Parquet-zstd", "esik")):
        d = r["deger"]
        print(f"{r['olcek']:>4} {r['metrik']:<40} {d if isinstance(d, str) else f'{d:,.2f}'} {r['birim']}")
print(f"geç olay: {[(g['sira'], g['en_yeni_olaydan_geride_s']) for g in gec]}")
print(f"çarpık: en fazla/ort {k['en_fazla_bolu_ortalama']:.3f}, az gönderen {len(k['ortalamanin_yuzde_80_altindaki_sensor'])} sensör")
print(f"olcumler.csv: {len(yeni)} satır eklendi")
