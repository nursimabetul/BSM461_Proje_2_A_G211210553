#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kişi 2 şekilleri.

  1) kanit/grafikler/kisi2_z1_esik.png   — Z1: hacimlerin H1 §7 eşik bantlarındaki yeri (1×, 10×, 100×)
  2) kanit/mimari/omurga_taslak.svg/.png — Z0: omurga şemasının IoT taslağı (graphviz 'dot' gerekir)

Girdi: kanit/olcumler/kisi2_olcek_hesabi.csv (önce analiz/kisi2_veri_ve_olcek.py çalıştırılmalı).
"""
import csv, shutil, subprocess
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

KOK = Path(__file__).resolve().parent.parent
GRAF = KOK / "kanit" / "grafikler"; GRAF.mkdir(parents=True, exist_ok=True)
MIM = KOK / "kanit" / "mimari"; MIM.mkdir(parents=True, exist_ok=True)

YAZI = "Inter" if any("Inter" in f.name for f in font_manager.fontManager.ttflist) else "DejaVu Sans"
plt.rcParams.update({"font.family": YAZI, "font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.spines.left": False, "svg.fonttype": "none"})
MUREKKEP, IKINCIL, CIZGI = "#1f2328", "#5b6470", "#d0d4da"
RENK = {"1x": "#0f6e6e", "10x": "#c46a1a", "100x": "#8a2d3b"}

with open(KOK / "kanit/olcumler/kisi2_olcek_hesabi.csv", encoding="utf-8") as f:
    H = {(r["olcek"], r["metrik"]): r for r in csv.DictReader(f)}


def deger(o, m):
    return float(H[(o, m)]["deger"])


# ── 1) Z1 eşik grafiği ───────────────────────────────────────────────────────
GB, TB = 1e9, 1e12
bantlar = [(1e8, 1 * GB, "< 1 GB\nbelleğe sığar"), (1 * GB, 10 * GB, "1–10 GB\ndizüstü belleği"),
           (10 * GB, 100 * GB, "10–100 GB\ndiskten akıtarak"), (100 * GB, 500 * GB, "100–500\nGB diske"),
           (500 * GB, 3 * TB, "0,5–3 TB\nzorlanır"), (3 * TB, 3e16, "> birkaç TB, sürekli büyüyor\ndağıtık sistem gerekli")]
fig, ax = plt.subplots(figsize=(10, 4.6), dpi=200)
for i, (a, b, ad) in enumerate(bantlar):
    son = i == len(bantlar) - 1
    ax.axvspan(a, b, color="#8a2d3b" if son else "#0f6e6e", alpha=0.10 if son else 0.03 + 0.012 * i, lw=0)
    ax.axvline(a, color=CIZGI, lw=0.8, zorder=0)
    x = (a * b) ** 0.5 if not son else 3e14
    ax.text(x, 3.62, ad, ha="center", va="bottom", fontsize=7.6, color=IKINCIL, linespacing=1.25)

satirlar = [("Kit verisi (3 M kayıt, JSON)", None), ("Bir günlük veri · Parquet-zstd", "gunluk_Parquet-zstd"),
            ("Bir günlük veri · JSON", "gunluk_json"), ("1.095 günlük arşiv · Parquet-zstd", "saklanan_Parquet-zstd")]
for y, (ad, m) in zip([3, 2, 1, 0], satirlar):
    ax.text(1.15e8, y + 0.30, ad, fontsize=9, color=MUREKKEP, fontweight="medium", va="bottom")
    ax.axhline(y, color=CIZGI, lw=0.6, zorder=1)
    if m is None:
        v = deger("kit", "toplu_json") * GB
        ax.scatter([v], [y], s=46, color=IKINCIL, zorder=3)
        ax.annotate("0,43 GB", (v, y), xytext=(0, -14), textcoords="offset points", ha="center", fontsize=8, color=IKINCIL)
        continue
    carpan = TB if m.startswith("saklanan") else GB
    for o in ("1x", "10x", "100x"):
        v = deger(o, m) * carpan
        ax.scatter([v], [y], s=56, color=RENK[o], zorder=3, edgecolor="white", linewidth=0.8)
        etiket = f"{v / 1e15:,.1f} PB" if v >= 1e15 else (f"{v / TB:,.1f} TB" if v >= TB else f"{v / GB:,.0f} GB")
        ax.annotate(etiket.replace(",", "X").replace(".", ",").replace("X", "."), (v, y), xytext=(0, -14),
                    textcoords="offset points", ha="center", fontsize=8, color=RENK[o])

ax.set_xscale("log"); ax.set_xlim(1e8, 3e16); ax.set_ylim(-0.6, 4.25); ax.set_yticks([])
ax.set_xticks([1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16])
ax.set_xticklabels(["1 GB", "10 GB", "100 GB", "1 TB", "10 TB", "100 TB", "1 PB", "10 PB"], color=IKINCIL)
ax.tick_params(axis="x", length=0)
for o, ad in [("1x", "kart (60.000 olay/s)"), ("10x", "10 katı"), ("100x", "100 katı")]:
    ax.scatter([], [], s=56, color=RENK[o], label=ad)
ax.legend(loc="upper right", frameon=False, fontsize=8.5, ncol=3, bbox_to_anchor=(1, 1.24))
fig.text(0.012, 0.965, "Bir gün tek makineye sığıyor, üç yıllık arşiv sığmıyor", fontsize=12.5, fontweight="semibold", color=MUREKKEP)
fig.text(0.012, 0.915, "Hacimlerin H1 §7 eşik tablosundaki yeri · onluk birim, logaritmik eksen · G211210553 senaryo kartı",
         fontsize=8.5, color=IKINCIL)
fig.text(0.012, 0.015, "Kaynak: kanit/olcumler/kisi2_olcek_hesabi.csv (143,57 bayt/olay ölçülen; zstd oranı 0,0804 Kişi 1 ölçümü)",
         fontsize=7.5, color=IKINCIL)
fig.subplots_adjust(left=0.012, right=0.99, top=0.80, bottom=0.12)
fig.savefig(GRAF / "kisi2_z1_esik.png"); plt.close(fig)
print("yazıldı:", GRAF / "kisi2_z1_esik.png")

# ── 2) Z0 omurga şeması (taslak) ─────────────────────────────────────────────
DOT = r'''digraph omurga {
  graph [rankdir=LR, splines=spline, nodesep=0.45, ranksep=0.55, pad=0.25, bgcolor="white",
         fontname="%(f)s", fontsize=11, labelloc=t, labeljust=l,
         label=<<FONT POINT-SIZE="15"><B>Omurga şeması — taslak (Adım 5)</B></FONT><BR/><FONT POINT-SIZE="10" COLOR="#5b6470">G211210553 · Şablon 3 IoT · kitteki bileşenler; <B>?</B> = henüz işlenmeyen katman (H8–H11), Adım 15'te kesinleşir</FONT><BR/> >];
  node  [shape=box, style="rounded,filled", fontname="%(f)s", fontsize=10, color="#c9ced6", fillcolor="#ffffff",
         penwidth=1.1, margin="0.16,0.08"];
  edge  [fontname="%(f)s", fontsize=8.5, color="#7a838f", fontcolor="#3c434c", arrowsize=0.7, penwidth=1.1];

  kaynak [label=<<B>① Olay kaynağı</B><BR/><FONT POINT-SIZE="9">200 sensör · 60.000 ölçüm/s<BR/>(kitte: araclar/uretici.py)</FONT>>, fillcolor="#eef6f6", color="#0f6e6e"];
  toplama [label=<<B>② Toplama</B><BR/><FONT POINT-SIZE="9">Kafka üreticisi<BR/>acks=all · idempotent</FONT>>];
  kuyruk [label=<<B>③ Günlük / Kuyruk</B><BR/><FONT POINT-SIZE="9">Apache Kafka 4.0.0 (KRaft)<BR/>konu: sensorler · anahtar: sensor_id<BR/>bölüm sayısı: ? (Z8)</FONT>>];
  akis [label=<<B>④ Akış işleme</B><BR/><FONT POINT-SIZE="9">Spark 3.5.3 Structured Streaming<BR/>pencere · watermark: ? (H11)</FONT>>, style="rounded,filled,dashed"];
  ham [label=<<B>⑤ Ham depolama</B><BR/><FONT POINT-SIZE="9">SeaweedFS 4.48 (S3 uyumlu)<BR/>s3://ham/iot/tarih=…/ · JSON Lines<BR/>işlenmiş: Parquet-zstd</FONT>>, fillcolor="#eef6f6", color="#0f6e6e"];
  toplu [label=<<B>⑥ Toplu işleme</B><BR/><FONT POINT-SIZE="9">Spark 3.5.3 (local) · Delta 3.2.1<BR/>temizle → join sensorler.csv → topla</FONT>>, fillcolor="#eef6f6", color="#0f6e6e"];
  servis [label=<<B>⑦ Servis deposu</B><BR/><FONT POINT-SIZE="9">? (aday: MongoDB 7.0 — H8)<BR/>p99 ≤ 50 ms</FONT>>, style="rounded,filled,dashed"];
  sunum [label=<<B>⑧ Sunum</B><BR/><FONT POINT-SIZE="9">? pano (Python grafikleri /<BR/>Grafana — Adım 12)</FONT>>, style="rounded,filled,dashed"];
  boyut [label=<<FONT POINT-SIZE="9"><B>Boyut tablosu</B><BR/>s3://ham/boyut/iot/sensorler.csv<BR/>200 satır</FONT>>, shape=note, fillcolor="#fafbfc"];

  kaynak -> toplama [label="  A  "];
  toplama -> kuyruk [label="  B  "];
  kuyruk -> akis [label="  C  "];
  akis -> servis [label="  D  ", style=dashed];
  servis -> sunum [label="  E  ", style=dashed];
  toplama -> ham [label="  F  "];
  ham -> toplu [label="  G  "];
  boyut -> toplu [style=dotted, arrowhead=none];
  toplu -> servis [label="  H  ", style=dashed];
  {rank=same; kaynak}
  {rank=same; toplama}
  {rank=same; kuyruk; ham}
  {rank=same; akis; toplu}
}
''' % {"f": YAZI}
(MIM / "omurga_taslak.dot").write_text(DOT, encoding="utf-8")
if shutil.which("dot"):
    subprocess.run(["dot", "-Tsvg", "-o", str(MIM / "omurga_taslak.svg"), str(MIM / "omurga_taslak.dot")], check=True)
    subprocess.run(["dot", "-Tpng", "-Gdpi=200", "-o", str(MIM / "omurga_taslak.png"), str(MIM / "omurga_taslak.dot")], check=True)
    print("yazıldı:", MIM / "omurga_taslak.svg", MIM / "omurga_taslak.png")
else:
    print("graphviz 'dot' yok: yalnız .dot yazıldı")
