import os
import pandas as pd
import matplotlib.pyplot as plt

# --------------------------------------------------
# 1. DOSYA YOLLARI
# --------------------------------------------------

KLASOR = os.path.join("kanit", "grafikler")

model_verisi = pd.read_csv(
    os.path.join(KLASOR, "batch1_model.csv")
)

yil_verisi = pd.read_csv(
    os.path.join(KLASOR, "batch1_yil.csv")
)

saatlik_veri = pd.read_csv(
    os.path.join(KLASOR, "batch2_saatlik.csv")
)

# Grafiklerin kaydedileceği klasör
os.makedirs(KLASOR, exist_ok=True)


# --------------------------------------------------
# 2. BATCH 1 - ANOMALİ ANALİZİ
# --------------------------------------------------

fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Model bazında anomali oranı
model_verisi = model_verisi.sort_values(
    "anomali_orani", ascending=False
)

axes[0].bar(
    model_verisi["model"].astype(str),
    model_verisi["anomali_orani"]
)

axes[0].set_title("Sensör Modellerine Göre Anomali Oranı")
axes[0].set_xlabel("Sensör Modeli")
axes[0].set_ylabel("Anomali Oranı (%)")
axes[0].grid(axis="y", alpha=0.3)

# Kurulum yılı bazında anomali oranı
yil_verisi = yil_verisi.sort_values("kurulum_yili")

axes[1].bar(
    yil_verisi["kurulum_yili"].astype(str),
    yil_verisi["ortalama_sensor_anomali_orani"]
)

axes[1].set_title("Kurulum Yılına Göre Anomali Oranı")
axes[1].set_xlabel("Kurulum Yılı")
axes[1].set_ylabel("Anomali Oranı (%)")
axes[1].grid(axis="y", alpha=0.3)

fig.suptitle(
    "Batch 1 - Sensör Anomali Analizi",
    fontsize=15
)

plt.tight_layout()

fig.savefig(
    os.path.join(KLASOR, "batch1_anomali_analizi.png"),
    dpi=300,
    bbox_inches="tight"
)

plt.close(fig)


# --------------------------------------------------
# 3. BATCH 2 - SAATLİK BÖLGESEL ANALİZ
# --------------------------------------------------

fig, axes = plt.subplots(2, 1, figsize=(13, 11))

bolgeler = sorted(saatlik_veri["bolge"].unique())

# Saatlik sıcaklık değişimi
for bolge in bolgeler:
    veri = saatlik_veri[
        saatlik_veri["bolge"] == bolge
    ].sort_values("saat")

    axes[0].plot(
        veri["saat"],
        veri["ortalama_sicaklik"],
        marker="o",
        markersize=3,
        label=bolge
    )

axes[0].set_title("Bölgelere Göre Saatlik Ortalama Sıcaklık")
axes[0].set_xlabel("Saat")
axes[0].set_ylabel("Ortalama Sıcaklık (°C)")
axes[0].set_xticks(range(24))
axes[0].grid(alpha=0.3)
axes[0].legend(ncol=4, fontsize=8)

# Saatlik nem değişimi
for bolge in bolgeler:
    veri = saatlik_veri[
        saatlik_veri["bolge"] == bolge
    ].sort_values("saat")

    axes[1].plot(
        veri["saat"],
        veri["ortalama_nem"],
        marker="o",
        markersize=3,
        label=bolge
    )

axes[1].set_title("Bölgelere Göre Saatlik Ortalama Nem")
axes[1].set_xlabel("Saat")
axes[1].set_ylabel("Ortalama Nem (%)")
axes[1].set_xticks(range(24))
axes[1].grid(alpha=0.3)
axes[1].legend(ncol=4, fontsize=8)

fig.suptitle(
    "Batch 2 - Bölgesel Saatlik Analiz",
    fontsize=15
)

plt.tight_layout()

fig.savefig(
    os.path.join(KLASOR, "batch2_saatlik_bolge_analizi.png"),
    dpi=300,
    bbox_inches="tight"
)

plt.close(fig)


# --------------------------------------------------
# 4. TAMAMLANMA
# --------------------------------------------------

print("\nGrafikler başarıyla oluşturuldu:")
print("1. kanit/grafikler/batch1_anomali_analizi.png")
print("2. kanit/grafikler/batch2_saatlik_bolge_analizi.png")
