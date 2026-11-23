# -*- coding: utf-8 -*-
"""A4 · Z1 ölçek hesabı — zarf arkası, senaryo kartı + ölçülen olay boyutu.

  python analiz/olcek.py                    # ham JSON
  python analiz/olcek.py --parquet-orani 0.2   # A6'da ölçülen Parquet/JSON boyut oranıyla

Birimler ondalık: 1 MB = 10^6 bayt, 1 GB = 10^9, 1 TB = 10^12, 1 PB = 10^15.
"""
import argparse

# kanit/senaryo.txt
HIZ = 2_500          # ortalama olay/s
TEPE_CARPAN = 2      # en yoğun saat ÷ ortalama
SAKLAMA_GUN = 365
# kanit/olcumler.csv (A3, 1.502 mesaj)
BAYT_OLAY = 327

GUN_S = 86_400
GBIT_MB_S = 125      # 1 Gbit/s ağ kartı = 125 MB/s (tanım gereği: 10^9 bit / 8)

ap = argparse.ArgumentParser()
ap.add_argument("--parquet-orani", type=float, help="Parquet boyutu ÷ JSON boyutu (A6'da ölçülür)")
a = ap.parse_args()


def birim(b: float) -> str:
    for ad, k in (("PB", 1e15), ("TB", 1e12), ("GB", 1e9), ("MB", 1e6)):
        if b >= k:
            return f"{b / k:,.2f} {ad}"
    return f"{b:,.0f} B"


print(f"Girdi: {HIZ:,} olay/s · tepe ×{TEPE_CARPAN} · {BAYT_OLAY} B/olay · {SAKLAMA_GUN} gün saklama\n")
print(f"{'kat':>4} | {'olay/s ort':>10} | {'tepe olay/s':>11} | {'ort MB/s':>8} | {'tepe MB/s':>9} | "
      f"{'olay/gün':>14} | {'GB/gün':>9} | {'saklanan ham':>12}" + (" | Parquet" if a.parquet_orani else ""))
for kat in (1, 10, 100):
    hiz = HIZ * kat
    ort_bs = hiz * BAYT_OLAY
    tepe_bs = ort_bs * TEPE_CARPAN
    gun_b = ort_bs * GUN_S                 # günlük toplam ORTALAMA hızla: tepe yalnızca en yoğun saatte
    toplam = gun_b * SAKLAMA_GUN
    satir = (f"{kat:>3}× | {hiz:>10,} | {hiz * TEPE_CARPAN:>11,} | {ort_bs / 1e6:>8.2f} | {tepe_bs / 1e6:>9.2f} | "
             f"{hiz * GUN_S:>14,} | {gun_b / 1e9:>9,.2f} | {birim(toplam):>12}")
    if a.parquet_orani:
        satir += f" | {birim(toplam * a.parquet_orani)}"
    print(satir)
    tarama_s = toplam / (GBIT_MB_S * 1e6)   # H1 aktarım darboğazı: süre = boyut ÷ hız
    print(f"      tüm saklanan ham veriyi 125 MB/s ile bir kez okumak: {tarama_s / 3600:,.1f} saat "
          f"({tarama_s / GUN_S:,.1f} gün) · bir günlük veri: {gun_b / (GBIT_MB_S * 1e6) / 60:,.1f} dk")
    if tepe_bs / 1e6 > GBIT_MB_S:
        print(f"      ↑ tepe {tepe_bs / 1e6:.1f} MB/s > tek 1 Gbit/s ağ kartı ({GBIT_MB_S} MB/s)")
