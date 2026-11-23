# -*- coding: utf-8 -*-
"""A7 · Z3 — bölüm anahtarı adayları, sıcak nokta, çözüm (önce/sonra), tekrarın sonuca etkisi.

  docker compose exec spark spark-submit is/a7_bolum.py            # 3 tekrar
Girdi: A6'da yazılan Parquet gzip ölçüm kopyası (5.000.000 satır, temizlenmemiş — tekrarlar içinde).

Neden böyle:
  * Bölüm dağılımı Spark'ın kendi kuralıyla hesaplanır: pmod(hash(anahtar), 8) — groupBy/Window shuffle'ı
    (hashpartitioning) satırları tam olarak böyle dağıtır (H6 §5.4). 8 = kitteki spark.sql.shuffle.partitions.
  * Ölçüt H3 §11: en dolu bölüm ÷ ortalama > 1,5 → sıcak nokta.
  * Çarpıklığın bedeli için combiner'ın kurtaramadığı bir iş seçildi (H5 §6.1): her kaynağın ardışık iki haberi
    arasındaki süre. Pencere fonksiyonu kaynağın BÜTÜN satırlarını tek göreve toplar ve sıralar.
  * Çözüm: bileşik anahtar (kaynak, tarih) → ardışık süre gün içinde hesaplanır. Bedeli: günü aşan aralıklar
    kaybolur (kaynak başına gün sayısı kadar satır); sayısı yazılır.
  * Görev süreleri Spark UI REST API'sinden (H5 §6.2, H6 §4.5): aşamanın en uzun ↔ ortanca görevi.
"""
import argparse, csv, datetime, json, time, urllib.request
from pyspark.sql import SparkSession, Window, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--tekrar", type=int, default=3)
a = ap.parse_args()

spark = SparkSession.builder.appName("a7-bolum").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

GIRDI = "s3a://islenmis/haber_olcum/gzip"
B = 8
OLCUM = "/kit/kanit/olcumler.csv"
VERI = "5.000.000 satır (Parquet gzip · temizlenmemiş)"
BUGUN = datetime.date.today().isoformat()
satirlar = []


def kaydet(ne, sonuc, birim, nasil, kac=1):
    satirlar.append([BUGUN, "A7", ne, kac, sonuc, birim, VERI, f"is/a7_bolum.py · {nasil}"])
    print(f"  {ne:62s} {sonuc} {birim}")


df = spark.read.parquet(GIRDI)
n = df.count()

# ── 1. Aday anahtarlar ─────────────────────────────────────────────────────
print(f"1) Aday bölüm anahtarları · {B} bölüm · {n:,} satır")
ADAYLAR = {"kaynak": F.col("kaynak"), "kategori": F.col("kategori"), "tarih": F.col("tarih"),
           "haber_id": F.col("haber_id"), "kaynak+tarih": F.struct("kaynak", "tarih")}


def denge(anahtar):
    """(farklı değer, en büyük anahtarın payı, en dolu ÷ ortalama, boş bölüm) — Spark'ın hash bölümlemesiyle."""
    farkli = df.select(anahtar.alias("k")).distinct().count()
    en_buyuk = df.groupBy(anahtar.alias("k")).count().agg(F.max("count")).first()[0]
    bolum = {r["b"]: r["count"] for r in
             df.groupBy(F.pmod(F.hash(anahtar), F.lit(B)).alias("b")).count().collect()}
    dolu = [bolum.get(i, 0) for i in range(B)]
    return farkli, en_buyuk / n, max(dolu) / (n / B), sum(1 for d in dolu if d == 0), dolu


for ad, ifade in ADAYLAR.items():
    farkli, pay, oran, bos, dolu = denge(ifade)
    durum = "SICAK NOKTA" if oran > 1.5 else "dengeli"
    kaydet(f"bölüm dengesi · {ad} (en dolu ÷ ortalama, {B} bölüm)", f"{oran:.2f}", "oran",
           f"{farkli} farklı değer · en büyük anahtar %{100*pay:.1f} · boş bölüm {bos} · {durum} · bölümler {dolu}")

# ── 2. Tuzlama ile karşılaştırma için: kaynak + deterministik tuz ──────────
# Tuz rastgele değil haber_id'den: iş yeniden çalışınca aynı satır aynı kovaya gider (H5 §11: belirlenimci).
tuzlu = F.concat_ws("#", F.col("kaynak"), F.pmod(F.hash("haber_id"), F.lit(B)).cast("string"))
farkli, pay, oran, bos, dolu = denge(tuzlu)
kaydet(f"bölüm dengesi · kaynak#tuz(0..{B-1}) (en dolu ÷ ortalama)", f"{oran:.2f}", "oran",
       f"{farkli} farklı değer · boş bölüm {bos} · bölümler {dolu}")


# ── 3. Çarpıklığa duyarlı iş: kaynak başına ardışık haber arası süre ───────
def gorev_suresi(grup):
    """Grubun işlerindeki aşamalar içinde en uzun görevi olan aşamanın (en uzun, ortanca) görev süresi, saniye."""
    uygulama = sc.applicationId
    en_iyi = (0.0, 0.0)
    for jid in sc.statusTracker().getJobIdsForGroup(grup):
        for sid in sc.statusTracker().getJobInfo(jid).stageIds:
            url = f"{sc.uiWebUrl}/api/v1/applications/{uygulama}/stages/{sid}/0/taskSummary?quantiles=0.5,1.0"
            try:
                q = json.load(urllib.request.urlopen(url, timeout=5))["executorRunTime"]
            except Exception:
                continue                     # atlanan aşama (AQE) özet vermez
            if q[1] / 1000 > en_iyi[0]:
                en_iyi = (q[1] / 1000, q[0] / 1000)
    return en_iyi


def ardisik(bolum_anahtari):
    w = Window.partitionBy(*bolum_anahtari).orderBy("olay_zamani")
    return (df.select("kaynak", "tarih", "olay_zamani")
              .withColumn("ara_s", F.col("olay_zamani").cast("double") - F.lag(F.col("olay_zamani").cast("double")).over(w))
              .groupBy("kaynak").agg(F.avg("ara_s").alias("ort_ara_s"), F.count("ara_s").alias("aralik")))


print(f"2) Ardışık haber arası süre · {a.tekrar} tekrar")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")   # 8 bölüm sabit kalsın, ölçülen o olsun
sonuclar = {}
for ad, anahtar in {"kaynak": ["kaynak"], "kaynak+tarih": ["kaynak", "tarih"]}.items():
    sureler, oranlar = [], []
    for i in range(a.tekrar):
        grup = f"a7-{ad}-{i}"
        sc.setJobGroup(grup, grup)
        t0 = time.time()
        sonuc = {r["kaynak"]: (r["ort_ara_s"], r["aralik"]) for r in ardisik(anahtar).collect()}
        sureler.append(time.time() - t0)
        uzun, ortanca = gorev_suresi(grup)
        oranlar.append(uzun / ortanca if ortanca else float("nan"))
    sonuclar[ad] = sonuc
    kaydet(f"süre · ardışık haber arası, bölüm={ad}", f"{sum(sureler)/len(sureler):.2f}", "s",
           "turlar: " + " / ".join(f"{s:.2f}" for s in sureler), a.tekrar)
    kaydet(f"görev süresi en uzun ÷ ortanca · bölüm={ad}", f"{sum(oranlar)/len(oranlar):.2f}", "oran",
           "Spark UI taskSummary executorRunTime · turlar: " + " / ".join(f"{o:.2f}" for o in oranlar), a.tekrar)
sc.setJobGroup("diger", "diger")
kayip = sum(v[1] for v in sonuclar["kaynak"].values()) - sum(v[1] for v in sonuclar["kaynak+tarih"].values())
kaydet("bileşik anahtarın bedeli: kaybolan gün-aşırı aralık", kayip, "aralık",
       "kaynak bölümünde aralık sayısı − kaynak+tarih bölümünde")
print("   kaynak başına ortalama ara (s):",
      {k: round(v[0], 2) for k, v in sorted(sonuclar["kaynak"].items(), key=lambda x: x[0] or "")})

# ── 4. Tekrar sonucu değiştiriyor mu? ─────────────────────────────────────
print("3) Tekrarın etkisi")
ham = {r["kategori"]: r["count"] for r in df.groupBy("kategori").count().collect()}
tekil = {r["kategori"]: r["count"] for r in df.dropDuplicates().groupBy("kategori").count().collect()}
fark = {k: ham[k] - tekil.get(k, 0) for k in ham}
en = max(fark, key=lambda k: fark[k] / ham[k])
kaydet("tekrar · toplam satır ham / tekilleştirilmiş", f"{sum(ham.values())} / {sum(tekil.values())}", "satır",
       "dropDuplicates() birebir satıra göre")
kaydet("tekrar · kategori sayımında en büyük fazla sayma", f"%{100*fark[en]/ham[en]:.3f} ({en}: +{fark[en]})",
       "oran", "ham − tekil, kategori başına")

with open(OLCUM, "a", newline="", encoding="utf-8") as f:
    csv.writer(f).writerows(satirlar)
print(f"{len(satirlar)} satır → kanit/olcumler.csv")
spark.stop()
