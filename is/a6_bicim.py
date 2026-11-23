# -*- coding: utf-8 -*-
"""A6 · Z2 — JSON ↔ Parquet (snappy / zstd / gzip): boyut, dosya sayısı, yazma ve sorgu süresi.

  docker compose exec spark spark-submit is/a6_bicim.py            # 3 tekrar (yönerge: en az 3)
  docker compose exec spark spark-submit is/a6_bicim.py --tekrar 5

Ne ölçülüyor, neden böyle:
  * Aynı ham veri (s3://ham/haber/) her biçimde birebir aynı içerikle yazılır — temizleme YOK (o A8'in kararı);
    karşılaştırılan tek şey biçim/kodek.
  * JSON da şema elle verilerek okunur; şema çıkarımı veriyi bir kez fazladan okur, o maliyet JSON'a haksızlık olurdu.
  * Sorgu iki sütuna dokunur (kategori, kaynak) → Parquet'in sütun budaması ve filtre itmesi devreye girer (H2 §8).
  * Her ölçüm --tekrar kez yapılır; hepsi ve ortalaması kanit/olcumler.csv'ye eklenir.
Parquet tarih'e göre bölümlenir (iskeletle aynı düzen; Z4 kararı ayrıca tartışılacak).
"""
import argparse, csv, datetime, time
from pyspark.sql import SparkSession, functions as F, types as T

ap = argparse.ArgumentParser()
ap.add_argument("--tekrar", type=int, default=3)
a = ap.parse_args()

spark = SparkSession.builder.appName("a6-bicim").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

HAM = "s3a://ham/haber/"
HEDEF = "s3a://islenmis/haber_olcum/{}"          # ölçüm kopyaları; asıl işlenmiş veri A8'de yazılır
KODEKLER = ["snappy", "zstd", "gzip"]
OLCUM = "/kit/kanit/olcumler.csv"
VERI = "5.000.000 satır (14 gün)"
BUGUN = datetime.date.today().isoformat()

SEMA = T.StructType([T.StructField(ad, T.StringType()) for ad in
                     ["olay_zamani", "haber_id", "kaynak", "kategori", "baslik", "metin"]])

jvm, conf = spark._jvm, spark._jsc.hadoopConfiguration()


def boyut(yol):
    """S3'teki klasörün toplam baytı ve (meta dosyalar hariç) veri dosyası sayısı."""
    p = jvm.org.apache.hadoop.fs.Path(yol)
    fs = p.getFileSystem(conf)
    dosyalar = fs.listFiles(p, True)
    toplam = adet = 0
    while dosyalar.hasNext():
        f = dosyalar.next()
        ad = f.getPath().getName()
        if ad.startswith("_") or ad.startswith("."):
            continue
        toplam += f.getLen()
        adet += 1
    return toplam, adet


def sorgu(df):
    return df.filter(F.col("kategori") == "ekonomi").groupBy("kaynak").count().collect()


def sure(fn):
    t0 = time.time()
    sonuc = fn()
    return time.time() - t0, sonuc


satirlar = []


def kaydet(ne, sonuclar, birim, nasil):
    ort = sum(sonuclar) / len(sonuclar)
    tekiller = " / ".join(f"{s:.2f}" for s in sonuclar)
    satirlar.append([BUGUN, "A6", ne, len(sonuclar), f"{ort:.3f}", birim, VERI, f"{nasil} · turlar: {tekiller}"])
    print(f"  {ne:55s} ort {ort:8.3f} {birim}   ({tekiller})")


ham = spark.read.schema(SEMA).json(HAM)

# ── 1. Parquet'e yaz: her kodek için bir kez (yazma süresi + boyut) ─────────
print("Parquet yazılıyor…")
parq = ham.withColumn("olay_zamani", F.to_timestamp("olay_zamani")).withColumn("tarih", F.to_date("olay_zamani"))
for kod in KODEKLER:
    yol = HEDEF.format(kod)
    t, _ = sure(lambda: parq.write.mode("overwrite").option("compression", kod).partitionBy("tarih").parquet(yol))
    b, n = boyut(yol)
    satirlar.append([BUGUN, "A6", f"Parquet {kod} yazma süresi (JSON'dan dönüştürme)", 1, f"{t:.1f}", "s", VERI,
                     "is/a6_bicim.py · tek ölçüm (yazma)"])
    satirlar.append([BUGUN, "A6", f"Parquet {kod} toplam boyut", 1, f"{b/1e6:.1f}", "MB", VERI,
                     f"is/a6_bicim.py · {b} bayt"])
    satirlar.append([BUGUN, "A6", f"Parquet {kod} dosya sayısı / ortalama dosya", 1, f"{n} / {b/n/1e6:.1f}",
                     "adet / MB", VERI, "is/a6_bicim.py · partitionBy(tarih)"])
    print(f"  {kod:7s} yazma {t:6.1f} s · {b/1e6:8.1f} MB · {n} dosya")

jb, jn = boyut(HAM)
print(f"  json    {jb/1e6:8.1f} MB · {jn} dosya")

# ── 2. Aynı sorgu, her biçimde --tekrar kez ────────────────────────────────
print(f"Sorgu: kategori='ekonomi' → kaynak başına sayım · {a.tekrar} tekrar")
kaynaklar = {"JSON": lambda: spark.read.schema(SEMA).json(HAM)}
for kod in KODEKLER:
    kaynaklar[f"Parquet {kod}"] = (lambda y: lambda: spark.read.parquet(y))(HEDEF.format(kod))

beklenen = None
for ad, oku in kaynaklar.items():
    sureler = []
    for _ in range(a.tekrar):
        t, sonuc = sure(lambda: sorgu(oku()))
        sureler.append(t)
        # kaynak'ı boş kirli kayıtlar None grubu oluşturur; None metinle sıralanamaz → "" ile sırala
        sonuc = sorted(((r["kaynak"], r["count"]) for r in sonuc), key=lambda x: (x[0] or "", x[1]))
        assert beklenen is None or sonuc == beklenen, f"{ad} farklı sonuç verdi!"   # biçim sonucu değiştirmemeli
        beklenen = sonuc
    kaydet(f"sorgu süresi {ad} (ekonomi · kaynak başına sayım)", sureler, "s", "is/a6_bicim.py")

print("Sonuç (her biçimde aynı):", beklenen)
with open(OLCUM, "a", newline="", encoding="utf-8") as f:
    csv.writer(f).writerows(satirlar)
print(f"{len(satirlar)} satır → kanit/olcumler.csv")
spark.stop()
