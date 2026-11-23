# -*- coding: utf-8 -*-
"""A6 · Z10 — Ham veride kirli kayıt ve tekrar sayımı + KVKK denetimi (milyonlarca kayıtta).

  docker compose exec spark spark-submit is/a6_kirli.py

Neden metin olarak okunuyor? Şemayla okununca "alan hiç yok" (eksik) ile "alan var ama null" (boş) ikisi de
null olur, ayırt edilemez. Üretici ikisini ayrı üretir; yönerge de ayrı sayılmasını ister. Ham satırda
'"alan":' geçmiyorsa eksik, '"alan": null' geçiyorsa boştur (üretici json.dumps biçimi).
"""
import csv, datetime
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("a6-kirli").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

HAM = "s3a://ham/haber/"
ALANLAR = ["olay_zamani", "haber_id", "kaynak", "kategori", "baslik", "metin"]
OLCUM = "/kit/kanit/olcumler.csv"
VERI = "5.000.000 satır (14 gün)"
BUGUN = datetime.date.today().isoformat()
satirlar = []


def kaydet(ne, sonuc, birim, nasil):
    satirlar.append([BUGUN, "A6", ne, 1, sonuc, birim, VERI, f"is/a6_kirli.py · {nasil}"])
    print(f"  {ne:60s} {sonuc} {birim}")


# cache() YOK: ilk denemede 1,6 GB metni önbelleğe almak 1 GB'lık sürücü belleğini doldurdu, iş çöktü
# (GCLocker → executor kaybı). H6 §3.4: cache yalnızca belleğe SIĞAN ara sonuç için. Her sayım S3'ten yeniden okur.
metin = spark.read.text(HAM)
n = metin.count()
kaydet("ham satır", n, "satır", "spark.read.text")

# 1) Eksik alan / boş değer — tek geçişte bütün alanlar
ifadeler = []
for a in ALANLAR:
    ifadeler += [F.sum((~F.col("value").contains(f'"{a}":')).cast("int")).alias(f"{a}_eksik"),
                 F.sum(F.col("value").contains(f'"{a}": null').cast("int")).alias(f"{a}_bos")]
sayim = metin.agg(*ifadeler).first().asDict()
print("Alan bazında:")
for a in ALANLAR:
    kaydet(f"kirli · {a} eksik / boş", f"{sayim[a + '_eksik']} / {sayim[a + '_bos']}", "satır",
           "ham satırda anahtar yok / anahtar: null")
kirli_kosul = None
for a in ALANLAR:
    k = (~F.col("value").contains(f'"{a}":')) | F.col("value").contains(f'"{a}": null')
    kirli_kosul = k if kirli_kosul is None else (kirli_kosul | k)
kirli = metin.filter(kirli_kosul).count()
kaydet("kirli kayıt toplam (en az bir alanı eksik ya da boş)", f"{kirli} (%{100*kirli/n:.3f})", "satır", "")

# 2) Tekrar: birebir aynı satır (üretici tekrarı böyle yazar)
tekil = metin.distinct().count()
kaydet("birebir aynı satır tekrarı (satır − tekil satır)", n - tekil, "satır", "count − distinct().count()")

# 3) haber_id üzerinden tekrar — boş/eksik kimlikler AYRI tutulur (A3 bulgusu: hepsi tek kayıt gibi birleşirdi)
sema = T.StructType([T.StructField(a, T.StringType()) for a in ALANLAR])
df = spark.read.schema(sema).json(HAM)
kimliksiz = df.filter(F.col("haber_id").isNull()).count()
kaydet("haber_id'si olmayan satır (eksik + boş)", kimliksiz, "satır", "isNull")
tekrar_id = (df.filter(F.col("haber_id").isNotNull()).groupBy("haber_id").count()
               .filter("count > 1").agg(F.count("*").alias("id"), F.sum(F.col("count") - 1).alias("fazla")).first())
kaydet("tekrar eden haber_id / fazladan satır", f"{tekrar_id['id']} / {tekrar_id['fazla']}", "adet / satır",
       "groupBy(haber_id) count>1")

# 4) KVKK: metinlerde kişi adı olabilecek kelime var mı?
kelime = (df.select(F.explode(F.split(F.concat_ws(" ", "baslik", "metin"), r"\s+")).alias("k"))
            .filter(F.length("k") > 0))
farkli = kelime.select(F.lower(F.regexp_replace("k", r"[.,]", "")).alias("k")).distinct().count()
kaydet("KVKK · başlık+metindeki farklı kelime", farkli, "kelime", "split + lower + distinct")
# Cümle başı dışında büyük harfle başlayan kelime (özel ad adayı): cümleleri ". " ile böl, ilk kelimeyi at
cumle = df.select(F.explode(F.split("metin", r"\.\s+")).alias("c"))
ortada = (cumle.select(F.explode(F.slice(F.split(F.trim("c"), r"\s+"), 2, 100)).alias("k"))
               .filter(F.col("k").rlike(r"^[A-ZÇĞİÖŞÜ]")))
kaydet("KVKK · cümle ortasında büyük harfli kelime (özel ad adayı)", ortada.count(), "kelime",
       "metin cümlelere bölündü, ilk kelime hariç")

with open(OLCUM, "a", newline="", encoding="utf-8") as f:
    csv.writer(f).writerows(satirlar)
print(f"{len(satirlar)} satır → kanit/olcumler.csv")
spark.stop()
