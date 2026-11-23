# -*- coding: utf-8 -*-
"""A3 · Veriyi tanı — Kafka'dan alınmış bir haber dökümünü inceler.

Döküm (kapsayıcıda, kit klasöründe):
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
      --topic haberler --from-beginning --timeout-ms 10000 --property print.key=true \
      --property print.timestamp=true --property print.partition=true --property key.separator='|' > dokum.txt
Çalıştırma:
  python analiz/veri_tani.py dokum.txt

Satır biçimi: CreateTime:<ms>|Partition:<n>|<anahtar>|<json>
CreateTime = üreticinin mesajı gönderdiği an. olay_zamani ile farkı = olayın ne kadar geç geldiği.
Yalnızca standart kütüphane: herkes kendi makinesinde aynı sonucu alsın.
"""
import json, sys
from collections import Counter
from datetime import datetime

ALANLAR = ["olay_zamani", "haber_id", "kaynak", "kategori", "baslik", "metin"]
GEC_ESIK_S = 5  # gönderim ile olay_zamani arasında 5 s'den fazla fark "geç" sayılır (kartın uyarı süresi)

satirlar, bozuk_json = [], 0
for ham in open(sys.argv[1], encoding="utf-8"):
    if not ham.startswith("CreateTime:"):
        continue
    ts, bolum, anahtar, govde = ham.rstrip("\n").split("|", 3)
    try:
        olay = json.loads(govde)
    except json.JSONDecodeError:
        bozuk_json += 1
        continue
    satirlar.append((int(ts.split(":")[1]), int(bolum.split(":")[1]), anahtar, olay, len(govde.encode("utf-8"))))

n = len(satirlar)
print(f"Toplam mesaj: {n}  (bozuk JSON: {bozuk_json})")

# 1) Kirli kayıt: eksik alan / boş değer
print("\n── Kirli kayıt ──")
for a in ALANLAR:
    eksik = sum(1 for *_, o, _ in satirlar if a not in o)
    bos = sum(1 for *_, o, _ in satirlar if a in o and o[a] in (None, ""))
    if eksik or bos:
        print(f"  {a:12s} eksik={eksik}  boş={bos}")
kirli = [o for *_, o, _ in satirlar if any(a not in o or o[a] in (None, "") for a in ALANLAR)]
print(f"  kirli kayıt: {len(kirli)} ({len(kirli)/n:.2%})  örnek: {json.dumps(kirli[0], ensure_ascii=False)[:200] if kirli else '-'}")
fazla = Counter(k for *_, o, _ in satirlar for k in o if k not in ALANLAR)
if fazla:
    print(f"  beklenmeyen alanlar: {dict(fazla)}")

# 2) Tekrar: aynı haber_id birden çok kez
print("\n── Tekrar ──")
idler = Counter(o.get("haber_id") for *_, o, _ in satirlar)
tekrar = {k: v for k, v in idler.items() if v > 1}
print(f"  tekrar eden haber_id: {len(tekrar)}  fazladan mesaj: {sum(v - 1 for v in tekrar.values())}")
for hid in list(tekrar)[:2]:
    kopyalar = [o for *_, o, _ in satirlar if o.get("haber_id") == hid]
    print(f"  {hid}: {len(kopyalar)} kopya, içerik aynı mı? {all(k == kopyalar[0] for k in kopyalar)}")

# 3) Çarpık anahtar: kaynak dağılımı ve Kafka bölüm dengesi
print("\n── Çarpık anahtar ──")
kaynaklar = Counter(a for _, _, a, _, _ in satirlar)
for k, v in kaynaklar.most_common():
    print(f"  {k:16s} {v:6d}  {v/n:6.1%}")
bolumler = Counter(b for _, b, *_ in satirlar)
ort = n / len(bolumler)
print(f"  Kafka bölümleri: {dict(sorted(bolumler.items()))}  en dolu ÷ ortalama = {max(bolumler.values())/ort:.2f}")

# 4) Geç olay: gönderim anı − olay_zamani
print("\n── Geç olay ──")
gecikme = []
for ts, _, _, o, _ in satirlar:
    try:
        oz = datetime.fromisoformat(o["olay_zamani"].replace("Z", "+00:00")).timestamp() * 1000
    except (KeyError, TypeError, ValueError):
        continue  # olay_zamani eksik/bozuk → kirli kayıt bölümünde sayıldı
    gecikme.append((ts - oz) / 1000)
gec = sorted(g for g in gecikme if g > GEC_ESIK_S)
print(f"  {GEC_ESIK_S} s'den geç: {len(gec)} / {len(gecikme)} ({len(gec)/len(gecikme):.2%})")
if gec:
    print(f"  gecikme (dk): en az {gec[0]/60:.1f}  ortanca {gec[len(gec)//2]/60:.1f}  en çok {gec[-1]/60:.1f}")

# 5) Boyut ve kategori
print("\n── Boyut / kategori ──")
boyut = [b for *_, b in satirlar]
print(f"  JSON bayt/olay: ortalama {sum(boyut)/n:.0f}  en az {min(boyut)}  en çok {max(boyut)}")
print(f"  kategoriler: {dict(Counter(o.get('kategori') for *_, o, _ in satirlar).most_common())}")
