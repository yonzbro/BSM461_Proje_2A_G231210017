# BSM 461 Büyük Veriye Giriş — Dönem Projesi Raporu (TASLAK)

> Çalışma taslağı. Başlıklar yönerge Bölüm 6 sırasındadır; her adımda ilgili bölüm doldurulur.
> **Kural:** buradaki her sayı `kanit/olcumler.csv`'de bir satıra karşılık gelir. Karşılığı olmayan sayı yazılmaz.

## 1. Kapak

| | |
|---|---|
| Grup kodu | G231210017 |
| Şube | 2A *(yazım hocayla teyit edilecek)* |
| Üyeler | *(doldurulacak)* |
| Şablon | 4 — Metin / haber akışı analitiği |
| Depo | https://github.com/yonzbro/BSM461_Proje_2A_G231210017 |
| Teslim commit'i | *(`git rev-parse teslim`)* |

**Senaryo kartı** (`kanit/senaryo.txt`, `araclar/senaryo.py --grup G231210017 --sablon haber`):

| Koşul | Değer |
|---|---|
| Sistem | bir haber toplayıcı |
| Ortalama hız | saniyede 2.500 haber |
| Tepe yük | en yoğun saatte ortalamanın 2 katı |
| Uyarı süresi | akış sorusunun cevabı en geç 5 saniyede |
| Geç veri | olayların %5'i en fazla 30 dakika geç gelir |
| Kayıp sınırı | geç kalıp atılan olay en fazla %0,1 |
| Saklama | ham veri 365 gün |
| Servis sorgusu | p99 en fazla 200 ms |

## 2. Özet

**Sorular** (2026-11-23'te grupça kabul edildi):

| | Soru | Tür | Dayanak |
|---|---|---|---|
| Akış 1 | Son 1 saatte başlıklarda en çok geçen 10 konu nedir? (1 saatlik kayan pencere, dakikada bir güncellenir) | sıralama / eğilim | A3'te gündem sinyali: 1.500 haberin 1.171'inde "yapay" geçiyor |
| Akış 2 | Bir kategorinin dakikalık haber hızı normalinin X katını aşınca uyarı | eşik | Kart: uyarı ≤ 5 s. X, toplu veride ölçülen dakikalık hız dağılımından belirlenecek |
| Toplu 1 | Kaynak karşılaştırması: kaynak türüne göre (`kaynaklar.csv` join) hacim ve kategori dağılımı | karşılaştırma | A3: kaynak payları çok dengesiz (en büyük kaynak %49,9) |
| Toplu 2 | Gündem konularının günler ve saatler boyunca seyri: ne zaman başlıyor, ne kadar sürüyor | eğilim | A3: konu yoğunlaşması |

*Ne kurduk ve en önemli üç bulgu en son yazılacak.*

## 3. Mimari — Z0

> **Taslak (A5, 2026-11-23).** "?" işaretli yerler ilgili adımda ölçülüp doldurulacak (yönerge Adım 5); son hâli Adım 15'te.

**Şekil 3.1 — Omurga şeması (H1 §12) bizim sistemimiz için**

```
 ①Olay kaynağı          ②Toplama               ③Günlük/Kuyruk          ④Akış işleme                ⑦Servis deposu      ⑧Sunum
 uretici.py ──────────▶ Kafka üretici ───────▶ Kafka 4.0.0 (KRaft) ──▶ Spark 3.5.3 Structured ──▶ MongoDB 7.0 ──────▶ Portal
 (--sablon haber,        acks=all,              konu: haberler           Streaming                   gündem, kategori    FastAPI + React
  --grup G231210017)     idempotent              bölüm: ? (A11)           pencere: ? watermark: ?     hızı, uyarılar      (/api/gundem,
      │                                         anahtar: kaynak (?)      (A11)                       (A9)                 /api/news)
      │                                                                                                    ▲                  ▲
      │  --toplu                                                                                           │                  │
      └───────────────▶ ⑤Ham depolama ─────────▶ ⑥Toplu işleme ───────────────────────────────────────────┤                  │
                        SeaweedFS 4.48 (S3)      Spark 3.5.3 + Delta 3.2.1                                  │                  │
                        s3://ham/haber/          temizleme → kaynaklar.csv join → toplulaştırma             │                  │
                        tarih=…/ (JSON)          → s3://tablolar (Delta) ────────────────────────────────────┘                  │
                                                 → Elasticsearch 8.15.3 (Türkçe analizör, A12) ─────────────────────────────────┘
```

| Kutu | Bileşen ve sürüm | Neden bu bileşen (şimdilik) | Karar adımı |
|---|---|---|---|
| ① Olay kaynağı | `araclar/uretici.py` (kit) | Yönerge: üreticiyi siz yazmazsınız | — |
| ② Toplama | Kafka üretici istemcisi, `acks=all` + idempotent (kit varsayılanı) | Kayıp sınırı ≤ %0,1 → onaysız yazma (acks=0) riskli; bedeli ölçülecek | A11 |
| ③ Kuyruk | Apache Kafka 4.0.0, KRaft (ZooKeeper yok) | Üretici hızlı/tüketici yavaşken tampon; aynı olayı birden çok tüketici okur (H1 §12) | A11: bölüm sayısı, anahtar |
| ④ Akış işleme | Spark 3.5.3 Structured Streaming | Toplu hatla aynı motor ve DataFrame API'si; olay zamanı + watermark desteği | A11 |
| ⑤ Ham depolama | SeaweedFS 4.48 (S3 uyumlu), JSON | Z1: 365 gün × 70,63 GB/gün = 25,78 TB → ucuz, büyüyen nesne deposu | A6: biçim, dizin düzeni |
| ⑥ Toplu işleme | Spark 3.5.3 + Delta Lake 3.2.1 | Z1: tam tarama tek makinede 57,3 sa → paralel okuma; Delta = atomik teslim (H4 §6.4) | A8, A10 |
| ⑦ Servis deposu | MongoDB 7.0 (akış sonuçları) · Elasticsearch 8.15.3 (arama) | Servis p99 ≤ 200 ms; "ihracatımızdan" araması Türkçe analizör ister | A9, A12 |
| ⑧ Sunum | Portal: FastAPI + React | Canlı gösterim; Gemini özeti kapalı | P1–P2 |

**Kit sadeleştirmesi (raporda açıkça belirtilecek):** gerçek sistemde ham kopya Kafka'dan S3'e bir
bağlayıcıyla (ör. bir sink işi) akardı; kitte ham geçmiş veri `uretici.py --toplu` ile **doğrudan** S3'e
yazılıyor. Yani ⑤'e giden ok bizde ②'den değil, ①'den çıkıyor.

**Tablo 3.1 — Garanti tablosu (H3 §10: her ok için üç soru) — iskelet**

| Ok | ① Kaç kopya (kit → üretim) | ② Sıra garantisi | ③ Ağ bölünürse | Teslim garantisi |
|---|---|---|---|---|
| ① → ② üretici → Kafka | 1 → 1 (istemci) | Bölüm içinde, idempotent üretici yeniden denemede sırayı korur | Üretici yeniden dener, zaman aşımında hata; **?** (A14 `docker compose stop kafka` deneyi) | Ağ kaynaklı tekrar engellenir (idempotent); uygulama tekrarı **engellenmez** (A3: 2 tekrar) |
| ② → ③ Kafka konusu | 1 → 3, `min.insync.replicas=2` | Yalnızca **bölüm içinde**; anahtar=kaynak olduğu için kaynak içinde sıralı, kaynaklar arası sıra yok | `acks=all` + min ISR → çoğunluk yoksa yazma durur (**C** tarafı) | — |
| ③ → ④ Kafka → Spark akış | Checkpoint 1 (S3) → **?** | Bölüm içinde; geç veri olay zamanına göre pencereye girer (watermark) | Tüketici bekler, sonra checkpoint'teki offset'ten devam | Checkpoint → en az bir kez; tam bir kez için idempotent yazma gerekir (**?** A11 yeniden başlatma deneyi) |
| ④ → ⑦ Spark → MongoDB | 1 → 3 (replica set) | Yok | **?** (A9: yazma onayı `majority` mi?) | **?** (A11: upsert ile idempotent mi?) |
| ① → ⑤ üretici → S3 ham | 1 → sağlayıcı/çoğaltma | Yok (günlük dosyalar) | **?** (A14 `docker compose stop s3` deneyi) | — |
| ⑤ → ⑥ S3 → Spark toplu | — | Gerekmez (toplu) | İş başarısız olur, baştan çalıştırılır (ham veri duruyor, H1 §12) | Yeniden çalıştırılabilir; dönüşümler belirlenimci olmalı (H5 §11) |
| ⑥ → Delta tablosu | 1 → 3 | — | Delta commit atomik: ya yeni sürüm ya eski sürüm (**?** A10 Ctrl+C deneyi) | Atomik teslim (Delta günlüğü) |
| ⑥ → Elasticsearch | 1 düğüm, replika 0 → 3 düğüm, replika 1 | — | **?** (A12/A14: ES durunca portal ne gösteriyor?) | **?** (`haber_id` belge kimliği olursa tekrar = üzerine yazma) |
| ⑦ → ⑧ servis → portal | — | — | **?** (A14: ES / Mongo kapalıyken portal) | — |

**Tablo 3.2 — Kitin tek bilgisayar sadeleştirmeleri ve üretimde olması gereken**

| Bileşen | Kitte | Üretimde | Neden |
|---|---|---|---|
| Kafka | 1 düğüm (KRaft, broker + controller aynı süreçte), replikasyon 1 | ≥ 3 aracı, replikasyon 3, `min.insync.replicas=2`; ayrı 3 controller | Tek düğüm ölürse veri gider; mutabakat için tek sayı (H3 §6) |
| S3 (SeaweedFS) | 1 düğüm, çoğaltma yok | Çok düğümlü / bulut nesne deposu | Disk ölürse ham veri gider (H4) |
| Spark | `local[*]`: Driver + tek Executor aynı süreç, 4 çekirdek; shuffle bölümü 8 | Küme yöneticisi (YARN/Kubernetes) + birçok Executor | H6 §1.2; gerçek shuffle ağdan geçer |
| Elasticsearch | Tek düğüm, replika 0 | ≥ 3 düğüm, replika ≥ 1 | Kopya yok; düğüm ölürse indeks gider |
| MongoDB | Tek `mongod` | 3 üyeli replica set; gerekirse shard | Tek hata noktası |
| Güvenlik | Kapalı (şifre, TLS yok) | Kimlik doğrulama + TLS | Asla internete açık çalıştırılmaz |
| Ham veri yolu | `uretici.py --toplu` doğrudan S3'e | Kafka → S3 bağlayıcısı | Tek kaynaktan iki yol (H1 §12) |

## 4. Ölçek ve eşik kararı — Z1

Hesap `analiz/olcek.py` ile yapıldı. Girdiler: senaryo kartı (hız, tepe çarpanı, saklama) ve A3'te ölçülen
olay başına ortalama JSON boyutu **327 bayt** (1.502 mesaj). Birimler ondalık (1 GB = 10⁹ bayt); aktarım hızı
için dersin sabiti kullanıldı: 1 Gb/s = 125 MB/s.

**Tablo 4.1 — Hacim (zarf arkası hesap)**

| | Ortalama olay/s | Tepe olay/s | Ort. MB/s | Tepe MB/s | Olay/gün | GB/gün | 365 günde ham veri |
|---|---|---|---|---|---|---|---|
| Kart (1×) | 2.500 | 5.000 | 0,82 | 1,64 | 216 milyon | 70,63 | **25,78 TB** |
| 10× | 25.000 | 50.000 | 8,18 | 16,35 | 2,16 milyar | 706,32 | 257,81 TB |
| 100× | 250.000 | 500.000 | 81,75 | 163,50 | 21,6 milyar | 7.063,20 | 2,58 PB |

Hesap: 2.500 olay/s × 327 B = 817.500 B/s; × 86.400 s = 70,63 GB/gün; × 365 gün = 25,78 TB.
*Parquet'e geçince* (A6'da ölçülen oran: Parquet gzip ÷ JSON = 106,5 ÷ 1.635,5 MB = **0,0651**, Tablo 5.1;
`olcek.py --parquet-orani 0.0651`): 365 günlük veri **1,68 TB** (10×: 16,78 TB · 100×: 167,83 TB). Tam tarama
125 MB/s ile 57,3 saatten **3,7 saate** iner — üstelik sütunlu biçimde bir sorgu yalnızca kullandığı sütunları okur
(H2 §8). H1 §7'ye göre 1,68 TB hâlâ "500 GB – birkaç TB, tek sunucuda zorlanır" bandındadır ve sürekli büyür;
dağıtık depolama kararı değişmez, ama biçim seçimi okunacak baytı ~15 kat küçültür.

**Ortalamaya mı, tepeye mi göre?** İkisi farklı bileşenler için:
- *Depolama ortalamaya göre:* günlük ve yıllık toplam, gün boyunca ortalama hızın birikimidir; tepe yalnızca
  en yoğun saattedir.
- *Akış bileşenleri (Kafka, akış işi, ES'e yazma) tepeye göre:* kart, uyarının en geç 5 s'de çıkmasını istiyor.
  Hat ortalamaya (2.500/s) göre kurulursa en yoğun saatte saniyede 2.500 olay işlenemeden birikir; bir saatte
  (5.000 − 2.500) × 3.600 = 9.000.000 olaylık kuyruk oluşur ve uyarılar dakikalarca gecikir. 5 s sınırı tam
  olarak en yoğun saatte bozulurdu. Tepe çarpanı yalnızca 2 olduğundan tepeye göre planlamanın bedeli de düşüktür.

**Tek makine mi? (H1: üç darboğaz)**

| Darboğaz | Hesap (1×) | Sonuç |
|---|---|---|
| Aktarım — alım | Tepe 1,64 MB/s ↔ tek 1 Gb/s ağ kartı 125 MB/s | Rahat sığar (%1,3) |
| Aktarım — geçmişi tarama | 25,78 TB ÷ 125 MB/s = **57,3 saat**; tek gün 9,4 dk | Toplu 2 ("konuların günler boyunca seyri") bütün yılı tararsa tek makinede 2,4 gün sürer → makul değil |
| Bellek | Bir günlük ham veri 70,63 GB × 3–5 (H1: diskteki metin belleğe alınınca 3–5 kat şişer) = **212–353 GB** ↔ makinenin RAM'i 15,7 GB (Docker'a 8,17 GB) | Bir gün bile belleğe sığmaz (RAM'in 13,5–22,5 katı). Veri parça parça (Spark bölümleri, akış pencereleri) işlenmeli |
| Depolama | 25,78 TB/yıl ↔ bu makinenin boş diski 360,8 GB | Disk **5,1 günde** dolar |
| Hesaplama | Kelime sayma, gruplama: hafif işlemler | Darboğaz değil |

*Neden 125 MB/s?* Ham veri S3'te, yani bir nesne deposunda durur ve ağ üzerinden okunur; bu yüzden payda
yerel diskin hızı değil, ağın hızıdır (H1 §5: 1 Gb/s ağ ≈ 125 MB/s, sabit diskten bile yavaş).

**Tablo 4.2 — H1 §7 "Tek makine 2026'da nereye kadar gider?" eşik tablosuna göre yerimiz**

| Hangi veri | Boyut | H1 §7 bandı | Tablonun önerdiği |
|---|---|---|---|
| Bir dakikalık / bir saatlik akış penceresi | 2.500/s × 3.600 s × 327 B ≈ 2,94 GB (1 saat) | 1–10 GB | Tek makine; dağıtık gerekmez |
| Bir günlük ham veri | 70,63 GB | 10–100 GB | Dizüstüne sığmaz; tek makinede diskten akıtarak (DuckDB/Polars) |
| 365 günlük saklama | 25,78 TB, **sürekli büyüyor** | > birkaç TB, sürekli büyüyor | **Dağıtık sistem (Spark, dağıtık depolama)** |

H1'in karar ağacı da aynı yere varır: "> birkaç TB → sürekli büyüyor mu? → evet → dağıtık sistem (şimdi gerekli)".
Bellek satırı tablo için çarpanla birlikte okunmuştur (H1 §3: tablodaki "sığar" ifadeleri 3–5× çarpanı içerir).

**Karar.** Alım tek makineye rahat sığar; **ilk tıkanan aktarım ve depolamadır**, işlemci değil.
Tablo 4.2'nin ilk iki satırı COST dersini hatırlatır (H1 vaka notu): tek bir günün ya da pencerenin işi küme
gerektirmez; dağıtık sistemi gerekli kılan **365 günlük, sürekli büyüyen geçmiştir**. Paralel okumanın kazancı
da bu satırdadır (H1 §5, 3. ders): 25,78 TB 10 düğüme dağıtılıp her düğüm kendi payını paralel okursa tam
tarama yaklaşık 57,3 / 10 ≈ 5,7 saate iner (Amdahl'ın saymadığı koordinasyon ve shuffle maliyeti hariç,
yani iyimser alt sınır). 365 günlük
saklama dağıtık, ucuz bir depo (nesne deposu, S3) ve geçmiş üzerindeki toplu işler için paralel okuma (Spark)
gerektirir. H1'in "aktarım" satırındaki doğru çözüm de budur: aktarım hızı sabit, okunan baytı küçült —
sütunlu biçim (Parquet), sıkıştırma, yalnızca gereken sütunu okumak (`baslik`, `olay_zamani`) ve tarih
bölümlemesiyle yalnızca ilgili günleri okumak. Kit tek makinede çalışır; bu nedenle verinin küçültülmüş bir
örneğiyle çalışıyoruz (kartın notu).

**10 ve 100 katı:**
- *10×:* 257,81 TB/yıl, tam tarama 23,9 gün; tek günü okumak bile 94,2 dk. Depolama ve toplu işleme kesin
  dağıtık olmalı. Alım (tepe 16,35 MB/s) hâlâ tek ağ kartına sığar, ama A3'teki çarpıklık burada önem kazanır:
  anahtar `kaynak` kalırsa en büyük kaynak (%49,9) tek Kafka bölümüne yüklenir.
- *100×:* 2,58 PB/yıl; tepe 163,5 MB/s **tek bir 1 Gb/s ağ kartını aşar** → Kafka birden çok aracıya
  (broker) yayılmalı; anahtar `kaynak` kalırsa yükün yarısı (~82 MB/s) tek bölüme, yani tek aracıya düşer, bu
  yüzden anahtar değişikliği zorunlu hâle gelir. Elasticsearch günde 21,6 milyar belgeyi indeksleyemez; çok
  düğümlü küme ve muhtemelen 365 gün yerine daha kısa bir "aranabilir" süre (eski veri yalnızca S3'te) gerekir.

## 5. Depolama — Z2, Z4, Z7

### 5.1 Dosya biçimi (Z2)
Toplu veri: `uretici.py --grup G231210017 --sablon haber --toplu 5000000 --gun 14` (üretim 237,6 s).
Ölçüm betiği `is/a6_bicim.py`: aynı ham veri, temizlemeden, dört biçimde; aynı sorgu (`kategori = 'ekonomi'` →
kaynak başına sayım, yalnızca 2 sütuna dokunur) her biçimde 3 kez. Spark 3.5.3 · kit (`local[*]`, 4 çekirdek,
sürücü belleği 1 GB). JSON da şema elle verilerek okundu (şema çıkarımının fazladan okuması ölçüme katılmadı).

**Tablo 5.1 — Biçim ve kodek karşılaştırması (5.000.000 satır)**

| Biçim | Boyut | JSON ÷ biçim | Dosya | Sorgu ort. (3 tur) | Turlar (s) |
|---|---|---|---|---|---|
| JSON (satır başına bir kayıt) | 1.635,5 MB | 1× | 14 | 4,460 s | 6,97 / 3,34 / 3,07 |
| Parquet snappy | 196,9 MB | 8,3× | 14 | 0,869 s | 1,64 / 0,49 / 0,48 |
| Parquet zstd | 121,1 MB | 13,5× | 14 | 0,547 s | 0,66 / 0,50 / 0,48 |
| Parquet gzip | 106,5 MB | 15,4× | 14 | 0,570 s | 0,55 / 0,59 / 0,57 |

- Dört biçim de birebir aynı sonucu verdi (betik her turda karşılaştırır).
- İlk tur her biçimde daha yavaş (JVM ısınması, dosya önbelleği — H2 §8'in ölçüm notu); ortalamaya dahil.
- Yazma süreleri tek ölçüm (snappy 34,9 s · zstd 18,2 s · gzip 17,0 s); snappy ilk sırada yazıldığı için ısınma
  maliyetini taşıyor → kodek kararına katılmadı.
- **H2 ile çelişen bulgu:** H2'nin satış verisinde zstd gzip'ten küçüktü; bizim Türkçe haber metinlerimizde
  gzip %12 daha küçük. H2 §7.5: "sıkışma oranı verinin özelliğidir, biçimin değil" — kendi verimizde ölçmenin nedeni.
- **Kodek kararı: Parquet + gzip** (grupça, 2026-11-23). Gerekçe: ölçülen tek anlamlı fark boyut — gzip zstd'den
  %12, snappy'den %46 küçük; sorgu farkı (zstd 0,547 ↔ gzip 0,570 s) tur dağılımının (0,48–0,66 s) içinde kalıyor.
  Z1'e göre ilk tıkanan aktarım ve depolama olduğu için boyut belirleyici. Ders notunun "depolama kritikse zstd"
  önerisinden (H2 §11) bilerek ayrıldık: o öneri başka bir verinin ölçümüne dayanıyor.
- Sütun budaması ve filtre itmesinin planda görünmesi (`ReadSchema`, `PushedFilters`) A8'deki `explain-1.txt` ile gösterilecek.

### 5.2 Nesne deposu ve dosya düzeni (Z4)
**Dizin düzeni: `tarih=YYYY-MM-DD/`** (grupça, 2026-11-23), hem ham (`s3://ham/haber/tarih=…/`) hem işlenmiş katmanda.

| Aday bölüm sütunu | Değer sayısı | Lehine | Aleyhine |
|---|---|---|---|
| **tarih** ✓ | 14 (günde 1) | Toplu 2 ve akış hep tarih aralığıyla sorgular → yalnızca ilgili günler okunur; `olay_zamani` hiç bozulmuyor (Tablo 10.1) | Bir günü tüm kaynaklarıyla okur |
| kaynak | 6 | Kaynak karşılaştırması (Toplu 1) | Çok çarpık: en büyük kaynak %49,9 (A3) → bir bölüm verinin yarısı; ayrıca 1.993 kayıtta `kaynak` eksik/boş |
| kategori | 8 | — | Sorgularımızın birincil filtresi değil; tarih × kategori dosya sayısını 8 katına çıkarır (küçük dosya, H4 §5) |

**Dosya sayısı ve boyutu:** ham katman 14 dosya × 116,8 MB; Parquet gzip 14 dosya × ~7,6 MB (106,5 ÷ 14).
Parquet dosyaları H4 §11'in "yüzlerce MB" ölçütünün altında, ama bu kitteki küçültülmüş örnektir: kartın
gerçek ölçeğinde günde 70,63 GB ham × 0,0651 ≈ 4,6 GB Parquet düşer — o ölçekte sorun küçük dosya değil, bir
günün birden çok dosyaya bölünmesi olur. 14 dosya için birleştirme (compaction) gerekmedi.

**Nesne deposunun bedeli (H4 §6.4):** Spark'ın `partitionBy` ile yazdığı çıktı `_temporary/` → asıl dizine
yeniden adlandırılarak teslim edilir; S3'te bu kopyala + sil'dir, atomik değildir. Yarıda kesilen bir yazma yarım
bir klasör bırakır ve okuyan iş hata vermeden eksik veri görür → Z7'de Delta ile karşılaştırılacak (A10).

### 5.3 Tablo biçimi (Z7)
*(A10)*

## 6. Bölümleme, çoğaltma, tutarlılık — Z3

Ölçüm: `is/a7_bolum.py`, A6'daki 5.000.000 satırlık Parquet (temizlenmemiş). Bölüm dağılımı Spark'ın kendi
shuffle kuralıyla hesaplandı — `pmod(hash(anahtar), 8)`, 8 = kitteki `spark.sql.shuffle.partitions` (H6 §4.4).
Sıcak nokta ölçütü H3 §11: en dolu bölüm ÷ ortalama > 1,5.

**Tablo 6.1 — Bölüm anahtarı adayları (8 bölüm)**

| Aday | Farklı değer | En büyük anahtarın payı | En dolu ÷ ortalama | Boş kova | Durum |
|---|---|---|---|---|---|
| kaynak | 7 (6 + boş) | %50,9 | **4,43** | 4 / 8 | sıcak nokta |
| kategori | 9 (8 + boş) | %22,0 | 1,98 | 2 / 8 | sıcak nokta |
| tarih | 14 | %7,1 | 2,86 | 2 / 8 | sıcak nokta |
| haber_id | 4.988.199 | ~%0 | **1,00** | 0 / 8 | dengeli |
| kaynak + tarih | 98 | %3,6 | 1,77 | 0 / 8 | sıcak nokta (sınırda) |
| kaynak # tuz (0–7) | 56 | — | 1,53 | 0 / 8 | sınırda |

Notlar: kategori de çarpık (en büyük %22,0, eşit dağılımda %12,5 olurdu). Muhtemel neden: üretici grup koduna
göre seçtiği 3 kaynağa birer tercihli kategori verir ve o kaynakların haberlerinin %60'ını oradan seçer
(`uretici.py`); büyük bir kaynak bunlardan biriyse ağırlığı kategoriye taşınır — kaynak × kategori dağılımı
A8'deki Toplu 1 sorgusunda ölçülecek. tarih'te günler eşit (357.142), ama bir kovaya 5
gün düştü (5 ÷ 14 × 8 = 2,86). haber_id dengeli, ama sorgularımız haber_id ile gruplamaz. Tuz haber_id'den
türetildi (rastgele değil, belirlenimci — H5 §11).

**İki ders:** (1) "Hash anahtarları dağıtır, satırları değil" (H3 §3.2): kaynak'ta tek anahtar verinin yarısı,
hiçbir hash onu bölemez (yarısı tek kovada, 8 kovanın 4'ü boş). (2) Az farklı değerli anahtar, değerleri eşit olsa bile dengelenmez: 14 günü 8 kovaya
hash'le dağıtmak bir kovaya 5 gün verir. Denge için anahtarın kova sayısından **çok** fazla değeri olmalı.

**Tablo 6.2 — Sıcak noktanın bedeli ve çözümü: kaynak başına ardışık haber arası süre (3 tur)**

Bu iş combiner'la kurtarılamaz (H5 §6.1): pencere fonksiyonu bir kaynağın bütün satırlarını tek göreve toplayıp
zamana göre sıralar. Görev süreleri Spark UI REST API'sinden (`taskSummary`, `executorRunTime`).

| Bölüm anahtarı | Süre ort. | En uzun ÷ ortanca görev | Sonuç farkı |
|---|---|---|---|
| kaynak (önce) | 8,68 s | **12,80** | — |
| kaynak + tarih (sonra) | **3,74 s** (2,3× hızlı) | **1,49** | 91 gün-aşırı aralık kayboldu (7 kaynak × 13 gün geçişi) |

**Karar: kaynak başına sıralı işlerde bileşik anahtar (kaynak, tarih).** Tuzlama değil, çünkü tuz bir kaynağın
satırlarını rastgele kovalara dağıtır ve "ardışık" kavramını bozar; sıralı işlerde tuzlama ancak ikinci bir
birleştirme aşamasıyla çalışır (H5 §6.3). Bileşik anahtar anlamı korur, bedeli ölçülü ve küçüktür (91 aralık,
5 milyon içinde). Kaynak başına **sayım/toplam** işlerinde ise çözüm gerekmez: combiner sıcak anahtarı map
tarafında tek sayıya indirir (H5 §6.3). Kafka konusunun anahtarı ayrıca A11'de kararlaştırılacak (aynı çarpıklık,
A3: bölüm dengesi 1,64).

*Yan bulgu:* kaynak başına ortalama ardışık haber arası ajans-kuzey 0,48 s, ekonomi-gunluk 5,46 s; `kaynak`'ı
boş kirli kayıtlar ortalama 603 s aralıkla geliyor — kirli kayıtların zamanda seyrek ve düzgün dağıldığını gösterir.

**Tekrar sonucu değiştiriyor mu? Evet.** Ham 5.000.000 satır, birebir tekilleştirilmiş 4.990.083. Kategori
sayımında en büyük fazla sayma **%0,207** (teknoloji: +1.048). Bu, kartın kabul ettiği **%0,1**'lik hata payının iki
katı → toplu işte tekrarlar mutlaka ayıklanmalı (A8). Akışta aynı sorun teslim garantisi olarak çıkar (A11).

**Çoğaltma (kit → üretim):** Tablo 3.2'de. Kitte her bileşen tek kopya; üretimde Kafka 3 kopya +
`min.insync.replicas=2`, MongoDB 3 üyeli replica set, Elasticsearch replika ≥ 1, nesne deposu sağlayıcının
çoğaltması. Neden 3: tek düğüm arızasında hem yazma hem okuma sürer ve çoğunluk tek sayıyla kurulur (H3 §4.3, §6).

**Tutarlılık: bölünmede C mi, A mı? Tek cevap yok — yol başına ayrı karar** (H1 §9: "istek başına ayarlanabilen
bir karar"):

| Yol | Bölünmede | PACELC | Gerekçe |
|---|---|---|---|
| Alım: üretici → Kafka | **C** — yazma durur (`acks=all`, `min.insync.replicas=2`) | PC/EC | Kart: kayıp ≤ %0,1. Sessiz kayıp yerine durmak; normal zamanda da tüm kopyaları bekler, gecikmeyi öder |
| Sunum: gündem paneli (MongoDB), arama (ES) | **A** — eski ama cevap | MongoDB varsayılan PA/EC (H3 §5.3) | Gündem listesinin birkaç saniye eski olması zararsız; panelin açılmaması zararlı (H1 §9 "beğeni sayısı" örneği) |

Seçim bedelleri A11 (Kafka durunca ne olur, A14 deneyi) ve A9'da (MongoDB yazma onayı) ölçülecek.

**A3'ten gelen ön bulgu — Kafka anahtarı.** Üretici mesaj anahtarı olarak `kaynak` kullanıyor. Yalnızca 6 kaynak
var ve `ajans-kuzey` mesajların %49,9'unu üretiyor. Varsayılan 3 bölümlü konuda bölüm dağılımı 293 / 819 / 390,
en dolu bölüm ÷ ortalama = **1,64** (1.502 mesaj). H3 §11'in ölçütüne göre (> 1,5) bu bir **sıcak noktadır**. Bu anahtarla bölüm sayısını artırmak dengeyi düzeltmez:
6 farklı anahtar en fazla 6 bölüm doldurur, en büyük kaynak tek bölümde kalır. A7/A11'de anahtar seçimi
bu ölçümle tartışılacak.

## 7. Toplu hat — Z5
*(A8)*

## 8. Akış hattı — Z8
*(A11)*

**A3'ten gelen ön bulgu — geç veri.** 1.502 mesajın 74'ünde (%4,93) gönderim anı ile `olay_zamani` arasında
5 saniyeden fazla fark var; gecikme 1,1–30,0 dk, ortanca 15,7 dk. Bu kartla uyumludur (%5, en fazla 30 dk).
Ortancanın aralığın ortasında olması gecikmenin düzgün dağıldığını düşündürür; öyleyse kayıp ≤ %0,1 sınırı
için watermark 30 dakikaya yakın olmalıdır. Örnek küçük (74 olay) — karar A11'deki watermark deneyiyle verilecek.

## 9. Servis deposu ve arama — Z6, Z9
*(A9, A12)*

**A3'ten gelen ön bulgu — Türkçe büyük/küçük harf.** Kelime sayımında Python'un `str.lower()` işlevi
"İhracatlar" kelimesini `i` + birleşik nokta (U+0307) + "hracatlar" yaptı; kelime noktada bölündü ve `i`,
`hracatlar` gibi anlamsız parçalar çıktı. Türkçeye özgü küçük harf dönüşümü yapılmazsa "İ" ile başlayan
kelimeler doğru aranamaz. A12'de Türkçe ve standard analizör bu açıdan da karşılaştırılacak.

## 10. Veri kalitesi ve bulgular — Z10

### 10.1 KVKK notu
Veri sözlüğündeki hiçbir alan doğrudan bir kişiyi tanımlamaz: `kaynak` bir kuruluş adıdır, `haber_id` haberin
kimliğidir. Kişi adı geçebilecek tek yer serbest metin alanlarıdır (`baslik`, `metin`). 1.502 haberlik akış
örneğinde metinler kalıplardan üretilmiş 118 farklı kelimeden oluşuyor ve cümle ortasında büyük harfle başlayan
(özel ad olabilecek) hiçbir kelime yok; kişi adı bulunmadı. Denetim 5.000.000 satırlık toplu veride tekrarlandı
(`is/a6_kirli.py`): **117 farklı kelime, cümle ortasında büyük harfli kelime 0.** (118 ↔ 117 farkı: iki sayım kelimeyi
farklı böler — akışta Python `\w+`, toplu veride Spark boşluğa göre bölme; sonuç aynı: kalıp kelimeler, özel ad yok.)
**Karar:** maskeleme gerekmiyor. *Gerçek bir haber akışında* metinde kişi adları olacağı için orada
kişi adlarını ayıklamak ya da saklama süresini sınırlamak gerekirdi; bu not raporda tartışılacak.

### 10.2 Kirli veri

**Tablo 10.1 — Toplu veride kusurlar (5.000.000 satır, `is/a6_kirli.py`)**

Eksik (alan satırda hiç yok) ile boş (`null`) ayrı sayıldı: şemayla okununca ikisi de `null` görünür, bu yüzden
ham satırlar metin olarak tarandı.

| Alan | Eksik | Boş |
|---|---|---|
| olay_zamani | 0 | 0 |
| haber_id | 942 | 946 |
| kaynak | 984 | 1.009 |
| kategori | 1.002 | 979 |
| baslik | 994 | 1.044 |
| metin | 1.025 | 1.026 |
| **En az bir alanı kusurlu satır** | **9.951 (%0,199)** | |

| Tekrar | Sayı | Yorum |
|---|---|---|
| Birebir aynı satır | 9.917 | Üreticinin bildirdiği tekrar sayısıyla aynı (sağlama ✓) |
| Tekrar eden `haber_id` | 9.914 | 3 eksik: kimliği boş kayıtların kopyaları kimlikle yakalanamaz |
| `haber_id`'si olmayan satır | 1.888 | Kimliğe göre ayıklama bunları tek kayda indirip 1.887 haberi sessizce silerdi |

**A8 için sonuç:** tekrar ayıklama birebir satıra göre yapılmalı; kimliksiz kayıtlar için ayrı karar (A8).
`olay_zamani` hiç bozulmuyor (üretici bu alanı kirletmiyor) → olay zamanına göre bölümleme güvenli.

*Akış örneğindeki ilk sayılar (A3):*

| Kusur | Akış örneği (1.502 mesaj) | Örnek | Ne yapacağız |
|---|---|---|---|
| Boş değer | 3 kayıt (%0,20): `metin` null = 2, `haber_id` boş = 1 | `H00000127`: `"metin": null` | *(A8'de karar)* |
| Tekrar eden olay | 2 fazladan mesaj | `H00000330`, `H00001118` ikişer kez, içerik birebir aynı | *(A8/A11'de karar)* |

**Dikkat:** tekrarlar `haber_id`'ye göre ayıklanırsa, `haber_id`'si boş kayıtların hepsi tek kayıt gibi birleşir.
Önce boş kimlikli kayıtlar ayrılmalı, sonra tekrar ayıklanmalı.

### 10.3 Grafikler
*(A8)*

## 11. Bu sistem nerede kırılır? — Z11

Dönem boyunca yaşanan ya da öngörülen kırılmaların ham listesi (sonra tabloya dökülecek):

| Kırılma | Tetikleyen | Belirti | Ne yaptık / ne yapılmalı |
|---|---|---|---|
| Ortam farkı sağlık denetimini kırdı | Windows'ta kapsayıcı içinde `localhost` → `::1` (IPv6); SeaweedFS yalnızca IPv4 dinliyor | `bsm461-s3` "unhealthy"; ona bağlı spark, araclar, s3-kur hiç başlamadı | Denetimde `127.0.0.1` kullanıldı. Ders: sağlık denetimi de bir bağımlılıktır; denetim yanlışsa sağlam servis "ölü" sayılır |
| Port çakışması | Aynı makinede başka bir Kafka 9092'de | `docker compose up` portu alamaz | Dış port 9094 yapıldı, kapsayıcı içi aynı kaldı |
| Tek bölüme yığılma | Anahtar = `kaynak`, en büyük kaynak %49,9 | En dolu bölüm ortalamanın 1,64 katı | A11'de anahtar/bölüm tasarımı |
| Önbellek belleği doldurdu | `is/a6_kirli.py` ilk sürümü 1,6 GB ham metni `cache()` ile belleğe aldı; sürücü belleği 1 GB | GC uyarıları ("GCLocker… too often"), ardından executor kaybı, `Block rdd_3_1 does not exist` — kanıt `kanit/kirilma-a6-cache-oom.log` | `cache()` kaldırıldı, iş tamamlandı. H6 §3.4: yalnızca belleğe sığan ara sonuç önbelleğe alınır. Üretimde: executor belleği veriye göre boyutlandırılır, `persist(MEMORY_AND_DISK)` |
| Bellek baskısı (yazma) | `partitionBy(tarih)` ile Parquet yazarken her görev aynı anda 8–12 dosya açık tutuyor | `MemoryManager: Total allocation exceeds 95%… Scaling row group sizes to 63%` | Row group'lar küçülüyor (Parquet verimi düşer); A6'da bilinçli kabul. Üretimde daha çok bellek ya da yazmadan önce `repartition("tarih")` (her görev tek tarih yazar) |
| Kirli anahtar | `kaynak` alanı boş/eksik kayıt | Gruplamada `None` grubu (ekonomi'de 274 haber); Python tarafında sıralama çöktü | Kirli kayıtlar temizlenmeden toplulaştırılmamalı (A8) |
| Satır sonu (CRLF) | Windows'ta klonlanan `bin/spark-submit` CRLF'e dönerse | Bütün `spark-submit` çağrıları başarısız | `.gitattributes` ile LF zorunlu (kitte var) |

## 12. Yapay zekâ kullanım beyanı — Z12
*(Sürekli güncellenecek.)* Claude Code (Anthropic): kit kurulumu ve Windows sorunlarının teşhisi, `analiz/veri_tani.py`
betiğinin yazımı, rapor taslağının ilk metni. Kararlar (sorular, kabul edilen bulgular) grupça verildi.

## 13. Katkı beyanı
*(doldurulacak)*

---

## Ek A — Ortam kanıtı
`kanit/ortam.png`

## Ek B — Veri sözlüğü

Kaynak: Kafka `haberler` konusu, `uretici.py --grup G231210017 --sablon haber`. Boyutlar 1.502 mesajlık örnekten.

| Alan | Anlamı | Tip (ham → işlenmiş) | Örnek | Not |
|---|---|---|---|---|
| `olay_zamani` | Haberin yayımlandığı an (olay zamanı) | string ISO-8601, UTC, ms → timestamp | `2026-11-23T10:34:29.360Z` | Pencereler buna göre kurulur; %4,93'ü gönderimden dakikalarca önce |
| `haber_id` | Haberin kimliği | string, `H` + 8 hane | `H00000001` | Tekrar ayıklama anahtarı; nadiren boş geliyor |
| `kaynak` | Yayımlayan kuruluş | string, 6 değer | `ajans-kuzey` | Kafka mesaj anahtarı; `kaynaklar.csv` join anahtarı; çok çarpık |
| `kategori` | Haberin konusu | string, 8 değer | `ekonomi` | gündem, spor, ekonomi, teknoloji, eğitim, sağlık, dünya, kültür |
| `baslik` | Başlık | string, Türkçe | `Borsadaki gelişmeler sona erdi` | Akış 1'in kaynağı; Türkçe ekler ("Borsadaki") analizör gerektirir |
| `metin` | Haber metni | string, Türkçe, 3 cümle | `Borsadaki gelişmeler rekor kırdı. …` | Arama (ES) alanı; nadiren null |

Olay başına JSON boyutu: ortalama 327 bayt (en az 166, en çok 367).

**Boyut tablosu** `s3://ham/boyut/haber/kaynaklar.csv` (6 satır, join anahtarı `kaynak`): `tur`, `sehir`,
`kurulus_yili` — içeriği toplu üretimde (A6) incelenecek.
