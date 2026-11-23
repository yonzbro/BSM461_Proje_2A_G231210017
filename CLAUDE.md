# BSM 461 Büyük Veriye Giriş — Dönem Projesi (Şablon 4: Haber)

Bu dosya Claude Code için proje bağlamıdır. Her oturumun başında oku, "Durum" bölümünü güncel tut.

## Proje özeti
- Ders: BSM 461 Büyük Veriye Giriş, Sakarya Üniversitesi, 2026-2027 Güz. Öğretim üyesi: Dr. Öğr. Üyesi Sümeyye Kaynak.
- Ödev yönergesi: `docs/Proje-Odevi.pdf`. Kararlarda her zaman yönergeye bak.
- Görev: başlangıç kiti (Docker: Kafka 4.0 KRaft, S3/SeaweedFS, Spark 3.5 + Delta, Elasticsearch, Kibana, MongoDB) üzerinde uçtan uca büyük veri hattı kurmak. Her kararı **ölçümle** gerekçelendirmek.
- Şablon: **4 — Metin / haber akışı analitiği**. Konu: `haberler`. Alanlar: olay_zamani, haber_id, kaynak, kategori, baslik, metin. Boyut tablosu: kaynaklar.csv (tür, şehir, kuruluş yılı).
- Öne çıkan katman: ters indeks ve Türkçe analiz (Elasticsearch zorunlu). "ihracatımızdan" araması "ihracat" geçen haberi bulmalı.
- Grup kodu: `G231210017` (üç öğrenci numarasının en küçüğü). Şube: `2A` (kullanıcı "2 a" dedi; ad içinde boşluk olamayacağı için 2A yazıldı — hocanın beklediği yazımı teyit et).
- Depo: https://github.com/yonzbro/BSM461_Proje_2A_G231210017 · rapor: `BSM461_Proje_2A_G231210017.pdf` · sunum: `…_sunum.pdf`.
- Senaryo kartı (`kanit/senaryo.txt`): 2.500 haber/s · tepe ×2 · uyarı 5 s · geç veri %5, en fazla 30 dk · kayıp ≤ %0,1 · saklama 365 gün · servis p99 200 ms.
- Depo adı: `BSM461_Proje_<şube>_<grupkodu>` (açık/public).
- **Teslim: 11 Aralık 2026 Cuma 23.59.** Geç teslim kabul edilmez. `git tag teslim` + push, commit kimliği rapor kapağına yazılır.

## Mimari
```
uretici.py ──▶ Kafka (haberler) ──▶ Spark Structured Streaming ──▶ MongoDB (gündem, kategori hızı)
   │                                          │
   └──▶ S3 ham JSON ──▶ Spark toplu ──▶ Delta tablolar
                                    └──▶ Elasticsearch (Türkçe analizör)
                                                   ▼
                            Portal (FastAPI + React) — portal/
```

## Sorular (2026-11-23 kabul edildi — ayrıntı `docs/rapor-taslak.md` §2)
- Akış 1: Son 1 saatte başlıklarda en çok geçen 10 konu (1 sa kayan pencere, dakikada bir güncelleme).
- Akış 2: Kategori başına dakikalık haber hızı normalin X katını aşınca uyarı (≤ 5 s). X toplu veriden ölçülecek, uydurulmayacak.
- Toplu 1: Kaynak türüne göre (kaynaklar.csv join) hacim ve kategori dağılımı.
- Toplu 2: Gündem konularının günler/saatler boyunca seyri.

Rapor taslağı: `docs/rapor-taslak.md` (Bölüm 6 başlıklarıyla; her adımda ilgili bölüm doldurulur).

## Portal
- Mehmet Salih'in önceki projesi "AI-Destekli-Haber-Portali" uyarlanıyor (FastAPI + React/Vite/Tailwind). Rapordaki YZ ve katkı beyanında bu kökeni belirt.
- CollectAPI **kaldırılacak**. Veri yalnızca kitin üreticisinden gelir.
- Endpoint'ler: `/api/news?q=&kategori=` (Elasticsearch araması), `/api/gundem` (MongoDB akış sonuçları), `/api/analiz` (toplu sonuçlar, isteğe bağlı).
- Gemini özeti varsayılan olarak **kapalı**; anahtar yoksa sistem sorunsuz çalışmalı.
- Portal `docker-compose.yml`'ye `--profile portal` ile eklenir. Sabit `localhost:8000` yerine ortam değişkeni kullanılır.
- Portal yalnızca Z9 (3 puan) ve sunumdaki canlı gösterime katkı verir. Puanın çoğu hat ve ölçümlerde. Portala fazla zaman harcama.

## Puanlama (öncelik sırası)
Z11 kırılma 20 · Z5 toplu 10 · Z8 akış 10 · Sunum 10 · Z0 mimari 8 · Z3 bölümleme 7 · Z10 veri kalitesi 7 · Z2 biçim 5 · Z7 Delta 5 · Z12+düzen 5 · Z6 servis 4 · Z1 ölçek 3 · Z4 S3 3 · Z9 arama 3

## Kurallar (yönergeden — ihlal etme)
- **Ölçüm kuralı:** "daha hızlı/iyi" diyen her iddianın yanında ölçülmüş bir sayı olmalı. Bütün ölçümler `kanit/olcumler.csv`'ye yazılır (ne, kaç kez, sonuç, birim, veri boyutu). **Ölçülmemiş sayıyı asla yazma veya uydurma** (o bileşen sıfır olur). Bir sayı gerekiyorsa komutu çalıştır ya da kullanıcıdan çalıştırmasını iste.
- Performans karşılaştırmaları en az 3 kez çalıştırılır, ortalaması alınır.
- Ham veri depoya konmaz; üretici komutu `--grup <kod>` ile yazılır.
- README'deki adımlar sıfırdan çalıştırıldığında hat ayağa kalkmalı.
- Var olmayan kaynak veya uydurma alıntı kullanma.
- Kullanıcının grup arkadaşları da her kararı savunabilmeli. Kod yazarken **neden** öyle yapıldığını kısaca açıkla.

## Depo düzeni
```
README.md  docker-compose.yml
is/        Spark işleri (toplu_is.py, akis_iskelet.py …)
analiz/    Python grafik/analiz betikleri
portal/    backend/ (FastAPI) + frontend/ (React)
docs/      yönerge PDF, rapor taslakları
kanit/     ortam.png, senaryo.txt, spark-ui-*.png, explain-*.txt, olcumler.csv, kirilma-deneyi.*
```
Komutlar kit klasöründe çalışır: `docker compose exec araclar python araclar/...`, `docker compose exec spark spark-submit is/...`.

## Adım planı ve durum
Bitenleri [x] yap, tarih ve kısa not ekle.

### Aşama 1 — Başlangıç (22–24 Kas)
- [ ] A1 Grup, grup kodu, açık GitHub deposu, kit kopyalandı — 2026-11-23: grup kodu belli; kit `github.com/sumeyyekaynak/bsm461-proje-kit`'ten (11d3993) depo köküne kopyalandı; yerel `git init` yapıldı, commit yok. Eksik: şube, GitHub deposu.
- [x] A2 `docker compose up -d` + `./dogrula.sh --tam` → `kanit/ortam.png` — 2026-11-23: hepsi ✅, `kanit/ortam.png` kaydedildi (PowerShell çıktısı).
- [x] A3 Senaryo kartı → `kanit/senaryo.txt`; veri sözlüğü; 4 kusura birer örnek; 4 soru; KVKK notu — 2026-11-23: `analiz/veri_tani.py` ile 1.502 akış mesajı incelendi, sayılar `kanit/olcumler.csv`'de, metin `docs/rapor-taslak.md`'de. KVKK: kişi adı YOK (118 kalıp kelime) — toplu veride tekrar kontrol edilecek. Önemli bulgular: anahtar=kaynak çarpık (%49,9, bölüm dengesi 1,64); boş haber_id tekrar ayıklamayı bozar; Python `lower()` "İ" sorunu.
- [x] A4 Z1 ölçek hesabı — 2026-11-23: `analiz/olcek.py`; 70,63 GB/gün, 25,78 TB/yıl; ilk tıkanan aktarım + depolama (tam tarama 57,3 sa, disk 5,1 günde dolar); 100×'te tepe 163,5 MB/s > 1 Gb/s. Taslak §4. Eksik: Parquet oranı (A6'dan sonra `--parquet-orani`). Not: H1 çalışma notlarında "1 GB → 3–5 GB" bellek çarpanı PDF metninde "35" olarak bozuk çıktı; slayttan teyit et.
- [ ] A5 Z0 mimari taslağı + garanti tablosu iskeleti

### Aşama 2 — Depolama ve toplu (25–29 Kas)
- [ ] A6 `--toplu` üretim; JSON/Parquet boyut ve sorgu süresi; snappy/zstd/gzip; S3 dizin düzeni ve dosya sayısı; kirli veri sayımı
- [ ] A7 Bölüm anahtarı adayları (kaynak, kategori, tarih); en dolu ÷ ortalama; tuzlama önce/sonra; tekrar eden olay sayısı; CAP/PACELC
- [ ] A8 Toplu iş: temizleme + kaynaklar.csv join + toplulaştırma; `explain` → `kanit/explain-1.txt` + yorum; Spark UI ekran görüntüsü; performans kararı (3 tekrar); kirli veri kararı; 2 grafik
- [ ] A10 Delta: ikinci yazma, DESCRIBE HISTORY, versionAsOf, Ctrl+C atomiklik deneyi (Delta ile çıplak Parquet karşılaştırması)

### Aşama 3 — Akış, servis, arama (30 Kas–4 Ara)
- [ ] A11 Kafka bölüm sayısı hesabı ve konu yeniden oluşturma; akış işi (pencere, watermark); watermark deneyi (kart oranı ve `--gec-orani 0.3`); yeniden başlatma deneyi; teslim garantisi
- [ ] A9 MongoDB: erişim deseni tablosu, veri modeli, shard anahtarı tartışması
- [ ] A12 Elasticsearch: mapping, Türkçe ve standard analizör karşılaştırması (aynı sorgu, sonuç sayısı), arama + toplama sorgusu

### Aşama 4 — Portal ve kırılma (5–7 Ara)
- [ ] P1 Portal backend'i ES/Mongo'ya bağla, CollectAPI'yi kaldır, Gemini isteğe bağlı
- [ ] P2 Frontend: arama kutusu, gündem paneli; docker-compose `portal` profili
- [ ] A14 Z11: en az 3 kırılma noktası; deneyler (Kafka stop, S3 stop, tepe yük, geç veri, ES stop → portal) → `kanit/kirilma-deneyi.*`

### Aşama 5 — Bitiriş (8–11 Ara)
- [ ] A13 Z10 toparlama · A15 Z0 son hâli (sürümler, tek makine sadeleştirmeleri)
- [ ] A16 Rapor (≤25 sayfa, Bölüm 6 başlıkları), YZ beyanı, katkı beyanı
- [ ] A17 Sıfırdan çalıştırma denemesi (kodu en az yazan üye yapar); `git tag teslim`; SABIS
- [ ] A18 Sunum PDF'i (ilk slayt omurga şeması, 10 dk) + yedek ekran kaydı

## Kararlar günlüğü
(Her önemli kararı tarih, karar, gerekçe ve ölçüm referansıyla buraya ekle.)

- **2026-11-23 · S3 sağlık denetimi `localhost` → `127.0.0.1`** (docker-compose.yml). Windows'ta kapsayıcı içinde `localhost` önce `::1`'e çözülüyor; SeaweedFS yalnızca IPv4 dinliyor → denetim hep başarısız, spark/araclar hiç başlamıyordu. Kanıt: `wget localhost:9333` reddedildi, `127.0.0.1:9333` exit 0. Z11 adayı (ortam farkı bağımlı servisleri durdurdu). README'de belirtilecek.
- **2026-11-23 · Kafka dış portu `9094:9092`** (docker-compose.yml). Kullanıcının başka projesi (`kayma-izleme`) 9092'yi kullanıyor. Kapsayıcı içi aynı: araçlar `kafka:29092`, yönergedeki `exec kafka … localhost:9092` komutları değişmeden çalışır. Bedel: Windows'tan doğrudan bağlanan istemci, duyurulan `localhost:9092`'ye yönlenir (bizde yok). README'de belirtilecek.
- **2026-11-23 · Git Bash'te `dogrula.sh` → `MSYS_NO_PATHCONV=1` gerekli.** Yoksa `name=^/bsm461-kafka$` filtresi yol dönüşümüyle bozulur, yanlış ❌ verir. PowerShell'de `dogrula.py` + `dogrula_spark.py` kullanılabilir.
- **Hipotez (karar değil, ölçülecek) · watermark:** kart 5 s uyarı ↔ 30 dk geç veri ↔ ≤ %0,1 kayıp çelişkisi. Öneri: `update` çıktı kipi + kısa tetikleyici (uyarı süresi için) + uzun watermark (kayıp sınırı için) + MongoDB upsert. Watermark süresi geç veri dağılımı ölçülünce belirlenecek.
