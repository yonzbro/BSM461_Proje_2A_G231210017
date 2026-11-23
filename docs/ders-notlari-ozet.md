# Ders notlarından proje kararlarına giren ölçütler

Kaynak: Dr. Öğr. Üyesi Sümeyye Kaynak, BSM 461 ders notları (H01–H07, 2026-2027 Güz). PDF'ler yerelde
`docs/ders-notlari/` altında (depoya konmaz). Raporda atıf: "H3 §11" gibi hafta + bölüm.

| Konu | Ölçüt / kural | Kaynak | Bizde nerede |
|---|---|---|---|
| Büyük veri tanımı | tahmini_süre(tek makine) > makul_süre_eşiği → büyük | H1 §2.3 | Z1 |
| Bellek çarpanı | Diskte 1 GB CSV bellekte 3–5 GB | H1 §3 | Z1 |
| Okuma hızları | HDD ~200 MB/s · SATA SSD ~550 · NVMe ~5.000 · 1 Gb/s ağ ~125 · 10 Gb/s ~1.250 | H1 §5 | Z1 |
| Eşik tablosu | <1 GB · 1–10 · 10–100 · 100–500 · 500 GB–birkaç TB · >birkaç TB sürekli büyüyor → dağıtık | H1 §7 | Z1 Tablo 4.2 |
| Garanti tablosu | Her ok için: ① kaç kopya ② sıra garantisi ③ bölünmede ne olur | H3 §10 | Z0 |
| Sıcak nokta eşiği | En dolu bölüm ÷ ortalama **> 1,5** ise sıcak nokta vardır | H3 §11 | Z3, Z8 |
| Hash | "Hash anahtarları dağıtır, satırları değil" — tek dev anahtarı hiçbir bölümleme bölemez → tuzlama | H3 §3.2 | Z3 |
| Quorum | W + R > N → okuma son yazmayı görür; tipik N=3 W=2 R=2 | H3 §4.3 | Z6 tartışma |
| Çoğaltma ≠ yedek | Silme komutu da kopyalanır | H3 §4.4 | Z11 |
| CAP / PACELC | P seçilmez, olur; "CA" yok. Cassandra PA/EL, MongoDB PA/EC, HBase PC/EC | H3 §5 | Z3 |
| Mutabakat | Tek sayı düğüm (3/5/7); Kafka 4.0 KRaft = Raft | H3 §6 | Z0 sadeleştirme |
| Olay zamanı ≠ işlem zamanı | Geç veri → watermark (H11) | H3 §7 | Z8 |
| İdempotentlik | Yeniden deneme iki kez yapabilir → idempotent tasarım | H3 §2 | Z3, Z8 |
| Küçük dosya | Bölüm başına en az birkaç yüz MB; 1 MB altı alarm; birleştirme (compaction) | H4 §5.4, §11 | Z4 |
| Nesne deposu | LIST en çok 1.000 anahtar/istek, sırayla; atomik rename yok → yarım klasör hata vermez → tablo biçimi | H4 §6.4 | Z4, Z7 |
| Parquet kodek | snappy en hızlı okuma, zstd en küçük; gzip'li CSV bölünemez | H2 §9 | Z2 |
| Sözlük kodlama | Az çeşit → çok kazanç; istatistikler ancak veri sıralı/bölümlüyse blok atlatır | H2 §7.5 | Z2 |
| Başabaş | N = dönüştürme ÷ (CSV sorgu − Parquet sorgu) | H2 §11 | Z2 |
| Broadcast join | Küçük taraf < 10 MB (`spark.sql.autoBroadcastJoinThreshold`) → BroadcastHashJoin, shuffle yok | H5 §5.2, H6 §5.2 | Z5 (kaynaklar.csv 6 satır) |
| Combiner | Toplam/sayım/max güvenli; ortalama yalnızca (toplam, adet) ile | H5 §4 | Z5 |
| Tuzlama bedeli | Sıcak görev kısa ise tuzlama işi uzatabilir — ölç | H5 §6.2 | Z3/Z5 |
| Garanti etiketi (toplu) | Görevler yeniden çalışır → dönüşümler belirlenimci olmalı; dış sisteme yazan kod iki kez yazabilir | H5 §11, H6 §8 | Z0 |
| Kit Spark | local[*], Driver+tek Executor aynı süreç, 4 çekirdek; shuffle.partitions = 8 (kit) | H6 §1.2, §4.4 | Z0 sadeleştirme |
| Plan okuma | 5 soru: kaç Exchange · neden · partial_ var mı · PushedFilters · ReadSchema | H6 §5.5 | Z5 explain yorumu |
| Spark UI | spark-submit bitince 4040 kapanır → betiğin sonuna `input()` / bekleme | H6 §4.5 | Z5 ekran görüntüsü |
| Küçük veride Spark | ~0,1 s sabit bedel; tek makineye sığan veride pandas/DuckDB hızlı | H6 §6.1 | Z5 tartışma |
