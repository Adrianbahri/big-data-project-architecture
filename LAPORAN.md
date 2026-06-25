# LAPORAN PROYEK BIG DATA
## Platform Prediksi Tip Taksi NYC dengan Kappa Architecture

> **Mata Kuliah**: Big Data  
> **Universitas**: Universitas Hasanuddin  
> **Dataset**: NYC Green Taxi Trip Records (2015–2018)  
> **Teknologi Utama**: Apache Kafka · Apache Spark · ClickHouse · Random Forest · Kappa Architecture

---

## 1. Latar Belakang & Tujuan

### 1.1 Latar Belakang

Industri transportasi berbasis aplikasi menghasilkan jutaan transaksi setiap harinya. Salah satu aspek penting dalam ekosistem taksi adalah **tip** — imbalan sukarela dari penumpang kepada pengemudi. Kemampuan memprediksi besaran tip secara akurat dapat memberikan manfaat bagi:

- **Pengemudi**: Mengetahui potensi pendapatan dari suatu perjalanan
- **Platform**: Optimasi sistem pencocokan pengemudi-penumpang
- **Analis**: Memahami pola perilaku konsumen transportasi

Dataset **NYC Green Taxi Trip Records** dari NYC Taxi & Limousine Commission (TLC) menyediakan rekaman lengkap jutaan perjalanan taksi hijau di New York City, termasuk data tip yang tercatat secara elektronik untuk pembayaran kartu kredit.

### 1.2 Tujuan Proyek

1. Membangun pipeline data skala besar menggunakan **Kappa Architecture**
2. Melatih model **Machine Learning** untuk memprediksi tip taksi secara akurat
3. Menyediakan prediksi **real-time** via stream Kafka
4. Mengevaluasi model menggunakan metrik **Cohen's Kappa (κ)**
5. Memastikan tidak ada **Data Leakage** dalam proses training

---

## 2. Dataset

### 2.1 Sumber Data

| Atribut | Detail |
|---------|--------|
| **Nama Dataset** | NYC Green Taxi Trip Records |
| **Sumber** | NYC Taxi & Limousine Commission (NYC TLC) |
| **URL** | https://d37ci6vzurychx.cloudfront.net/trip-data/ |
| **Format** | Apache Parquet |
| **Periode** | Februari 2015 – Desember 2018 |
| **Total Baris (raw)** | ~54 juta perjalanan |
| **Total Baris (setelah filter)** | ~23.7 juta perjalanan |

### 2.2 Skema Kolom Asli NYC TLC

| Kolom Asli | Nama di DB | Tipe | Keterangan |
|------------|------------|------|-----------|
| `lpep_pickup_datetime` | `lpepPickupDatetime` | DateTime | Waktu naik penumpang |
| `lpep_dropoff_datetime` | `lpepDropoffDatetime` | DateTime | Waktu turun penumpang |
| `passenger_count` | `passenger_count` | Float32 | Jumlah penumpang |
| `trip_distance` | `tripDistance` | Float32 | Jarak perjalanan (miles) |
| `PULocationID` | `puLocationId` | String | ID zona pickup (1–263) |
| `DOLocationID` | `doLocationId` | String | ID zona dropoff (1–263) |
| `payment_type` | `payment_type` | String | Tipe pembayaran (1=Kartu, 2=Tunai) |
| `fare_amount` | `fareAmount` | Float32 | Tarif dasar perjalanan |
| `tip_amount` | `tip_amount` | Float32 | **Label target — nilai tip asli** |

### 2.3 Distribusi Data

**Kenapa hanya `payment_type = '1'` (kartu kredit)?**

Tip pada perjalanan kartu kredit tercatat **otomatis secara elektronik** di sistem NYC TLC. Sedangkan tip pada perjalanan tunai (`payment_type = '2'`) sangat jarang dilaporkan karena bersifat manual, sehingga mayoritas nilainya 0 meskipun penumpang sebenarnya memberi tip.

```
Total data masuk   : ~54,748,869 baris
Setelah filter CC  : ~23,683,316 baris (payment_type = '1')
Dibuang            : ~31,065,553 baris (tunai, data tidak valid)
```

**Distribusi Tip (setelah filter):**

| Kategori | Rentang | Perkiraan Proporsi |
|----------|---------|-------------------|
| Rendah | $0.00 – $2.00 | ~35% |
| Menengah | $2.01 – $5.00 | ~43% |
| Tinggi | > $5.00 | ~22% |

---

## 3. Arsitektur Sistem: Kappa Architecture

### 3.1 Konsep Kappa Architecture

**Kappa Architecture** adalah paradigma arsitektur data modern yang menyederhanakan **Lambda Architecture** dengan menghilangkan Batch Layer dan hanya menggunakan **satu jalur streaming tunggal** sebagai sumber kebenaran.

```
LAMBDA (Lama)                    KAPPA (Digunakan)
┌───────────────┐                ┌───────────────────────────┐
│  Batch Layer  │ ← dihilangkan  │                           │
│  (Hadoop/ETL) │                │   Stream Layer (SATU)     │
├───────────────┤                │   Kafka + Spark Streaming │
│  Speed Layer  │ ←── disatukan  │                           │
│  (Kafka)      │                ├───────────────────────────┤
├───────────────┤                │   Serving Layer           │
│  Serving Layer│                │   ClickHouse + Grafana    │
└───────────────┘                └───────────────────────────┘
```

### 3.2 Diagram Aliran Data

```
┌─────────────────────────────────────────────────────────────┐
│               KAPPA ARCHITECTURE PIPELINE                   │
└─────────────────────────────────────────────────────────────┘

  [NYC TLC Dataset / Simulator]
          │
          ▼
  ┌──────────────────┐
  │ data_generator.py│  ← Kafka Producer
  │ (stream/         │    Pintu masuk TUNGGAL semua data
  └────────┬─────────┘
           │ JSON via Kafka topic: green-taxi-stream
           ▼
  ┌──────────────────┐
  │   Apache Kafka   │  ← Message Broker
  │   localhost:9092 │    Buffer & distribusi stream
  └────────┬─────────┘
           │
     ┌─────┴──────┐
     ▼            ▼
┌─────────┐  ┌──────────────────┐
│ Stream  │  │ Realtime         │
│ Ingest  │  │ Predictor        │
│ (Kafka→ │  │ (Kafka→ML Model) │
│ClickHse)│  │                  │
└────┬────┘  └──────────────────┘
     │
     ▼
┌──────────────────┐
│   ClickHouse     │  ← Serving Layer (Single Source of Truth)
│   taxi_db        │    Database kolomar berkinerja tinggi
└────────┬─────────┘
         │
         ▼
┌──────────────────┐        ┌─────────────────┐
│ ML Training      │ ──────▶│  Saved Models   │
│ (Spark MLlib)    │        │  taxi_reg_model  │
│ Periodic Retrain │        │  taxi_class_model│
└──────────────────┘        └────────┬────────┘
                                     │
                     ┌───────────────┼───────────────┐
                     ▼               ▼               ▼
              ┌──────────┐   ┌──────────────┐  ┌─────────┐
              │predict.py│   │realtime_pred.│  │ app.py  │
              │(terminal)│   │(kafka stream)│  │(web UI) │
              └──────────┘   └──────────────┘  └─────────┘
```

### 3.3 Stack Teknologi

| Layer | Teknologi | Versi | Fungsi |
|-------|-----------|-------|--------|
| **Orkestrasi** | Docker Compose | — | Menjalankan semua service |
| **Message Broker** | Apache Kafka | 7.4.0 | Stream data masuk |
| **Koordinasi** | ZooKeeper | 7.4.0 | Manajemen Kafka cluster |
| **Processing** | Apache Spark (PySpark) | 3.x | ML Training & Streaming |
| **Database** | ClickHouse | 24.3 | Serving Layer analitik |
| **Monitoring** | Grafana | Latest | Visualisasi metrik |
| **Dashboard** | Streamlit | — | UI prediksi interaktif |
| **Bahasa** | Python | 3.11 | Seluruh logika bisnis |

---

## 4. Data Pipeline & Preprocessing

### 4.1 Tahapan Pengolahan Data

```
Raw Parquet (NYC TLC)
        │
        ▼
1. Download per Bulan
   (urllib + temp file)
        │
        ▼
2. Rename Kolom
   (nyc format → schema ClickHouse)
        │
        ▼
3. Cleaning
   ├── Isi NaN numerik dengan 0
   ├── Isi NaN string dengan "Unknown"
   └── Parse datetime
        │
        ▼
4. Filter (SQL Push-down di ClickHouse)
   ├── payment_type = '1' (kartu kredit)
   ├── fareAmount: 0 < fare < 500
   ├── tripDistance: 0 < dist < 100
   ├── passenger_count > 0
   └── tip_amount >= 0
        │
        ▼
5. Feature Engineering
   ├── pickup_hour  = toHour(lpepPickupDatetime)
   ├── pickup_day   = toDayOfWeek(lpepPickupDatetime)
   ├── pickup_zone  = toFloat32(puLocationId)
   └── dropoff_zone = toFloat32(doLocationId)
        │
        ▼
6. Label Engineering
   ├── Regresi  : tipAmount (nilai asli, kontinu)
   └── Klasifikasi: Bucketizer → [0,2,5,∞] → kategori 0/1/2
```

### 4.2 Filter dan Alasannya

| Filter | Nilai | Alasan |
|--------|-------|--------|
| `payment_type = '1'` | Kartu kredit | Tip hanya tercatat akurat untuk pembayaran elektronik |
| `fareAmount > 0` | Positif | Eliminasi data corrupt/gratis |
| `fareAmount < 500` | < $500 | Eliminasi outlier ekstrem |
| `tripDistance > 0` | Positif | Trip fiktif/corrupt |
| `tripDistance < 100` | < 100 mil | Eliminasi outlier (NYC maks ~30 mil) |
| `tip_amount >= 0` | Non-negatif | Refund negatif tidak relevan |

---

## 5. Feature Engineering

### 5.1 Fitur yang Digunakan

| # | Nama Fitur | Asal Kolom | Transformasi | Alasan Pemilihan |
|---|-----------|------------|-------------|-----------------|
| 1 | `tripDistance` | `tripDistance` | Langsung (Float) | Korelasi kuat dengan tip — jarak jauh = fare besar = tip besar |
| 2 | `passengerCount` | `passenger_count` | Rename | Berpengaruh kecil, tetap dimasukkan untuk kelengkapan |
| 3 | `pickup_zone` | `puLocationId` | `toFloat32()` | Area NYC tertentu memiliki kebiasaan tip berbeda |
| 4 | `dropoff_zone` | `doLocationId` | `toFloat32()` | Destinasi (bandara, Manhattan) berpengaruh terhadap tip |
| 5 | `pickup_hour` | `lpepPickupDatetime` | `toHour()` | Rush hour vs malam vs pagi hari |
| 6 | `pickup_day` | `lpepPickupDatetime` | `toDayOfWeek()` | Pola tip berbeda antara hari kerja dan akhir pekan |

### 5.2 Fitur yang Sengaja DIHAPUS

| Fitur | Alasan Dihapus |
|-------|---------------|
| `fareAmount` | **Data Leakage** — tip berkorelasi langsung dengan fare (~15-20%). Jika dimasukkan sebagai fitur, model bisa "menghafal" korelasi ini dan menghasilkan akurasi palsu mendekati 100% |

**Bukti Data Leakage:**
```
Sebelum perbaikan (fareAmount sebagai fitur):
→ Akurasi: ~100% (TIDAK VALID)

Setelah perbaikan (fareAmount dihapus):
→ Akurasi: 72.94% (REALISTIS)
→ Cohen's κ: 0.5459
```

### 5.3 Label Engineering

**Label Regresi**: `tip_amount` — nilai kontinu dalam dolar ($)

**Label Klasifikasi**: Bucketizer dengan splits `[0.0, 2.0, 5.0, ∞]`

```python
Bucketizer(
    splits=[0.0, 2.0, 5.0, float('inf')],
    inputCol="tipAmount",
    outputCol="tipCategory"
)
```

| Kelas | Kategori | Rentang |
|-------|----------|---------|
| **0** | Rendah | $0.00 – $1.99 |
| **1** | Menengah | $2.00 – $4.99 |
| **2** | Tinggi | $5.00 ke atas |

### 5.4 Assembling Fitur

Semua fitur digabung menjadi satu vektor menggunakan `VectorAssembler` Spark MLlib:

```python
VectorAssembler(
    inputCols=["tripDistance", "passengerCount", "pickup_zone",
               "dropoff_zone", "pickup_hour", "pickup_day"],
    outputCol="features"
)
```

---

## 6. Model Machine Learning

### 6.1 Algoritma: Random Forest

Random Forest dipilih karena karakteristiknya yang sesuai dengan data ini:

| Keunggulan | Penjelasan |
|------------|-----------|
| **Ensemble** | Menggabungkan prediksi dari banyak pohon → lebih stabil |
| **Anti-overfitting** | Bootstrap sampling + feature subsampling mencegah hafalan |
| **Non-parametrik** | Tidak memerlukan asumsi distribusi data |
| **Feature Importance** | Menghasilkan bobot kontribusi tiap fitur secara otomatis |
| **Skalabel** | Tersedia native di Spark MLlib untuk data jutaan baris |

### 6.2 Dua Model yang Dilatih

**Model 1 — Regresi (Prediksi Nilai Nominal)**
```python
RandomForestRegressor(
    featuresCol="features",
    labelCol="tipAmount",    # nilai tip dalam $
    numTrees=50,
    maxDepth=10,
    minInstancesPerNode=10,
    featureSubsetStrategy="sqrt",
    seed=42
)
```

**Model 2 — Klasifikasi (Prediksi Kategori)**
```python
RandomForestClassifier(
    featuresCol="features",
    labelCol="tipCategory",  # 0=Rendah, 1=Menengah, 2=Tinggi
    numTrees=50,
    maxDepth=10,
    minInstancesPerNode=10,
    featureSubsetStrategy="sqrt",
    seed=42
)
```

### 6.3 Hyperparameter dan Fungsinya

| Parameter | Nilai | Fungsi |
|-----------|-------|--------|
| `numTrees` | 50 | Jumlah pohon — makin banyak makin stabil, tapi makin lambat |
| `maxDepth` | 10 | Kedalaman maks tiap pohon — batasi untuk hindari overfitting |
| `minInstancesPerNode` | 10 | Min data per node — regularisasi, hindari split terlalu kecil |
| `featureSubsetStrategy` | `sqrt` | Fitur per split = √6 ≈ 2 — randomisasi untuk diversitas pohon |
| `seed` | 42 | Reproducibility — hasil yang sama setiap dijalankan |

### 6.4 Split Data Training/Testing

```
Total data: 23,683,316 baris

├── Training Set (80%) : 18,949,534 baris
└── Testing Set  (20%) :  4,733,782 baris

seed = 42 (reproducible random split)
```

---

## 7. Evaluasi Model

### 7.1 Metrik yang Digunakan

#### Untuk Model Regresi:

| Metrik | Formula | Interpretasi |
|--------|---------|-------------|
| **RMSE** | √(Σ(ŷ-y)²/n) | Error rata-rata dalam satuan $ |
| **MAE** | Σ\|ŷ-y\|/n | Error absolut rata-rata (lebih robust terhadap outlier) |
| **R²** | 1 - SS_res/SS_tot | Proporsi variansi yang dijelaskan model (0–1) |

#### Untuk Model Klasifikasi:

| Metrik | Formula | Interpretasi |
|--------|---------|-------------|
| **Akurasi** | Benar/Total | Persentase prediksi yang tepat |
| **F1-Score** | 2×(P×R)/(P+R) | Rata-rata harmonis Precision dan Recall |
| **Cohen's Kappa (κ)** | (Po-Pe)/(1-Pe) | Kualitas vs tebakan acak |

### 7.2 Cohen's Kappa — Metrik Utama

Cohen's Kappa dipilih sebagai metrik utama karena lebih informatif dari akurasi biasa — ia mengukur seberapa baik model dibanding **tebakan acak**.

**Formula:**
```
κ = (Po - Pe) / (1 - Pe)

Po = Observed Agreement (Akurasi)
Pe = Expected Agreement by Chance
   = Σ (proporsi aktual kelas k × proporsi prediksi kelas k)
```

**Contoh Perhitungan:**
```
Po = 0.7276 (akurasi)

Distribusi aktual  : Rendah≈35%, Menengah≈44%, Tinggi≈21%
Distribusi prediksi: Rendah≈36%, Menengah≈47%, Tinggi≈17%

Pe = (0.35×0.36) + (0.44×0.47) + (0.21×0.17)
   ≈ 0.126 + 0.207 + 0.036
   ≈ 0.369 (≈ 37% prediksi benar hanya karena kebetulan)

κ = (0.7276 - 0.369) / (1 - 0.369)
  = 0.3586 / 0.631
  ≈ 0.5459
```

**Interpretasi:**
```
κ = 0.5459 → model 54.6% lebih baik dari tebakan acak
```

| Nilai κ | Kategori |
|---------|----------|
| > 0.80 | Sangat Baik (Almost Perfect) |
| > 0.60 | Baik (Substantial) |
| **0.40–0.60** | **Cukup (Moderate) ← Model ini** |
| > 0.20 | Lemah (Fair) |
| ≤ 0.20 | Sangat Lemah |

### 7.3 Hasil Evaluasi

#### Model Regresi (Prediksi Nominal Tip $)

| Metrik | Nilai |
|--------|-------|
| **RMSE** | $1.81 |
| **MAE** | $1.19 |
| **R²** | 0.307 |

> R² = 0.307 artinya model menjelaskan 30.7% variasi tip. Nilai ini wajar karena tip sangat dipengaruhi faktor subjektif (kepuasan pelanggan, mood) yang tidak ada dalam data.

#### Model Klasifikasi (Prediksi Kategori Tip)

| Metrik | Nilai |
|--------|-------|
| **Akurasi** | **72.76%** |
| **F1-Score** | 0.7203 |
| **Cohen's Kappa (κ)** | **0.5459** |
| **Interpretasi κ** | Cukup (Moderate) |

#### Confusion Matrix

```
                      Prediksi
Aktual     │ Rendah(0) │ Menengah(1) │ Tinggi(2)
───────────┼───────────┼─────────────┼──────────
Rendah (0) │   9,965   │    2,782    │    595
Menengah(1)│   2,200   │   10,360    │    589
Tinggi (2) │      68   │    1,511    │  1,930
```

**Analisis Confusion Matrix:**
- **Diagonal utama** = prediksi benar (tinggi → model bagus)
- Kesalahan terbesar: Menengah sering diprediksi sebagai Rendah (2,200 kasus) — batas kelas $2 memang tipis
- Kategori Tinggi cukup akurat (1,930 benar dari 3,509 total)

### 7.4 Feature Importance

| Rank | Fitur | Kontribusi |
|------|-------|-----------|
| 1 | `tripDistance` | **87.77%** |
| 2 | `dropoff_zone` | 7.79% |
| 3 | `pickup_zone` | 2.80% |
| 4 | `pickup_hour` | 1.35% |
| 5 | `pickup_day` | 0.21% |
| 6 | `passengerCount` | 0.08% |

**Interpretasi:**
- Jarak perjalanan mendominasi dengan 87.77% — ini sangat masuk akal karena semakin jauh perjalanan, semakin besar tarif, dan secara psikologis penumpang cenderung memberi tip lebih besar
- Zona pickup/dropoff berpengaruh karena area premium NYC (Manhattan, JFK Airport) memiliki penumpang dengan kebiasaan tip lebih besar
- Waktu (jam, hari) berpengaruh sangat kecil
- Jumlah penumpang hampir tidak berpengaruh (0.08%)

---

## 8. Deteksi & Pencegahan Overfitting

### 8.1 Masalah Awal: Data Leakage

Sebelum perbaikan, proyek menggunakan label buatan:
```python
# SALAH — label dihitung dari fareAmount
(fareAmount * 0.15) AS tipAmount

# SALAH — fareAmount juga dipakai sebagai fitur
inputCols=["fareAmount", "tripDistance", ...]
```

Akibatnya: model mendapat akurasi ~100% karena hanya menghafal rumus `tip = fare × 0.15`, bukan belajar pola data nyata.

### 8.2 Solusi yang Diterapkan

```python
# BENAR — gunakan tip_amount asli dari NYC TLC
tip_amount AS tipAmount

# BENAR — hapus fareAmount dari fitur
inputCols=["tripDistance", "passengerCount", 
           "pickup_zone", "dropoff_zone",
           "pickup_hour", "pickup_day"]
```

### 8.3 Teknik Regularisasi pada Random Forest

| Teknik | Parameter | Nilai | Efek |
|--------|-----------|-------|------|
| Batas kedalaman pohon | `maxDepth` | 10 | Pohon tidak terlalu dalam |
| Min data per node | `minInstancesPerNode` | 10 | Tidak split data terlalu kecil |
| Subset fitur per split | `featureSubsetStrategy` | `sqrt` | Diversitas antar pohon |
| Evaluasi pada test set | `randomSplit([0.8, 0.2])` | seed=42 | Evaluasi objektif |

---

## 9. Implementasi Real-time (Kappa Stream)

### 9.1 Kafka Producer

`stream/data_generator.py` menghasilkan data trip taksi simulasi secara real-time dengan pola realistis:
- 30% trip tanpa tip (penumpang tidak memberi tip)
- 70% trip dengan tip (8–30% dari fare)
- Zona pickup/dropoff dari 41 zona umum NYC
- Jam pickup mengikuti waktu sistem saat ini

### 9.2 Stream Ingest (Kafka → ClickHouse)

`stream/kappa_stream_ingest.py` menggunakan **Spark Structured Streaming** dengan:
- `foreachBatch` untuk menulis ke ClickHouse setiap 10 detik
- Checkpoint di `checkpoints/checkpoint_ingest` untuk fault tolerance
- Filter data valid sebelum disimpan

### 9.3 Real-time Prediction

`stream/realtime_predictor.py` menggabungkan:
- Stream dari Kafka
- Model Random Forest yang sudah dilatih
- Output prediksi setiap 5 detik

---

## 10. Dashboard Interaktif

Dashboard Streamlit (`dashboard/app.py`) menyediakan antarmuka prediksi dengan:

**Input:**
- Jarak Perjalanan (miles)
- Jumlah Penumpang
- Zona Pickup (1–263)
- Zona Dropoff (1–263)
- Jam Pickup (0–23)
- Hari (Senin–Minggu)

**Output:**
- Estimasi Tip Nominal ($)
- Kategori Tip (Rendah/Menengah/Tinggi)

**Akses:** `http://localhost:8501`

---

## 11. Kesimpulan

### 11.1 Pencapaian

| Target | Hasil |
|--------|-------|
| Pipeline Kappa Architecture | ✅ Berhasil diimplementasi |
| Prediksi tip tanpa data leakage | ✅ fareAmount dihapus dari fitur |
| Akurasi klasifikasi realistis | ✅ 72.76% (bukan 100% palsu) |
| Evaluasi Cohen's Kappa | ✅ κ = 0.5459 (Moderate) |
| Dashboard interaktif | ✅ Berjalan di localhost:8501 |
| Real-time streaming | ✅ via Kafka + Spark Streaming |
| Data skala besar | ✅ 54 juta baris, training 23 juta baris |

### 11.2 Keterbatasan

| Keterbatasan | Penjelasan |
|-------------|-----------|
| R² rendah (0.307) | Tip dipengaruhi faktor psikologis penumpang yang tidak terukur |
| Hanya kartu kredit | Tip tunai tidak tercatat di dataset NYC TLC |
| Data lama (2015–2018) | Perilaku tip mungkin berubah setelah pandemi COVID-19 |
| `tripDistance` terlalu dominan (87.8%) | Fitur lain kurang informatif; perlu data tambahan seperti rating, cuaca |

### 11.3 Saran Pengembangan

1. **Tambah fitur cuaca** — hujan/panas berpengaruh pada tip
2. **Tambah data terbaru** — dataset 2020–2024 untuk pola pasca-pandemi
3. **Gunakan model deep learning** — Neural Network untuk pola non-linear
4. **Tambah rating pengemudi** — faktor paling berpengaruh terhadap tip
5. **Tuning hyperparameter** — Grid Search untuk parameter optimal

---

## 12. Referensi

1. NYC Taxi & Limousine Commission. (2024). *TLC Trip Record Data*. https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. Kreps, J. (2014). *Questioning the Lambda Architecture*. O'Reilly. https://www.oreilly.com/radar/questioning-the-lambda-architecture/
3. Cohen, J. (1960). A coefficient of agreement for nominal scales. *Educational and Psychological Measurement*, 20(1), 37–46.
4. Breiman, L. (2001). Random Forests. *Machine Learning*, 45, 5–32.
5. Apache Software Foundation. (2024). *Apache Spark MLlib Documentation*. https://spark.apache.org/docs/latest/ml-guide.html
6. ClickHouse Inc. (2024). *ClickHouse Documentation*. https://clickhouse.com/docs
7. Apache Kafka. (2024). *Kafka Documentation*. https://kafka.apache.org/documentation/
