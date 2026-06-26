# Arsitektur Sistem: Kappa Architecture untuk NYC Taxi Tip Prediction

Dokumen ini menjelaskan desain arsitektur proyek, alasan pemilihan Kappa Architecture dibandingkan Lambda Architecture, serta bagaimana aliran data bekerja di sistem kami.

---

# 1. Mengapa Memilih Kappa Architecture dibanding Lambda Architecture?

Lambda Architecture secara historis sangat populer untuk sistem big data karena memisahkan pemrosesan menjadi dua jalur:
1. **Batch Layer**: Memproses data historis dalam jumlah besar untuk akurasi tinggi (tapi lambat).
2. **Speed/Stream Layer**: Memproses data baru secara real-time untuk latensi rendah (cepat tapi kurang presisi).
3. **Serving Layer**: Menggabungkan hasil dari kedua layer untuk disajikan ke user.

Namun, pada proyek ini kami memilih **Kappa Architecture** karena beberapa alasan fundamental berikut:

- **Single Pipeline (Satu Aliran Kode)**: Pada Lambda, kita harus menulis dan memelihara dua codebase berbeda (misal MapReduce untuk batch dan Storm/Flink untuk streaming). Di Kappa, semua data diperlakukan sebagai stream. Kami hanya perlu memelihara satu kode pemrosesan menggunakan PySpark Structured Streaming baik untuk data historis maupun data baru.
- **ClickHouse sebagai Unified Serving Layer**: ClickHouse bertindak sebagai satu-satunya *source of truth*. Data streaming dari Kafka disimpan langsung ke ClickHouse. Saat kami ingin melakukan pelatihan ulang model ML (retraining), Spark membaca langsung dari ClickHouse. Tidak ada pemisahan penyimpanan data antara batch dan real-time.
- **Konsistensi Logika Data**: Karena tidak ada dua layer pemrosesan yang terpisah, risiko perbedaan perhitungan logika data (data drift/logic drift) antara hasil batch dan hasil streaming dapat dihindari sepenuhnya.
- **Efisiensi Resource**: Menghilangkan kebutuhan untuk menjalankan infrastruktur penyimpanan batch yang berat (seperti HDFS lengkap) untuk penyimpanan operasional harian.

---

# 2. Arsitektur Proyek Kami

Sistem kami terdiri dari 4 layer utama yang saling terhubung secara real-time:

```
[ Data Generator ] (Python)
       │ (Simulasi data taksi via JSON)
       ▼
[ Ingestion Layer ] (Apache Kafka: topic 'green-taxi-stream')
       │
       ├──────────────────────────────────────────┐
       ▼ (Stream Ingest)                          ▼ (Real-time Predictor)
[ Processing Layer ] (PySpark)             [ ML Prediction Layer ] (PySpark MLlib)
       │ (Data Cleaning & Parsing)                │ (Predict Tip & Category)
       ▼                                          ▼
[ Serving Layer ] (ClickHouse Database)    [ Output / Console Print ]
       │                                          
       ├──────────────────────────────────────────┐
       ▼ (Query Data Historis)                    ▼ (Query Real-time & Metrik)
[ Batch Retraining ] (Spark MLlib)         [ Visualization ] (Grafana / Streamlit)
```

## Detail Komponen Arsitektur:

### A. Ingestion Layer (Kafka)
- **data_generator.py**: Mensimulasikan aktivitas transaksi taksi nyata dengan membaca dataset NYC Green Taxi dan mengirimkannya ke Kafka broker sebagai message stream format JSON.
- **Kafka Broker**: Mengelola antrean data pada topic `green-taxi-stream` dengan latensi sangat rendah.

### B. Stream Processing & Storage Layer (Spark Streaming + ClickHouse)
- **kappa_stream_ingest.py**: Mengonsumsi data secara real-time dari Kafka menggunakan PySpark Structured Streaming. Script melakukan parsing skema JSON, pembersihan data (data cleansing), dan langsung melakukan pemuatan data (*insert*) ke ClickHouse menggunakan library `clickhouse-connect`.
- **ClickHouse**: Menyimpan data transaksi secara permanen dengan performa baca-tulis yang sangat tinggi untuk analitik skala besar.

### C. Machine Learning Layer (Spark MLlib)
- **spark_batch_training.py**: Melakukan pelatihan awal atau periodik (retraining) model Random Forest (Regresi & Klasifikasi) dengan membaca data historis langsung dari ClickHouse.
- **realtime_predictor.py**: Mengonsumsi stream Kafka secara real-time, mengaplikasikan model Random Forest yang telah dilatih untuk memprediksi besaran tip penumpang secara langsung sebelum data disimpan.

### D. Serving & Visualization Layer (Streamlit + Grafana)
- **Streamlit**: Dashboard interaktif untuk mencoba simulasi prediksi tip secara manual dan memantau sampel metrik evaluasi model.
- **Grafana**: Membaca langsung dari ClickHouse secara real-time untuk memvisualisasikan grafik tren operasional, kecepatan data masuk (*throughput*), serta distribusi transaksi terbaru.
