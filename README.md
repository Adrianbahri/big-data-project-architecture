# NYC Green Taxi Data Analytics Platform

Project ini merupakan platform pemrosesan data (data ingestion & storage) berskala besar untuk menganalisis data perjalanan **NYC Green Taxi (2014-2018)**. Platform ini dibangun menggunakan arsitektur data modern dengan teknologi berikut:
- **Docker & Docker Compose**: Orkestrasi kontainer untuk seluruh layanan infrastruktur.
- **ClickHouse**: Database kolomar berkinerja tinggi untuk analitik data berskala besar (~120 juta data).
- **Apache Spark (Master & Worker)**: Framework pemrosesan data terdistribusi untuk komputasi analitik.
- **Apache Kafka & ZooKeeper**: Layer ingestion data streaming secara real-time.
- **Grafana**: Alat visualisasi untuk memantau metrik data perjalanan taksi secara interaktif.
- **Python**: Script otomatisasi untuk inisialisasi tabel, pengunduhan file Parquet dari cloud, dan ingestion data historis.

---

## 🛠️ Prasyarat (Prerequisites)

Sebelum teman Anda mulai menjalankan project ini, pastikan mereka sudah menginstal software berikut di komputer mereka:

1. **Docker Desktop**
   - Digunakan untuk menjalankan database ClickHouse, Kafka, Spark, dan Grafana secara instan.
   - [Download Docker Desktop](https://www.docker.com/products/docker-desktop/)
2. **Python 3.11**
   - **PENTING**: Sangat disarankan untuk menggunakan **Python 3.11**. Hindari versi terbaru (seperti Python 3.14) karena beberapa pustaka sains data seperti `numpy` dan `pandas` akan mengalami error saat kompilasi instalasi.
   - [Download Python](https://www.python.org/downloads/)
3. **Git**
   - Untuk melakukan cloning repositori ini dari GitHub.

---

## 🚀 Langkah-Langkah Instalasi & Setup

### 1. Kloning Repositori & Masuk ke Direktori Project
Buka terminal (macOS/Linux) atau Command Prompt/PowerShell (Windows), lalu jalankan:
```bash
git clone <url-repositori-github-anda>
cd "BIG DATA AKHIR"
```

### 2. Setup Virtual Environment & Install Dependensi Python
Buat lingkungan virtual Python (venv) agar tidak mengganggu instalasi library global Anda.

**Untuk macOS / Linux:**
```bash
# 1. Membuat virtual environment dengan Python 3.11
python3.11 -m venv venv

# 2. Mengaktifkan virtual environment
source venv/bin/activate

# 3. Upgrade pip ke versi terbaru
pip install --upgrade pip

# 4. Menginstal semua dependensi yang diperlukan
pip install -r requirements.txt
```

**Untuk Windows (PowerShell):**
```powershell
# 1. Membuat virtual environment
python -m venv venv

# 2. Mengaktifkan virtual environment
.\venv\Scripts\Activate.ps1

# 3. Upgrade pip
python -m pip install --upgrade pip

# 4. Menginstal dependensi
pip install -r requirements.txt
```

---

### 3. Menjalankan Layanan Docker (ClickHouse, Kafka, Spark, Grafana)
Pastikan Docker Desktop Anda sudah aktif, lalu jalankan perintah berikut untuk mengunduh image dan menjalankan seluruh container di background:
```bash
docker compose up -d
```

#### Detail Layanan yang Berjalan:
| Nama Layanan | Port Eksternal | Kegunaan | Kredensial Default |
| :--- | :--- | :--- | :--- |
| **ClickHouse Server** | `8123` (HTTP) / `9000` (TCP) | Database Analitik Kolom | User: `mahasiswa`, Pass: `bigdata123` |
| **Spark Master** | `8080` (Web UI) / `7077` (Koneksi) | Manajemen cluster Spark | - |
| **Spark Worker** | - | Pekerja pemrosesan Spark (Limit RAM 4GB) | - |
| **Grafana** | `3000` | Dashboard visualisasi analitik | User: `admin`, Pass: `admin123` |
| **Kafka & ZooKeeper** | `9092` | Broker streaming data real-time | - |

Untuk memverifikasi semua container berjalan dengan lancar:
```bash
docker compose ps
```

---

### 4. Inisialisasi Database dan Tabel ClickHouse
Jalankan script Python `create_table.py` untuk otomatis membuat database `taxi_db` dan tabel `green_taxi` di dalam ClickHouse:
```bash
python create_table.py
```

*Skema tabel ini dirancang menggunakan tipe data optimal (seperti `LowCardinality` untuk menghemat penyimpanan) dan menggunakan engine **MergeTree** dengan partisi bulanan (`PARTITION BY toYYYYMM(lpepPickupDatetime)`).*

---

## 📥 Cara Mengimpor Data ke ClickHouse

Ada dua script ingestion yang siap digunakan:

### Opsi A: Ingestion Otomatis dari TLC Cloud (Sangat Direkomendasikan)
Script ini akan mengunduh file dataset `.parquet` resmi secara otomatis dari CloudFront server NYC TLC per bulan (dari Februari 2015 s.d. Desember 2018), membersihkan datanya, dan langsung memasukkannya ke database ClickHouse Anda:
```bash
python import_taxi.py
```
*Script ini sudah ditambahkan fitur penanganan timeout otomatis agar proses pengunduhan file Parquet besar tidak macet di tengah jalan.*

### Opsi B: Ingestion dari File CSV Lokal
Jika Anda memiliki file dataset berukuran besar di lokal komputer Anda (misal `nyc_green_taxi.csv` ~2GB+), Anda bisa memprosesnya dengan:
```bash
python batch_ingest.py
```
*Script ini menggunakan teknik pemrosesan **chunking** sebesar 500.000 baris per iterasi agar penggunaan RAM laptop tetap aman dan tidak freeze.*

---

## 🔍 Cara Verifikasi & Query Data

Anda bisa terhubung langsung ke mesin ClickHouse via Docker menggunakan `clickhouse-client` bawaan kontainer:

```bash
docker exec -it clickhouse clickhouse-client --user mahasiswa --password bigdata123
```

Setelah masuk ke shell ClickHouse client, jalankan query berikut untuk memeriksa isi tabel:
```sql
-- Pindah ke database taxi
USE taxi_db;

-- Menghitung total data yang berhasil masuk ke tabel
SELECT count() FROM green_taxi;

-- Menghitung total baris berdasarkan tipe pembayaran (payment_type)
SELECT payment_type, count() AS total_transaksi
FROM green_taxi
GROUP BY payment_type;

-- Mencari rata-rata biaya perjalanan berdasarkan jumlah penumpang
SELECT passenger_count, avg(fareAmount) AS rata_rata_tarif
FROM green_taxi
GROUP BY passenger_count
ORDER BY passenger_count ASC;
```

Ketik `exit` untuk keluar dari shell ClickHouse client.

---

## 📊 Integrasi Dashboard Grafana

Visualisasikan hasil analisis data Anda dengan langkah berikut:
1. Buka browser Anda dan akses: `http://localhost:3000`.
2. Login menggunakan username `admin` dan password `admin123`.
3. Masuk ke **Connections** -> **Data Sources** -> Klik **Add data source**.
4. Cari dan pilih **ClickHouse** (Plugin `grafana-clickhouse-datasource` sudah otomatis dipasang via Docker Compose).
5. Isi konfigurasi koneksi ClickHouse sebagai berikut:
   - **Server Address**: `clickhouse` (atau `localhost`)
   - **Port**: `9000` (Native Port)
   - **Database**: `taxi_db`
   - **Username**: `mahasiswa`
   - **Password**: `bigdata123`
6. Klik **Save & test**. Jika koneksi berhasil, Anda bisa mulai membuat grafik visualisasi menggunakan SQL queries di Grafana!
