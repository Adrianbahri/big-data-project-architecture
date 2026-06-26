# Dokumentasi Eksekusi & Akses Resource Proyek Big Data

# 1. Daftar Port & Akses Resource

Kafka Broker:
- Host: localhost:9092

ClickHouse Database:
- Host HTTP: localhost:8123
- Host TCP: localhost:9000
- Database: taxi_db
- User: mahasiswa
- Password: bigdata123

Grafana Dashboard:
- URL: http://localhost:3000
- User: admin
- Password: admin123

Spark Master Web UI:
- URL: http://localhost:8080

Streamlit Dashboard:
- URL: http://localhost:8501

---

# 2. Alur Eksekusi Terminal

Jalankan perintah berikut secara berurutan pada terminal terpisah setelah docker compose berjalan:

# Terminal 1: Data Generator
cd "/Users/adrian/Documents/IT/BIG DATA AKHIR"
source venv/bin/activate
cd stream && python data_generator.py

# Terminal 2: Stream Ingest
cd "/Users/adrian/Documents/IT/BIG DATA AKHIR"
source venv/bin/activate
cd stream && python kappa_stream_ingest.py

# Terminal 3: Real-time Predictor
cd "/Users/adrian/Documents/IT/BIG DATA AKHIR"
source venv/bin/activate
cd stream && python realtime_predictor.py

# Terminal 4: Streamlit Dashboard
cd "/Users/adrian/Documents/IT/BIG DATA AKHIR"
source venv/bin/activate
cd dashboard && streamlit run app.py

# Terminal 5: Model Evaluation (Opsional)
cd "/Users/adrian/Documents/IT/BIG DATA AKHIR"
source venv/bin/activate
cd ml && python evaluate_model.py

---

# 3. Cara Memantau Update Database ClickHouse

# Hitung total data hasil streaming
docker exec -it clickhouse clickhouse-client --user mahasiswa --password bigdata123 --database taxi_db --query "SELECT count() FROM green_taxi WHERE VendorID = 'stream'"

# Tampilkan 5 data streaming terbaru
docker exec -it clickhouse clickhouse-client --user mahasiswa --password bigdata123 --database taxi_db --query "SELECT VendorID, lpepPickupDatetime, fareAmount, tip_amount FROM green_taxi WHERE VendorID = 'stream' ORDER BY lpepPickupDatetime DESC LIMIT 5"

# Bandingkan total data batch vs stream
docker exec -it clickhouse clickhouse-client --user mahasiswa --password bigdata123 --database taxi_db --query "SELECT VendorID, count() FROM green_taxi GROUP BY VendorID"

