import clickhouse_connect
from kafka import KafkaProducer
import json
import time
import pandas as pd

print("1. Menghubungkan ke Kafka & ClickHouse...")
# Koneksi Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Koneksi ClickHouse
client = clickhouse_connect.get_client(
    host='localhost', port=8123, 
    username='mahasiswa', password='bigdata123', database='taxi_db'
)

print("2. Mengambil sampel 50.000 perjalanan taksi (Kartu Kredit) dari ClickHouse...")
# Kita filter langsung untuk Kartu Kredit (payment_type=1) dan hapus argo minus
# Catatan: Karena tipAmount tidak diimpor ke tabel ClickHouse, kita simulasikan tipAmount sebesar 15% dari fareAmount
query = """
    SELECT 
        (fareAmount * 0.15) AS tipAmount, 
        fareAmount, 
        tripDistance, 
        passenger_count AS passengerCount,
        toFloat32(puLocationId) AS pickup_zone,
        toFloat32(doLocationId) AS dropoff_zone,
        toHour(lpepPickupDatetime) AS pickup_hour,
        toDayOfWeek(lpepPickupDatetime) AS pickup_day
    FROM green_taxi 
    WHERE payment_type = '1' 
      AND fareAmount > 0 
      AND tripDistance > 0
    LIMIT 50000
"""
# Kita gunakan client.query lalu buat DataFrame secara manual untuk menghindari bug LowCardinality dari clickhouse-connect
result = client.query(query)
df = pd.DataFrame(result.result_rows, columns=result.column_names)

print("3. Memulai Simulasi Streaming Arsitektur Kappa...")
records = df.to_dict(orient='records')

# Menembakkan data satu per satu seperti aliran air (stream)
for i, record in enumerate(records):
    # Tembakkan ke topic Kafka
    producer.send('green-taxi-stream', value=record)
    
    # Beri jeda/laporan agar terlihat efek real-time nya
    if (i + 1) % 5000 == 0:
        producer.flush() # Pastikan semua antrean terkirim
        print(f"    --> {i + 1} data taksi telah mengalir ke Kafka...")
        time.sleep(2) # Jeda simulasi 2 detik setiap 5000 perjalanan

print("\nSimulasi Streaming Selesai! Semua data sudah di dalam Topic Kafka.")