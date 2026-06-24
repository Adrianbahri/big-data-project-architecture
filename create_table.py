import clickhouse_connect

# 1. Koneksi ke ClickHouse lokal
# PERBAIKAN: Menggunakan kredensial dari environment Docker Anda
client = clickhouse_connect.get_client(
    host='localhost', 
    port=8123, 
    username='mahasiswa', 
    password='bigdata123'
)

# 2. Buat Database
client.command('CREATE DATABASE IF NOT EXISTS taxi_db')

# 3. Buat Tabel MergeTree yang Dioptimalkan
schema_query = """
CREATE TABLE IF NOT EXISTS taxi_db.green_taxi (
    VendorID LowCardinality(String),
    lpepPickupDatetime DateTime,
    lpepDropoffDatetime DateTime,
    passenger_count Float32,
    tripDistance Float32,
    puLocationId LowCardinality(String),
    doLocationId LowCardinality(String),
    RatecodeID LowCardinality(String),
    payment_type LowCardinality(String),
    fareAmount Float32
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(lpepPickupDatetime)
ORDER BY (puLocationId, lpepPickupDatetime)
SETTINGS index_granularity = 8192;
"""
client.command(schema_query)
print("Tabel green_taxi berhasil dibuat!")