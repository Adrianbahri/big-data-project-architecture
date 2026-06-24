import pandas as pd
import clickhouse_connect
import time

# 1. Koneksi ke ClickHouse menggunakan kredensial praktikum
client = clickhouse_connect.get_client(
    host='localhost', 
    port=8123, 
    username='mahasiswa', 
    password='bigdata123'
)

# GANTI dengan nama file CSV dataset taksi 2GB Anda yang sebenarnya
FILE_PATH = 'nyc_green_taxi.csv'  
CHUNK_SIZE = 500000  # Memproses 500.000 baris per iterasi agar RAM laptop aman

print("Memulai proses Ingestion Data Historis ke ClickHouse...")
start_time = time.time()
total_inserted = 0

# Sesuaikan dengan kolom yang kita buat di tabel ClickHouse
columns_to_read = [
    'VendorID', 'lpepPickupDatetime', 'lpepDropoffDatetime', 
    'passenger_count', 'tripDistance', 'puLocationId', 
    'doLocationId', 'RatecodeID', 'payment_type', 'fareAmount'
]

try:
    # Baca CSV secara bertahap menggunakan chunksize
    for chunk in pd.read_csv(FILE_PATH, chunksize=CHUNK_SIZE, usecols=columns_to_read, dtype=str):
        # Konversi tipe data tanggal
        chunk['lpepPickupDatetime'] = pd.to_datetime(chunk['lpepPickupDatetime'])
        chunk['lpepDropoffDatetime'] = pd.to_datetime(chunk['lpepDropoffDatetime'])
        
        # Konversi numerik dan tangani nilai kosong (NaN) agar ClickHouse tidak error
        numerics = ['passenger_count', 'tripDistance', 'fareAmount']
        for col in numerics:
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(0.0)
            
        # Isi nilai string yang kosong
        strings = ['VendorID', 'puLocationId', 'doLocationId', 'RatecodeID', 'payment_type']
        chunk[strings] = chunk[strings].fillna('Unknown')

        # Masukkan potongan data (dataframe) ke ClickHouse
        client.insert_df('taxi_db.green_taxi', chunk)
        
        total_inserted += len(chunk)
        print(f"Berhasil memasukkan {total_inserted:,} baris...")

except Exception as e:
    print(f"Terjadi kesalahan: {e}")

print(f"\nSelesai! Total {total_inserted:,} data dimasukkan dalam {time.time() - start_time:.2f} detik.")