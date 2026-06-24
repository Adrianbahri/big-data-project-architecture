# Tambahkan print() di bawah setiap baris penting
print("DEBUG: Sedang mencoba load data dari ClickHouse...")
df_full = spark.read.format("jdbc")...load()
print("DEBUG: Data berhasil dimuat ke Spark DataFrame!")