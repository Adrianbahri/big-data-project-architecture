from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

print("1. Menginisialisasi Apache Spark (Membutuhkan beberapa detik)...")
# Kita harus memasukkan package khusus agar Spark bisa membaca Kafka
spark = SparkSession.builder \
    .appName("TaxiTipPredictionKappa") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
    .getOrCreate()

# Mengurangi log bawaan Spark yang terlalu cerewet
spark.sparkContext.setLogLevel("WARN")

print("2. Mendefinisikan Skema Data Taksi...")
# Skema ini harus sama persis dengan data yang dikirim oleh producer Python Anda
schema = StructType([
    StructField("tipAmount", DoubleType(), True),
    StructField("fareAmount", DoubleType(), True),
    StructField("tripDistance", DoubleType(), True),
    StructField("passengerCount", IntegerType(), True),
    StructField("pickup_zone", DoubleType(), True),
    StructField("dropoff_zone", DoubleType(), True),
    StructField("pickup_hour", IntegerType(), True),
    StructField("pickup_day", IntegerType(), True)
])

print("3. Mulai Menyedot Data dari Kafka secara Real-Time...")
# Membaca stream dari topik 'green-taxi-stream' di Kafka
df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-taxi-stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Mengubah data Kafka yang berupa biner (byte) menjadi bentuk Tabel/Dataframe JSON
df_parsed = df_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

print("4. Menampilkan Aliran Data ke Terminal...")
# Tampilkan aliran data ke layar secara langsung (console)
query = df_parsed \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="2 seconds") \
    .start()

query.awaitTermination()