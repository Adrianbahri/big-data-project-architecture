import os
# PAKSA SPARK MENGGUNAKAN JAVA 17 (Wajib untuk Mac M-Series)
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
# Gunakan driver JDBC ClickHouse untuk integrasi data yang stabil
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.clickhouse:clickhouse-jdbc:0.4.6 pyspark-shell"

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, Bucketizer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator

# 1. Inisialisasi Spark
print("1. Menginisialisasi Apache Spark...")
spark = SparkSession.builder \
    .appName("TaxiTipPredictionSystem") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("INFO")

# 2. Menyambungkan ke ClickHouse dan Cleaning Data (SQL Push-down)
print("2. Menyambungkan ke ClickHouse dan menarik data bersih...")
# Kita lakukan pembersihan (filter) langsung di SQL agar RAM Spark tidak penuh
query = """(
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
      AND fareAmount > 0 AND fareAmount < 500
      AND tripDistance > 0 AND tripDistance < 100
      AND passenger_count > 0
) AS taxi_data"""

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:clickhouse://localhost:8123/taxi_db") \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("user", "mahasiswa") \
    .option("password", "bigdata123") \
    .option("dbtable", query) \
    .load()

print(f"3. Dataset dimuat! Jumlah baris: {df.count()}")

# 3. Feature & Label Engineering
# Label untuk Klasifikasi: Rendah ($0-$2), Menengah ($2-$5), Tinggi (>$5)
bucketizer = Bucketizer(splits=[0.0, 2.0, 5.0, float('inf')], inputCol="tipAmount", outputCol="tipCategory")
df_labeled = bucketizer.transform(df)

# Merakit semua kolom fitur menjadi satu vektor
assembler = VectorAssembler(
    inputCols=["fareAmount", "tripDistance", "passengerCount", "pickup_zone", "dropoff_zone", "pickup_hour", "pickup_day"], 
    outputCol="features"
)
data_ml = assembler.transform(df_labeled)

# Split data: 80% Training, 20% Testing
train_data, test_data = data_ml.randomSplit([0.8, 0.2], seed=42)

# 4. Pelatihan Model (Cabang Regresi & Klasifikasi)
print("4. Melatih Model...")
# Model Regresi (Nominal)
rf_reg = RandomForestRegressor(featuresCol="features", labelCol="tipAmount", numTrees=10)
model_reg = rf_reg.fit(train_data)

# Model Klasifikasi (Kategori)
rf_class = RandomForestClassifier(featuresCol="features", labelCol="tipCategory", numTrees=10)
model_class = rf_class.fit(train_data)

# 5. Evaluasi
print("5. Evaluasi Metrik Akurasi...")
# Evaluasi Regresi
rmse = RegressionEvaluator(labelCol="tipAmount", metricName="rmse").evaluate(model_reg.transform(test_data))
print(f"   -> RMSE (Regresi): {rmse:.2f}")

# Evaluasi Klasifikasi
acc = MulticlassClassificationEvaluator(labelCol="tipCategory", metricName="accuracy").evaluate(model_class.transform(test_data))
print(f"   -> Akurasi (Klasifikasi): {acc * 100:.2f}%")

# 6. Simpan Model untuk Deployment
print("6. Menyimpan model ke disk...")
model_reg.write().overwrite().save("./taxi_reg_model")
model_class.write().overwrite().save("./taxi_class_model")
print("Selesai! Model siap digunakan untuk dashboard.")