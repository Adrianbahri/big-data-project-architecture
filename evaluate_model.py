import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler, Bucketizer

# 1. Inisialisasi
# Pastikan konfigurasi paket ada di SparkSession.builder
spark = SparkSession.builder \
    .appName("ModelEvaluation") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.4.6") \
    .getOrCreate()

# 2. Muat Model
model_reg = RandomForestRegressionModel.load("./taxi_reg_model")
model_class = RandomForestClassificationModel.load("./taxi_class_model")

# 3. Muat Data Uji (Gunakan query yang sama dengan saat training, 
# tapi pakai LIMIT kecil saja agar cepat, misalnya 10.000 baris)
query = """(
    SELECT (fareAmount * 0.15) AS tipAmount, fareAmount, tripDistance, 
           passenger_count AS passengerCount, toFloat32(puLocationId) AS pickup_zone, 
           toFloat32(doLocationId) AS dropoff_zone, toHour(lpepPickupDatetime) AS pickup_hour, 
           toDayOfWeek(lpepPickupDatetime) AS pickup_day
    FROM green_taxi WHERE payment_type = '1' LIMIT 10000
) AS taxi_data"""

df = spark.read.format("jdbc").option("url", "jdbc:clickhouse://localhost:8123/taxi_db") \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver").option("dbtable", query) \
    .option("user", "mahasiswa").option("password", "bigdata123").load()

# 4. Feature Engineering yang sama dengan training
bucketizer = Bucketizer(splits=[0.0, 2.0, 5.0, float('inf')], inputCol="tipAmount", outputCol="tipCategory")
df_labeled = bucketizer.transform(df)

assembler = VectorAssembler(
    inputCols=["fareAmount", "tripDistance", "passengerCount", "pickup_zone", "dropoff_zone", "pickup_hour", "pickup_day"], 
    outputCol="features"
)
test_data = assembler.transform(df_labeled)

# 5. Tampilkan Confusion Matrix (Klasifikasi)
print("\n--- Confusion Matrix ---")
predictions = model_class.transform(test_data)
predictions.crosstab('tipCategory', 'prediction').show()

# 6. Tampilkan Feature Importance (Regresi)
feature_names = ["fareAmount", "tripDistance", "passengerCount", "pickup_zone", "dropoff_zone", "pickup_hour", "pickup_day"]
importances = model_reg.featureImportances
print("\n--- Feature Importance ---")
for name, imp in zip(feature_names, importances):
    print(f"{name}: {imp:.4f}")