import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

# 1. Inisialisasi
spark = SparkSession.builder.appName("TaxiPrediction").getOrCreate()

# 2. Muat Model yang sudah dilatih
model = RandomForestRegressionModel.load("./taxi_tip_rf_model")

# 3. Simulasi Input Data Baru (Contoh: Penumpang baru saja turun)
data_baru = [(15.5, 3.2, 1, 12, 140, 9, 2)] # fareAmount, dist, pax, pu, do, hour, day
columns = ["fareAmount", "tripDistance", "passengerCount", "pickup_zone", "dropoff_zone", "pickup_hour", "pickup_day"]
df_input = spark.createDataFrame(data_baru, columns)

# 4. Assembling (Wajib mengikuti kolom yang sama saat training!)
assembler = VectorAssembler(inputCols=columns, outputCol="features")
df_features = assembler.transform(df_input)

# 5. Prediksi!
prediksi = model.transform(df_features)
print(f"Prediksi Tip untuk Penumpang ini adalah: ${prediksi.select('prediction').collect()[0][0]:.2f}")