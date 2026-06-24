import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

import streamlit as st
from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler

# Inisialisasi Spark (Mode local agar ringan untuk Dashboard)
@st.cache_resource
def get_spark():
    return SparkSession.builder.appName("TaxiDashboard").master("local[*]").getOrCreate()

spark = get_spark()

# Memuat Model Regresi dan Klasifikasi
@st.cache_resource
def load_models():
    reg_model = RandomForestRegressionModel.load("./taxi_reg_model")
    class_model = RandomForestClassificationModel.load("./taxi_class_model")
    return reg_model, class_model

model_reg, model_class = load_models()

# Tampilan Dashboard
st.title("🚖 Prediksi Tip Taksi NYC (AI Powered)")
st.write("Sistem ini memprediksi nilai nominal dan kategori tip penumpang.")

# Widget Input User
fare = st.number_input("Fare Amount ($)", min_value=0.0, value=15.0)
dist = st.number_input("Trip Distance (miles)", min_value=0.0, value=2.0)
pax = st.slider("Passenger Count", 1, 6, 1)

# Tombol Prediksi
if st.button("Prediksi Tip"):
    # Membentuk DataFrame untuk prediksi
    # Nilai default untuk pickup/dropoff/waktu
    data = [(fare, dist, pax, 12.0, 140.0, 12, 3)] 
    cols = ["fareAmount", "tripDistance", "passengerCount", "pickup_zone", "dropoff_zone", "pickup_hour", "pickup_day"]
    df = spark.createDataFrame(data, cols)
    
    # Assembly fitur
    assembler = VectorAssembler(inputCols=cols, outputCol="features")
    df_features = assembler.transform(df)
    
    # Prediksi Regresi
    pred_nominal = model_reg.transform(df_features).select("prediction").collect()[0][0]
    
    # Prediksi Klasifikasi
    pred_class = model_class.transform(df_features).select("prediction").collect()[0][0]
    
    # Mapping label
    label_map = {0.0: "Rendah ($0-$2)", 1.0: "Menengah ($2-$5)", 2.0: "Tinggi (>$5)"}
    
    # Tampilan Hasil
    col1, col2 = st.columns(2)
    col1.metric("Estimasi Nominal", f"${pred_nominal:.2f}")
    col2.metric("Kategori Tip", label_map.get(pred_class, "N/A"))
    
    st.success("Prediksi selesai diproses menggunakan pipeline Spark MLlib.")