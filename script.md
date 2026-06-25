# 1. Test prediksi cepat
cd ml && python predict.py

# 2. Evaluasi lengkap (Cohen's Kappa, Confusion Matrix)
cd ml && python evaluate_model.py

# 3. Dashboard interaktif
cd dashboard && streamlit run app.py

# 4. Real-time streaming (butuh Kafka aktif — sudah running via Docker)
cd stream && python data_generator.py          # Terminal 1
cd stream && python kappa_stream_ingest.py     # Terminal 2
cd stream && python realtime_predictor.py      # Terminal 3

# 5. Atau otomatis semua (dari root)
bash run_all.sh
