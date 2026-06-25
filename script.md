# Terminal 1 — dari folder root project
cd "/Users/adrian/Documents/IT/BIG DATA AKHIR"
source venv/bin/activate
cd stream && python data_generator.py

# Terminal 2
cd "/Users/adrian/Documents/IT/BIG DATA AKHIR"
source venv/bin/activate
cd stream && python kappa_stream_ingest.py

# Terminal 3
cd "/Users/adrian/Documents/IT/BIG DATA AKHIR"
source venv/bin/activate
cd stream && python realtime_predictor.py
