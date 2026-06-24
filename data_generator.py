import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("--- Simulasi Real-time Taksi NYC Dimulai ---")
print("--- Aliran data santai (1-3 detik per data) ---")

def generate_taxi_trip():
    # Membuat data acak yang realistis
    fare = round(random.uniform(5.0, 100.0), 2)
    tip = round(fare * random.uniform(0.10, 0.25), 2)
    
    return {
        "tipAmount": tip,
        "fareAmount": fare,
        "tripDistance": round(random.uniform(0.5, 30.0), 2),
        "passengerCount": random.randint(1, 6),
        "pickup_zone": float(random.randint(1, 263)),
        "dropoff_zone": float(random.randint(1, 263)),
        "pickup_hour": datetime.now().hour,
        "pickup_day": datetime.now().isoweekday()
    }

try:
    while True:
        data = generate_taxi_trip()
        producer.send('green-taxi-stream', value=data)
        
        # Logika 'Slow and Steady'
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Taksi baru terdeteksi: Fare=${data['fareAmount']}, Dist={data['tripDistance']}mi")
        
        # Jeda santai 1 hingga 3 detik
        time.sleep(random.uniform(1.0, 3.0))
except KeyboardInterrupt:
    print("\nSimulasi dihentikan oleh user.")
finally:
    producer.close()