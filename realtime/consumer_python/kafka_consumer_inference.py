# kafka_consumer_inference.py
import json
import time
from collections import deque, defaultdict
from kafka import KafkaConsumer
import numpy as np
import joblib
import os

MODEL_PATH = os.path.join("realtime", "model", "model.joblib")
KAFKA_BOOTSTRAP = ["localhost:9092"]
TOPIC = "sensor_data"

# parameters
WINDOW_SIZE = 6  # number of recent messages to average per sensor
PRINT_INTERVAL = 1.0  # seconds

def rolling_avg(deq):
    if not deq:
        return 0.0
    return sum(deq) / len(deq)

def main():
    print("Loading model:", MODEL_PATH)
    model = joblib.load(MODEL_PATH)
    print("Model loaded.")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="consumer_test_group_1",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    temps = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
    hums = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
    last_print = time.time()

    print("Consumer started, waiting for messages...")
    for msg in consumer:
        rec = msg.value
        sensor = rec.get("sensor_id", "unknown")
        temp = float(rec.get("temperature", 0))
        hum = float(rec.get("humidity", 0))
        timestamp = rec.get("timestamp", time.time())

        temps[sensor].append(temp)
        hums[sensor].append(hum)

        avg_temp = rolling_avg(temps[sensor])
        avg_hum = rolling_avg(hums[sensor])

        # prediction using model
        X = np.array([[avg_temp, avg_hum]])
        try:
            pred = int(model.predict(X)[0])
        except Exception as e:
            pred = None
            print("Prediction error:", e)

        # Print result
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] sensor={sensor} temp={temp:.2f} hum={hum:.2f} | avg_temp={avg_temp:.2f} avg_hum={avg_hum:.2f} => pred={pred}")

        # simple rate limiting of prints
        if time.time() - last_print > PRINT_INTERVAL:
            last_print = time.time()
if __name__ == "__main__":
    main()
