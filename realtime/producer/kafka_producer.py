# kafka_producer.py
import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

topic = 'sensor_data'

def simulate_record():
    return {
        "sensor_id": f"sensor_{random.randint(1,5)}",
        "timestamp": int(time.time()),
        "temperature": round(random.uniform(20, 40), 2),
        "humidity": round(random.uniform(30, 80), 2),
        "status": random.choice(["OK","WARN","FAIL"])
    }

if __name__ == "__main__":
    while True:
        rec = simulate_record()
        producer.send(topic, value=rec)
        print("sent", rec)
        time.sleep(0.5)  # 2 msgs/sec
