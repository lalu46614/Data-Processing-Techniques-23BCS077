# Data Processing Techniques Project

This project demonstrates multiple data processing techniques including:

1. **Preprocessing Data with Spark**  
   - Handling missing values, duplicates, feature engineering, and scaling.

2. **Incremental Data Processing (CDC)**  
   - Change Data Capture with Python and Pandas.

3. **In-Memory Data Processing**  
   - Spark-based in-memory computation demo.

4. **Real-Time Data Processing**  
   - Kafka producer and consumer for live data streaming and inference.

## Folder Structure

- `preprocessing/` – Spark data preprocessing scripts and sample data  
- `cdc_incremental/` – Incremental CDC-based processing scripts  
- `in_memory/` – In-memory processing examples using Spark  
- `realtime/` – Kafka-based real-time data streaming and ML inference  
- `docker/` – Docker setup for Kafka and other services  
- `requirements.txt` – Python dependencies  

## How to Run

1. Install dependencies:
pip install -r requirements.txt

2.Run preprocessing scripts:
python preprocessing/data_preprocessing_spark.py

3.Run incremental CDC processing:
python cdc_incremental/cdc_consumer_incremental.py

4.Run in-memory Spark demo:
python in_memory/spark_in_memory_demo.py

5.Run Kafka producer and consumer for real-time processing:
python realtime/producer/kafka_producer.py
python realtime/consumer_python/kafka_consumer_inference.py

python realtime/producer/kafka_producer.py
python realtime/consumer_python/kafka_consumer_inference.py
