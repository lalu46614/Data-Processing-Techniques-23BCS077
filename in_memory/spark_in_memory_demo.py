# in_memory_processing.py
import pandas as pd
import numpy as np

# Sample data (replace with actual CSV if needed)
data = {
    "sensor_id": ["sensor_1", "sensor_2", "sensor_3", "sensor_4", "sensor_5"],
    "timestamp": [1760627011, 1760627012, 1760627012, 1760627013, 1760627013],
    "temperature": [39.45, 30.96, 27.97, 33.23, 38.17],
    "humidity": [44.73, 55.41, 72.4, 73.12, 45.46],
    "status": ["WARN", "FAIL", "OK", "FAIL", "OK"]
}

# Load data into memory
df = pd.DataFrame(data)
print("Raw Data:")
print(df)

# Feature engineering: temperature/humidity ratio
df["temp_hum_ratio"] = df["temperature"] / df["humidity"]

# Encode status as numeric
status_mapping = {"OK": 0, "WARN": 1, "FAIL": 2}
df["status_code"] = df["status"].map(status_mapping)

# In-memory aggregation: average temperature & humidity
agg_df = df.groupby("status_code").agg({
    "temperature": "mean",
    "humidity": "mean",
    "temp_hum_ratio": "mean"
}).reset_index()

print("\nAggregated Data (in-memory):")
print(agg_df)

# Optional: simple clustering in-memory
from sklearn.cluster import KMeans

features = df[["temperature", "humidity", "temp_hum_ratio"]]
kmeans = KMeans(n_clusters=2, random_state=42)
df["cluster"] = kmeans.fit_predict(features)

print("\nData with Clusters (in-memory):")
print(df)
