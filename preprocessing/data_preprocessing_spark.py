# data_preprocessing_spark.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import DoubleType, ArrayType
from pyspark.ml.feature import VectorAssembler, MinMaxScaler

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataPreprocessing") \
    .getOrCreate()

# Load CSV data
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

print("Raw Data:")
df.show(5)

# Handle missing values
df = df.fillna({
    "temperature": df.agg({"temperature": "avg"}).collect()[0][0],
    "humidity": df.agg({"humidity": "avg"}).collect()[0][0],
    "status": "OK"
})

# Remove duplicates
df = df.dropDuplicates()

# Feature engineering: temp/hum ratio
df = df.withColumn("temp_hum_ratio", col("temperature") / col("humidity"))

# Encode status column as numeric using when
df = df.withColumn(
    "status_code",
    when(col("status") == "OK", 0)
    .when(col("status") == "WARN", 1)
    .when(col("status") == "FAIL", 2)
)

# Assemble features into a vector for scaling
assembler = VectorAssembler(
    inputCols=["temperature", "humidity", "temp_hum_ratio"], 
    outputCol="features_vector"
)
df = assembler.transform(df)

# Scale features between 0 and 1
scaler = MinMaxScaler(inputCol="features_vector", outputCol="scaled_features")
scaler_model = scaler.fit(df)
df = scaler_model.transform(df)

# Convert vector to array for CSV compatibility
def vector_to_array(v):
    return v.toArray().tolist()

vector_to_array_udf = udf(vector_to_array, ArrayType(DoubleType()))
df = df.withColumn("scaled_features_array", vector_to_array_udf("scaled_features"))

# Select final columns
df_final = df.select(
    "sensor_id",
    "timestamp",
    "temperature",
    "humidity",
    "temp_hum_ratio",
    "status_code",
    "scaled_features_array"
)

print("Processed & Scaled Data:")
df_final.show(10, truncate=False)

# Save cleaned data to CSV
df_final.write.csv("sensor_data_cleaned_spark.csv", header=True, mode="overwrite")

spark.stop()
