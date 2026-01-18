from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, avg
from pyspark.ml import PipelineModel
import json

spark = SparkSession.builder \
    .appName("StreamingFraudDetection") \
    .getOrCreate()

# Read streaming data from Kafka
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ethereum-transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON schema
schema = "transaction_hash STRING, sender STRING, receiver STRING, value DOUBLE, gas_price DOUBLE, timestamp LONG"

parsed_df = streaming_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*").withColumn("timestamp", col("timestamp").cast("timestamp"))

# Feature engineering for streaming
features_df = parsed_df \
    .groupBy(window("timestamp", "1 minute"), "sender") \
    .agg(
        count("*").alias("tx_count"),
        avg("value").alias("avg_value")
    )

# Load pre-trained model
model = PipelineModel.load("models/fraud_detection_model")

# Apply model to streaming data
def detect_anomalies(batch_df, batch_id):
    try:
        predictions = model.transform(batch_df)
        anomalies = predictions.filter(col("prediction") == 1)
        
        if anomalies.count() > 0:
            print(f"[ALERT] Batch {batch_id}: {anomalies.count()} anomalies detected!")
            anomalies.show()
            
            # Write to output topic
            anomalies.select("sender", "tx_count", "avg_value", "prediction") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("topic", "fraud-alerts") \
                .mode("append") \
                .save()
    except Exception as e:
        print(f"Error in batch {batch_id}: {str(e)}")

# Process streaming data with foreachBatch
query = features_df.writeStream \
    .foreachBatch(detect_anomalies) \
    .option("checkpointLocation", "/tmp/streaming_checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
