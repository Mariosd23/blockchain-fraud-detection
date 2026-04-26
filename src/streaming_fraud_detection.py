"""
Real-time Streaming Fraud Detection using PySpark Structured Streaming + Kafka
Monitors Ethereum transactions in real-time and flags anomalies.

Prerequisites:
    - Kafka running on localhost:9092
    - Topic 'ethereum-transactions' with JSON messages
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, avg, stddev, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from sklearn.ensemble import IsolationForest
import numpy as np


def create_streaming_pipeline():
    """Set up the streaming fraud detection pipeline."""
    
    spark = SparkSession.builder \
        .appName("StreamingFraudDetection") \
        .getOrCreate()
    
    # Define the expected JSON schema for incoming transactions
    tx_schema = StructType([
        StructField("transaction_hash", StringType(), True),
        StructField("sender", StringType(), True),
        StructField("receiver", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("gas_price", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ])
    
    # Read streaming data from Kafka
    streaming_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ethereum-transactions") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON messages
    parsed_df = streaming_df.select(
        from_json(col("value").cast("string"), tx_schema).alias("data")
    ).select("data.*").withColumn("timestamp", col("timestamp").cast("timestamp"))
    
    # Feature engineering for streaming: aggregate per sender per minute
    features_df = parsed_df \
        .groupBy(window("timestamp", "1 minute"), "sender") \
        .agg(
            count("*").alias("tx_count"),
            avg("value").alias("avg_value"),
            avg("gas_price").alias("avg_gas_price"),
        )
    
    # Pre-train a simple model on historical thresholds
    # In production, load a pre-trained model from disk
    print("🚀 Streaming Fraud Detection Pipeline Starting...")
    print("   Kafka topic: ethereum-transactions")
    print("   Window: 1 minute")
    print("   Features: tx_count, avg_value, avg_gas_price per sender")
    
    # Apply rule-based anomaly detection on each micro-batch
    def detect_anomalies(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        
        try:
            pdf = batch_df.toPandas()
            
            if len(pdf) < 5:
                # Not enough data for IsolationForest, use simple thresholds
                pdf["is_anomaly"] = (
                    (pdf["tx_count"] > 10) |
                    (pdf["avg_value"] > pdf["avg_value"].quantile(0.95))
                ).astype(int)
            else:
                # Use IsolationForest for anomaly detection
                X = pdf[["tx_count", "avg_value", "avg_gas_price"]].fillna(0).values
                iso = IsolationForest(contamination=0.1, random_state=42)
                pdf["is_anomaly"] = (iso.fit_predict(X) == -1).astype(int)
            
            anomalies = pdf[pdf["is_anomaly"] == 1]
            
            if len(anomalies) > 0:
                print(f"\n[ALERT] Batch {batch_id}: {len(anomalies)} anomalies detected!")
                for _, row in anomalies.iterrows():
                    print(f"   Sender: {row['sender'][:10]}... | "
                          f"Txs: {row['tx_count']} | "
                          f"Avg Value: {row['avg_value']:.2f} ETH")
        except Exception as e:
            print(f"Error in batch {batch_id}: {str(e)}")
    
    # Process streaming data with foreachBatch
    query = features_df.writeStream \
        .foreachBatch(detect_anomalies) \
        .option("checkpointLocation", "/tmp/streaming_checkpoint") \
        .outputMode("update") \
        .start()
    
    print("✅ Pipeline running. Waiting for transactions...")
    print("   Press Ctrl+C to stop.\n")
    
    query.awaitTermination()


if __name__ == "__main__":
    create_streaming_pipeline()
