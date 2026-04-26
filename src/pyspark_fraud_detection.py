"""
PySpark Fraud Detection - Distributed Processing
Demonstrates how to scale anomaly detection across multiple nodes
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import col, when, lit
from sklearn.ensemble import IsolationForest
import pandas as pd
import numpy as np

def run_pyspark_analysis():
    """
    Demonstrates PySpark distributed processing for fraud detection.
    Uses PySpark for data processing and feature engineering,
    then scikit-learn IsolationForest for anomaly detection.
    """
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("BlockchainFraudDetection") \
        .getOrCreate()
    
    print("🚀 PySpark Fraud Detection - Distributed Processing")
    print("=" * 60)
    
    # In production, this would read from:
    # - BigQuery: spark.read.format("bigquery").load("project.dataset.table")
    # - HDFS: spark.read.csv("hdfs://path/to/data")
    # - Cloud Storage: spark.read.parquet("gs://bucket/path")
    
    # For demo, create sample data
    print("\n📊 Creating sample Ethereum transaction data...")
    
    data = [
        (1, 0.5, 21000, 50),      # Normal transaction
        (2, 1.2, 21000, 45),      # Normal
        (3, 500, 21000, 1200),    # Anomaly - high value, high gas price
        (4, 0.8, 21000, 48),      # Normal
        (5, 10000, 21000, 1500),  # Anomaly - very high value
    ] * 200  # 1000 transactions
    
    columns = ["tx_id", "value_eth", "gas", "gas_price_gwei"]
    df = spark.createDataFrame(data, columns)
    
    print(f"✅ Created {df.count():,} sample transactions")
    
    # Feature engineering with PySpark
    print("\n🔧 Feature Engineering...")
    assembler = VectorAssembler(
        inputCols=["value_eth", "gas", "gas_price_gwei"],
        outputCol="features"
    )
    df_features = assembler.transform(df)
    
    # Anomaly detection using scikit-learn (collect features to driver)
    print("\n🤖 Running Isolation Forest...")
    pdf = df.select("tx_id", "value_eth", "gas", "gas_price_gwei").toPandas()
    X = pdf[["value_eth", "gas", "gas_price_gwei"]].values
    
    iso_forest = IsolationForest(
        contamination=0.05,
        random_state=42,
        n_estimators=100,
        n_jobs=-1
    )
    pdf["anomaly"] = iso_forest.fit_predict(X)
    pdf["anomaly"] = pdf["anomaly"].map({1: 0, -1: 1})  # 1 = anomaly
    
    # Count anomalies
    anomalies = pdf["anomaly"].sum()
    total = len(pdf)
    
    print(f"✅ Detected {anomalies:,} anomalies out of {total:,} transactions")
    print(f"   Anomaly rate: {anomalies/total*100:.2f}%")
    
    # Show results
    print("\n📋 Sample Anomalous Transactions:")
    anomalous = pdf[pdf["anomaly"] == 1].head(10)
    print(anomalous[["tx_id", "value_eth", "gas_price_gwei", "anomaly"]].to_string(index=False))
    
    print("\n" + "=" * 60)
    print("✅ PySpark Analysis Complete")
    print("=" * 60)
    print("\nKey Benefits of PySpark:")
    print("  • Distributed processing across multiple nodes")
    print("  • Handles millions/billions of transactions efficiently")
    print("  • Fault-tolerant (can restart failed tasks)")
    print("  • Integrates with BigQuery, HDFS, Cloud Storage")
    print("  • Scales from laptop to enterprise clusters")
    
    spark.stop()

if __name__ == "__main__":
    run_pyspark_analysis()
