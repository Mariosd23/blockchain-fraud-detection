"""
PySpark Fraud Detection - Distributed Processing
Demonstrates how to scale anomaly detection across multiple nodes
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import IsolationForest
import pandas as pd

def run_pyspark_analysis():
    """
    Demonstrates PySpark distributed processing for fraud detection
    This shows how the analysis scales from single machine to distributed cluster
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
    
    # Feature engineering
    print("\n🔧 Feature Engineering...")
    assembler = VectorAssembler(
        inputCols=["value_eth", "gas", "gas_price_gwei"],
        outputCol="features"
    )
    df_features = assembler.transform(df)
    
    # Anomaly detection
    print("\n🤖 Running Isolation Forest (distributed)...")
    iso_forest = IsolationForest(
        contaminationRate=0.05,
        randomSeed=42,
        numTrees=100
    )
    
    model = iso_forest.fit(df_features)
    predictions = model.transform(df_features)
    
    # Count anomalies
    anomalies = predictions.filter(predictions.anomaly == 1).count()
    total = predictions.count()
    
    print(f"✅ Detected {anomalies:,} anomalies out of {total:,} transactions")
    print(f"   Anomaly rate: {anomalies/total*100:.2f}%")
    
    # Show results
    print("\n📋 Sample Results:")
    predictions.select("tx_id", "value_eth", "gas_price_gwei", "anomaly") \
        .filter(predictions.anomaly == 1) \
        .show(10)
    
    print("\n" + "=" * 60)
    print("✅ PySpark Analysis Complete")
    print("=" * 60)
    print("\nKey Benefits of PySpark:")
    print("  • Distributed processing across multiple nodes")
    print("  • Handles millions/billions of transactions efficiently")
    print("  • Fault-tolerant (can restart failed tasks)")
    print("  • Integrates with BigQuery, HDFS, Cloud Storage")
    print("  • Scales from laptop to enterprise clusters")

if __name__ == "__main__":
    run_pyspark_analysis()
