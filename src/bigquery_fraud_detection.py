"""
BigQuery Fraud Detection - Scalable Data Warehouse Querying
Demonstrates how to query massive Ethereum dataset efficiently
"""

from google.cloud import bigquery
import pandas as pd
from sklearn.ensemble import IsolationForest

def run_bigquery_analysis():
    """
    Query 1M real Ethereum transactions from BigQuery
    Demonstrates scalable data warehouse capabilities
    """
    
    print("🚀 BigQuery Fraud Detection - Real Ethereum Data")
    print("=" * 60)
    
    client = bigquery.Client()
    
    # Query massive Ethereum dataset
    print("\n📊 Querying BigQuery for Ethereum transactions...")
    print("   Dataset: bigquery-public-data.ethereum_blockchain.transactions")
    print("   Size: ~500GB+ available")
    
    query = """
    SELECT 
        from_address,
        to_address,
        CAST(value AS FLOAT64) / 1e18 as value_eth,
        CAST(gas_price AS FLOAT64) / 1e9 as gas_price_gwei,
        EXTRACT(HOUR FROM block_timestamp) as hour_of_day
    FROM `bigquery-public-data.ethereum_blockchain.transactions`
    WHERE block_number > 17000000
    LIMIT 100000
    """
    
    df = client.query(query).to_dataframe()
    
    print(f"✅ Retrieved {len(df):,} transactions")
    print(f"   Columns: {', '.join(df.columns)}")
    
    # Data cleaning
    print("\n🧹 Data Cleaning...")
    df = df.dropna()
    df = df[(df['value_eth'] >= 0) & (df['gas_price_gwei'] >= 0)]
    print(f"✅ Cleaned: {len(df):,} valid transactions")
    
    # Anomaly detection
    print("\n🤖 Running Isolation Forest...")
    X = df[['value_eth', 'gas_price_gwei', 'hour_of_day']].values
    
    model = IsolationForest(contamination=0.05, random_state=42, n_jobs=-1)
    df['anomaly_score'] = model.fit_predict(X)
    
    anomalies = (df['anomaly_score'] == -1).sum()
    
    print(f"✅ Detected {anomalies:,} anomalies ({anomalies/len(df)*100:.2f}%)")
    
    # Top suspicious addresses
    print("\n🎯 Top Suspicious Addresses:")
    suspicious = df[df['anomaly_score'] == -1].nlargest(10, 'value_eth')[
        ['from_address', 'to_address', 'value_eth', 'gas_price_gwei']
    ]
    
    for idx, row in suspicious.iterrows():
        print(f"   {row['from_address'][:10]}... → {row['to_address'][:10]}... | {row['value_eth']:.2f} ETH")
    
    print("\n" + "=" * 60)
    print("✅ BigQuery Analysis Complete")
    print("=" * 60)
    print("\nKey Benefits of BigQuery:")
    print("  • Query 500GB+ dataset in seconds")
    print("  • No infrastructure to manage")
    print("  • Cost-effective ($6.25 per TB scanned)")
    print("  • Automatic scaling")
    print("  • Integration with ML tools")

if __name__ == "__main__":
    run_bigquery_analysis()
