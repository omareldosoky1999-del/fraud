from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, when, year, month, dayofmonth

spark = SparkSession.builder.appName("GoldLayerProcessing").getOrCreate()

# Load Silver transactions
silver_df = spark.read.parquet("s3a://silver/transactions/")

# Fraud KPIs
fraud_kpis = silver_df.groupBy(
    year(col("Trans_date")).alias("year"),
    month(col("Trans_date")).alias("month"),
    dayofmonth(col("Trans_date")).alias("day")
).agg(
    count("*").alias("total_transactions"),
    sum(when(col("fraud_flag") == 1, 1).otherwise(0)).alias("fraud_transactions"),
    avg("trans_amount").alias("avg_transaction_amount"),
    sum("trans_amount").alias("total_transaction_amount")
).withColumn(
    "fraud_rate", col("fraud_transactions") / col("total_transactions")
)

# Write KPIs to Gold layer
fraud_kpis.write.mode("append").parquet("s3a://gold/fraud_kpis/")

# Fraud Alerts (from rules + ML predictions)
fraud_alerts = silver_df.filter(col("fraud_flag") == 1)
fraud_alerts.write.mode("append").parquet("s3a://gold/fraud_alerts/")

# Customer Risk Profiles
customer_risk = silver_df.groupBy("clt_id").agg(
    count("*").alias("transactions_count"),
    avg("trans_amount").alias("avg_amount"),
    sum(when(col("fraud_flag") == 1, 1).otherwise(0)).alias("fraud_count")
).withColumn(
    "customer_risk_score", col("fraud_count") / col("transactions_count")
)

customer_risk.write.mode("append").parquet("s3a://gold/customer_risk/")
