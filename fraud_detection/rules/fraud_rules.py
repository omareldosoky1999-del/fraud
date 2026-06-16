from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, window, lit

spark = SparkSession.builder.appName("FraudRulesEngine").getOrCreate()

# Load Silver transactions
transactions = spark.read.parquet("s3a://silver/transactions/")

# Rule 1: High Transaction Amount
high_amount = transactions.filter(col("Trans_amount") > 10000).withColumn("fraud_reason", lit("High Amount"))

# Rule 2: Velocity Check (too many transactions in short time)
velocity = transactions.groupBy("Clt_id", window(col("Trans_date"), "1 hour")) \
    .count().filter(col("count") > 10).withColumn("fraud_reason", lit("Velocity Check"))

# Rule 3: Country Mismatch (source vs destination mismatch)
country_mismatch = transactions.filter(col("Country_Src") != col("Country_Dest")) \
    .withColumn("fraud_reason", lit("Country Mismatch"))

# Rule 4: Impossible Travel (same client, different countries within 1 hour)
impossible_travel = transactions.groupBy("Clt_id", "Country_Src", window(col("Trans_date"), "1 hour")) \
    .count().filter(col("count") > 1).withColumn("fraud_reason", lit("Impossible Travel"))

# Rule 5: Blacklisted Merchants
blacklisted_merchants = transactions.filter(col("Trans_destination").isin(["BlacklistedMerchant1", "BlacklistedMerchant2"])) \
    .withColumn("fraud_reason", lit("Blacklisted Merchant"))

# Rule 6: Abnormal Frequency (too many transactions per day)
abnormal_frequency = transactions.groupBy("Clt_id", window(col("Trans_date"), "1 day")) \
    .count().filter(col("count") > 50).withColumn("fraud_reason", lit("Abnormal Frequency"))

# Union all fraud alerts
fraud_alerts = high_amount.union(velocity).union(country_mismatch).union(impossible_travel).union(blacklisted_merchants).union(abnormal_frequency)

# Write alerts to Gold layer (analytics)
fraud_alerts.write.mode("append").parquet("s3a://gold/fraud_alerts/")

# Write alerts to HBase (operational serving)
fraud_alerts.write \
    .format("org.apache.hadoop.hbase.spark") \
    .option("hbase.table", "fraud_alerts") \
    .option("hbase.zookeeper.quorum", "hbase-zookeeper") \
    .save()
