from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, current_timestamp, expr, lit, rand, round
from pyspark.sql.types import *

spark = SparkSession.builder.appName("BronzeLayerWithEnrichment").getOrCreate()


hadoop_conf = spark._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)
# Read raw transactions from Kafka (already deserialized Avro)
transactions_stream = spark.read.parquet("s3a://bronze/transactions/")

schema = StructType([
    StructField("Card_id", IntegerType()),
    StructField("Clt_id", IntegerType()),
    StructField("Country_Dest", StringType()),
    StructField("Country_Src", StringType()),
    StructField("Currency", StringType()),
    StructField("Dest_account_No", StringType()),
    StructField("Dev_Ip_Location", StringType()),
    StructField("Dev_id", IntegerType()),
    StructField("Trans_Reason", StringType()),
    StructField("Trans_Ref_No", StringType()),
    StructField("Trans_amount", IntegerType()),
    StructField("Trans_date", StringType()),
    StructField("Trans_destination", StringType()),
    StructField("Trans_id", IntegerType()),
    StructField("Trans_status", StringType()),
    StructField("Trans_type", StringType())
])

df = spark.readStream.schema(schema).parquet("s3a://bronze/transactions/")
transactions_stream = df
# Enrichment columns
enriched_df = transactions_stream \
    .withColumn("time_since_last_trans", round(rand() * 120, 2)) \
    .withColumn("distance_from_last_loc", round(rand() * 500, 2)) \
    .withColumn("risk_score",
        when(col("Trans_amount") > 50000, round(rand() * 30 + 70, 2))
        .otherwise(round(rand() * 40, 2))
    ) \
    .withColumn("new_current_amount", col("Trans_amount") * 1.05) \
    .withColumn("country_src", col("Country_Src")) \
    .withColumn("country_dest", col("Country_Dest")) \
    .withColumn("dest_account_no", col("Dest_account_no"))

# Surrogate keys (mocked for Hive compatibility)
final_fact_df = enriched_df.select(
    lit(1).alias("trans_sk"),
    lit(1001).alias("clt_sk"),
    lit(20001).alias("card_sk"),
    lit(300001).alias("device_sk"),
    lit(20260522).alias("date_sk"),   # YYYYMMDD mock
    lit(115000).alias("time_sk"),     # HHMMSS mock
    lit(1).alias("junk_sk"),
    col("Trans_id").alias("trans_id"),
    col("Trans_ref_no").alias("trans_ref_no"),
    col("Trans_destination").alias("trans_destination"),
    col("Dev_ip_location").alias("dev_ip_location"),
    col("country_src"),
    col("country_dest"),
    col("Trans_reason").alias("trans_reason"),
    col("Currency").alias("currency"),
    col("dest_account_no"),
    col("Trans_amount").alias("trans_amount"),
    col("time_since_last_trans"),
    col("distance_from_last_loc"),
    col("risk_score"),
    col("new_current_amount"),
    col("Trans_type"),
    col("Trans_status")
)

# Write enriched Bronze data to MinIO (partitioned by year/month/day)
query_bronze = final_fact_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://bronze/transactions/") \
    .option("checkpointLocation", "s3a://bronze/checkpoints/write_bronze/") \
    .partitionBy("Trans_type", "Trans_status") \
    .trigger(processingTime="30 seconds") \
    .queryName("Write_Bronze_Enriched") \
    .start()

query_bronze.awaitTermination()
