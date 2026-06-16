from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

spark = SparkSession.builder.appName("SilverLayerProcessing").getOrCreate()



# Read Bronze data
bronze_df = spark.readStream.parquet("s3a://bronze/transactions/")

# Deduplication
deduped_df = bronze_df.dropDuplicates(["trans_id", "trans_ref_no"])

# Business rule validation
validated_df = deduped_df \
    .filter((col("trans_amount") > 0) & (col("trans_amount") <= 100000)) \
    .filter((col("currency").isin("USD", "EGP", "EUR", "GBP"))) \
    .filter((year(col("Trans_date")) >= 2015) & (year(col("Trans_date")) <= 2025))

# Standardization (normalize casing, formats)
standardized_df = validated_df \
    .withColumn("currency", col("currency").cast("string")) \
    .withColumn("country_src", col("country_src")) \
    .withColumn("country_dest", col("country_dest"))

# Write to Silver layer
query_silver = standardized_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://silver/transactions/") \
    .option("checkpointLocation", "s3a://silver/checkpoints/transactions/") \
    .partitionBy("Trans_type", "Trans_status") \
    .outputMode("append") \
    .start()

query_silver.awaitTermination()
