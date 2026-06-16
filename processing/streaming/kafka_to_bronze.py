import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_SCHEMA_REGISTRY = os.getenv("KAFKA_SCHEMA_REGISTRY", "http://schema-registry:8081")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")

# Spark session
spark = SparkSession.builder \
    .appName("KafkaToBronze") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# Read from Kafka
transactions_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize Avro payload (using schema registry)
# For simplicity, assume value is JSON; Avro deserialization can be added with spark-avro
transactions_parsed = transactions_df.selectExpr("CAST(value AS STRING) as json_value")

# Write to Bronze (Parquet in MinIO)
query = transactions_parsed.writeStream \
    .format("parquet") \
    .option("path", "s3a://bronze/transactions/") \
    .option("checkpointLocation", "s3a://bronze/checkpoints/kafka_to_bronze/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
