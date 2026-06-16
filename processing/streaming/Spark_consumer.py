# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, regexp_replace, to_timestamp, when, current_timestamp, expr, lit, rand
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import round
# import happybase



# ==============================
# 1. Start Spark Session
# ==============================
spark = SparkSession.builder \
    .appName("Financial_Transactions_Streaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-avro_2.12:3.0.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================
# 2. Load Avro Schema (from .avsc file)
# ==============================
SCHEMA_PATH = r"/app/ingestion/schema/transaction.avsc"
if not os.path.exists(SCHEMA_PATH):
    SCHEMA_PATH = "/app/ingestion/schema/transaction.avsc"

with open(SCHEMA_PATH, "r") as f:
    schema_json = f.read()

# ==============================
# 3. Read Stream from Kafka
# ==============================
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .option("kafka.client.id", "spark-consumer-client") \
    .option("kafka.request.timeout.ms", "60000") \
    .option("failOnDataLoss", "false") \
    .load()

# ==============================
# 4. Deserialize Avro
# ==============================
streaming_df = raw_stream \
    .select(
        expr("substring(value, 6, length(value))").alias("avro_value"),
        col("timestamp").alias("kafka_timestamp")
    ) \
    .select(
        from_avro(col("avro_value"), schema_json).alias("data"),
        col("kafka_timestamp")
    ) \
    .select("data.*", "kafka_timestamp")


# ==============================
# 5. Enrichment & Advanced Analytics (الأعمدة الناقصة)
# ==============================
# هنا بنضيف الحسابات الذكية اللي الـ Hive مستنيها بناءً على الـ Logic بتاعك
enriched_df = streaming_df \
    .withColumn(
        "time_since_last_trans",
        round(rand() * 120, 2)  # محاكاة لحساب فرق الوقت بالدقائق مثلاً
    ) \
    .withColumn(
        "distance_from_last_loc",
        round(rand() * 500, 2)  # محاكاة لحساب المسافة الجغرافية بالكيلومتر
    ) \
    .withColumn(
        "junk_sk",
        floor(rand() * 7 + 1).cast("int")  # محاكاة لتصنيف المعاملات (عادي / فاشل / احتيال)
    ) \
    .withColumn(
        "risk_score",
        # ريسك عالي للداتا الضخمة
        when(col("Trans_amount") > 5000, round(rand() * 30 + 70, 2))
        .otherwise(round(rand() * 40, 2))
    ) \
    .withColumn(
        "new_current_amount",
        col("Trans_amount") * 1.5  # زيادة افتراضية أو تعديل الرصيد
    ) \
    .withColumn(
        "country_src", col("Country_Src")  # تظبيط الـ Case لتطابق الـ Hive
    ) \
    .withColumn(
        "country_dest", col("Country_Dest")
    ) \
    .withColumn(
        "dest_account_no", col("Dest_account_no")
    )

# ==============================
# 6. Mapping to Hive Fact Table Schema
# ==============================
# بنعمل Fake/Mock للـ Surrogate Keys عشان الداتا تنزل بترتيب أعمدة الـ Hive بالظبط
# في الحقيقة الـ SKs دي بتيجي من Join مع جداول الـ Dimensions
final_fact_df = enriched_df.select(
    col("Trans_id").alias("trans_sk"),                # Mock Surrogate Key
    col("Clt_id").alias("clt_sk"),                # Mock Client FK
    col("Card_id").alias("card_sk"),               # Mock Card FK
    col("Dev_id").alias("device_sk"),             # Mock Device FK
    to_timestamp(col("Trans_date"), "yyyy-MM-dd hh:mm:ss a").alias("event_timestamp"),            # Mock Time FK (HHMMSS)
    # Mock Junk FK (Normal / Failed / Fraud)
    col("junk_sk"),
    col("Trans_ref_no").alias("trans_ref_no"),
    col("Trans_destination").alias("trans_destination"),
    col("Dev_ip_location").alias("dev_ip_location"),
    col("country_src"),
    col("country_dest"),
    col("Trans_reason").alias("trans_reason"),
    col("currency"),
    col("dest_account_no"),
    col("Trans_amount").alias("trans_amount"),
    col("time_since_last_trans"),
    col("distance_from_last_loc"),
    col("risk_score"),
    col("new_current_amount"),
    # عمود الـ Partition الأول (لازم يكون في الآخر في الـ Select)
    col("Trans_type").alias("trans_type"),
    # عمود الـ Partition الثاني (لازم يكون في الآخر في الـ Select)
    col("Trans_status").alias("trans_status")
)

# ==============================
# 7. Filters (لو محتاجهم للـ Console)
# ==============================
failed_tx = final_fact_df.filter(col("Trans_status") == "Failed")


# def save_partition(iterator):

#     connection = happybase.Connection(
#         host="hbase",
#         port=9090
#     )

#     connection.open()

#     table = connection.table("transactions")

#     for row in iterator:

#         try:

#             print(row)

#             table.put(
#                 str(row.trans_sk).encode(),
#                 {
#                     b'info:client': str(row.clt_sk).encode(),
#                     b'info:card': str(row.card_sk).encode(),
#                     b'info:device': str(row.device_sk).encode(),
#                     b'info:amount': str(row.trans_amount).encode(),
#                     b'info:risk_score': str(row.risk_score).encode(),
#                     b'info:status': str(row.trans_status).encode(),
#                     b'info:type': str(row.trans_type).encode(),
#                     b'info:country_src': str(row.country_src).encode(),
#                     b'info:country_dest': str(row.country_dest).encode(),
#                 }
#             )

#         except Exception as e:

#             print("======================")
#             print(row)
#             print(e)
#             print("======================")

#     connection.close()

# def write_to_hbase(df, epoch_id):

#     df.foreachPartition(save_partition)

# ==============================
# 8. Write to HDFS (Parquet) - المتوافق مع Hive
# ==============================
# هنا بنرمي الـ Fact كامل بالـ Partitions الصح في مسار الـ Hive
query_parquet = final_fact_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path",              "hdfs://namenode:8020/user/project/data_warehouse/transactions/") \
    .option("checkpointLocation", "hdfs://namenode:8020/user/project/checkpoints/transactions/") \
    .partitionBy("Trans_type", "Trans_status") \
    .trigger(processingTime="30 seconds") \
    .queryName("Write_To_HDFS") \
    .start()

# ==============================
# 9. Write to Console (للمتابعة فقط)
# ==============================
query_all = final_fact_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .trigger(processingTime="5 seconds") \
    .start()

query_export = final_fact_df.coalesce(1).writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/app/output/transactions_csv/") \
    .option("checkpointLocation", "/app/checkpoints/export_csv/") \
    .option("header", True) \
    .trigger(processingTime="30 seconds") \
    .start()
# query_hbase = final_fact_df.writeStream \
#     .foreachBatch(write_to_hbase) \
#     .outputMode("append") \
#     .trigger(processingTime="30 seconds") \
#     .queryName("Write_To_HBase") \
#     .start()

print("✅ Spark Streaming started — Pipelines are feeding HDFS for Hive integration!")
spark.streams.awaitAnyTermination()
