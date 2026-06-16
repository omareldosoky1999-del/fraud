# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit, when, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import pandas as pd

# ==============================
# Spark Session
# ==============================
spark = SparkSession.builder \
    .appName("FraudDetectionStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================
# Transaction Schema
# ==============================
transaction_schema = StructType([
    StructField("Card_id", StringType(), True),
    StructField("Clt_id", StringType(), True),
    StructField("Country_Dest", StringType(), True),
    StructField("Country_Src", StringType(), True),
    StructField("Currency", StringType(), True),
    StructField("Dest_account_No", StringType(), True),
    StructField("Dev_Ip_Location", StringType(), True),
    StructField("Dev_id", StringType(), True),
    StructField("Trans_Reason", StringType(), True),
    StructField("Trans_Ref_No", StringType(), True),
    StructField("Trans_amount", DoubleType(), True),
    StructField("Trans_date", StringType(), True),
    StructField("Trans_destination", StringType(), True),
    StructField("Trans_id", StringType(), True),
    StructField("Trans_status", StringType(), True),
    StructField("Trans_type", StringType(), True)
])

# ==============================
# Read from Kafka
# ==============================
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON payload
transactions = raw_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), transaction_schema).alias("data")) \
    .select("data.*")

# ==============================
# Feature Engineering (Part 1)
# ==============================

# Convert Trans_date to timestamp
transactions = transactions.withColumn("Trans_timestamp", to_timestamp(col("Trans_date"), "yyyy-MM-dd HH:mm:ss"))

# is_weekend
transactions = transactions.withColumn("is_weekend",
    when(col("Trans_timestamp").isNotNull() & (col("Trans_timestamp").cast("date").isin(["Saturday","Sunday"])), lit(1)).otherwise(lit(0))
)

# is_holiday
holidays = ["2026-01-01", "2026-05-01", "2026-07-23"]
transactions = transactions.withColumn("is_holiday",
    when(col("Trans_timestamp").cast("date").isin(holidays), lit(1)).otherwise(lit(0))
)

# Deduplication (يجب أن تتم قبل العمليات المعتمدة على الحالة)
transactions = transactions.dropDuplicates(["Trans_id"])

# إجباري: الـ Watermarking لازم يتعمل قبل الـ State Management
transactions = transactions.withWatermark("Trans_timestamp", "10 minutes")


# ============================================================
# Stateful Processing (بديل الـ Window & Lag المسبب للخطأ)
# ============================================================

# 1. تحديد شكل الـ State (هنخزن فيها وقت آخر عملية للعميل)
state_schema = StructType([
    StructField("last_trans_timestamp", TimestampType(), True)
])

# 2. تحديد شكل المخرجات (نفس الحقول القديمة + الحقل الجديد)
output_schema = StructType(transactions.schema.fields + [
    StructField("time_since_last_trans", DoubleType(), True)
])

# 3. الدالة التي ستقوم بحساب الفارق الزمني لكل عميل بناءً على حالته السابقة
def calculate_time_since_last(key, pdf_group, state):
    clt_id = key[0]
    
    # ترتيب العمليات المتاحة حالياً للعميل زمنياً
    pdf_group = pdf_group.sort_values("Trans_timestamp")
    
    # استرجاع آخر وقت مسجل للعميل من الذاكرة (لو موجود)
    current_state = state.get if state.exists else (None,)
    last_time = current_state[0]
    
    time_diffs = []
    
    for index, row in pdf_group.iterrows():
        current_time = row["Trans_timestamp"]
        
        if last_time is not None and pd.notnull(last_time) and pd.notnull(current_time):
            # حساب الفارق بالثواني
            diff = (current_time - last_time).total_seconds()
        else:
            diff = 0.0  # أول عملية للعميل في النظام
            
        time_diffs.append(diff)
        last_time = current_time
    
    # تحديث الـ State بآخر وقت عملية وصلنا له عشان العملية القادمة
    state.update((last_time,))
    
    # إضافة العمود الجديد للـ Pandas DataFrame وإرجاعه
    pdf_group["time_since_last_trans"] = time_diffs
    return pdf_group

# 4. تطبيق الدالة السحرية
transactions = transactions \
    .groupBy(col("Clt_id")) \
    .applyInPandasWithState(
        func=calculate_time_since_last,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="Append",
        timeoutConf="NoTimeout"
    )

# ==============================
# Feature Engineering (Part 2)
# ==============================

# distance_from_last_loc (placeholder)
transactions = transactions.withColumn("distance_from_last_loc", lit(0.0))

# risk_score
transactions = transactions.withColumn("risk_score",
    when(col("Trans_amount") > 5000, lit(0.9))
    .when(col("is_weekend") == 1, lit(0.7))
    .otherwise(lit(0.2))
)

# new_current_amount
transactions = transactions.withColumn("new_current_amount", col("Trans_amount") * -1)

# is_fraud
transactions = transactions.withColumn("is_fraud",
    when(col("risk_score") > 0.8, lit(1)).otherwise(lit(0))
)

# ==============================
# Write to Console (for debug)
# ==============================
query = transactions.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()