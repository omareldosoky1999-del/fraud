from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("FraudMLTraining").getOrCreate()

# Load Silver transactions
transactions = spark.read.parquet("s3a://silver/transactions/")

# Feature engineering
features = transactions.groupBy("Clt_id").agg(
    count("*").alias("transactions_last_day"),
    avg("Trans_amount").alias("average_transaction_amount")
)

dataset = transactions.join(features, "Clt_id")

# Label: fraud if flagged by rules (Phase 8)
dataset = dataset.withColumn("label", col("fraud_flag").cast("int"))

# Assemble features
assembler = VectorAssembler(
    inputCols=["Trans_amount", "transactions_last_day", "average_transaction_amount"],
    outputCol="features"
)

# Logistic Regression model
lr = LogisticRegression(featuresCol="features", labelCol="label")

pipeline = Pipeline(stages=[assembler, lr])

# Train model
model = pipeline.fit(dataset)

# Save model locally (MLflow integration in Phase 10)
model.write().overwrite().save("s3a://gold/models/fraud_lr_model")
