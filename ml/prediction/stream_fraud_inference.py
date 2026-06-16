from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

spark = SparkSession.builder.appName("FraudMLInference").getOrCreate()

# Load trained model
model = PipelineModel.load("s3a://gold/models/fraud_lr_model")

# Stream Silver transactions
transactions_stream = spark.readStream.parquet("s3a://silver/transactions/")

# Apply model
predictions = model.transform(transactions_stream)

# Write predictions to Gold layer
predictions.writeStream \
    .format("parquet") \
    .option("path", "s3a://gold/fraud_predictions/") \
    .option("checkpointLocation", "s3a://gold/checkpoints/fraud_predictions/") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
