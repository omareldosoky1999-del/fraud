import mlflow
import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("FraudMLTrainingWithMLflow").getOrCreate()

# Load Silver transactions
transactions = spark.read.parquet("s3a://silver/transactions/")

# Feature engineering
assembler = VectorAssembler(
    inputCols=["Trans_amount", "transactions_last_day", "average_transaction_amount"],
    outputCol="features"
)

lr = LogisticRegression(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[assembler, lr])

# Enable MLflow tracking
mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("FraudDetection")

with mlflow.start_run():
    model = pipeline.fit(transactions)
    
    # Log model
    mlflow.spark.log_model(model, "fraud_model")
    
    # Log parameters
    mlflow.log_param("algorithm", "LogisticRegression")
    
    # Log metrics (example: accuracy)
    predictions = model.transform(transactions)
    accuracy = predictions.filter(predictions.label == predictions.prediction).count() / predictions.count()
    mlflow.log_metric("accuracy", accuracy)
    
    # Register model
    mlflow.register_model("runs:/{run_id}/fraud_model", "FraudDetectionModel")
