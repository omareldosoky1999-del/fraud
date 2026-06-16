from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'fraud_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['fraud-monitoring@bank.com'],
    'retries': 2,
}

with DAG(
    'fraud_detection_pipeline',
    default_args=default_args,
    description='Fraud Detection End-to-End Pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Phase 6: Ingestion (Kafka → Bronze)
    ingest_bronze = BashOperator(
        task_id='ingest_bronze',
        bash_command='spark-submit /app/processing/streaming/kafka_to_bronze.py'
    )

    # Phase 7: Validation (Bronze → Silver)
    validate_silver = BashOperator(
        task_id='validate_silver',
        bash_command='spark-submit /app/data_quality/validate_bronze_to_silver.py'
    )

    # Phase 8: Fraud Rules Engine
    fraud_rules = BashOperator(
        task_id='fraud_rules',
        bash_command='spark-submit /app/fraud_detection/rules/fraud_rules.py'
    )

    # Phase 9–10: ML Training + MLflow Logging
    train_ml_model = BashOperator(
        task_id='train_ml_model',
        bash_command='spark-submit /app/ml/training/train_with_mlflow.py'
    )

    # Phase 13: Gold Aggregation
    aggregate_gold = BashOperator(
        task_id='aggregate_gold',
        bash_command='spark-submit /app/processing/batch/write_gold.py'
    )

    # Phase 16: Refresh Power BI (via Trino)
    refresh_powerbi = BashOperator(
        task_id='refresh_powerbi',
        bash_command='python /app/analytics/refresh_powerbi.py'
    )

    # DAG dependencies
    ingest_bronze >> validate_silver >> fraud_rules >> train_ml_model >> aggregate_gold >> refresh_powerbi
