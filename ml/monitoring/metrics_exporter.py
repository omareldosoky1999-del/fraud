from prometheus_client import start_http_server, Gauge
import time

# Metrics
fraud_rate = Gauge('fraud_rate', 'Fraud rate in production')
precision = Gauge('fraud_precision', 'Precision of fraud model')
recall = Gauge('fraud_recall', 'Recall of fraud model')
f1_score = Gauge('fraud_f1', 'F1 score of fraud model')
drift_score = Gauge('feature_drift_score', 'Drift score of transaction features')

def monitor_loop():
    while True:
        # Example values (replace with actual Spark/MLflow metrics)
        fraud_rate.set(0.04)
        precision.set(0.85)
        recall.set(0.78)
        f1_score.set(0.81)
        drift_score.set(0.12)  # 0 = no drift, 1 = severe drift
        time.sleep(60)

if __name__ == "__main__":
    start_http_server(9090)
    monitor_loop()
