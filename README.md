# 🛡️ Enterprise Fraud Detection Data Platform

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Kafka-Streaming-black?logo=apachekafka" />
  <img src="https://img.shields.io/badge/Apache%20Spark-Structured%20Streaming-orange?logo=apachespark" />
  <img src="https://img.shields.io/badge/Apache%20Hive-Data%20Warehouse-yellow" />
  <img src="https://img.shields.io/badge/Apache-HBase-red" />
  <img src="https://img.shields.io/badge/Apache-Airflow-017CEE?logo=apacheairflow" />
  <img src="https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker" />
  <img src="https://img.shields.io/badge/Python-3.x-blue?logo=python" />
  <img src="https://img.shields.io/badge/Status-Completed-success" />
</p>

## 📌 Project Overview

The **Enterprise Fraud Detection Data Platform** is a production-inspired end-to-end data engineering project designed to simulate how financial institutions monitor transactions and identify potentially fraudulent activities in real time.

The platform demonstrates the integration of modern big data technologies to ingest, process, analyze, and store banking transactions while supporting both operational and analytical use cases.

---

## 🎯 Project Objectives

- Build a scalable streaming architecture.
- Process transaction events in real time.
- Detect suspicious activities using fraud rules.
- Store data for historical analysis.
- Enable fast operational lookups.
- Orchestrate workflows using Airflow.
- Deploy the entire platform using Docker.

---

# 🏗️ Architecture

```text
                        ┌───────────────────┐
                        │ Transaction       │
                        │ Generator         │
                        └─────────┬─────────┘
                                  │
                                  ▼
                        ┌───────────────────┐
                        │ Kafka Producer    │
                        └─────────┬─────────┘
                                  │
                                  ▼
                        ┌───────────────────┐
                        │ Apache Kafka      │
                        └─────────┬─────────┘
                                  │
                                  ▼
                   ┌────────────────────────────┐
                   │ Spark Structured Streaming │
                   └───────┬───────────┬────────┘
                           │           │
                           │           │
                           ▼           ▼
                  ┌────────────┐ ┌────────────┐
                  │ Hive DW    │ │ HBase      │
                  │ Analytics  │ │ Real-Time  │
                  └─────┬──────┘ └─────┬──────┘
                        │              │
                        ▼              ▼
                  ┌────────────────────────┐
                  │ Fraud Investigation    │
                  │ Reporting & Insights   │
                  └────────────────────────┘
```

---

# 🚀 Technology Stack

| Layer | Technology |
|---------|------------|
| Data Generation | Python |
| Messaging | Apache Kafka |
| Serialization | Apache Avro |
| Stream Processing | Spark Structured Streaming |
| Data Warehouse | Apache Hive |
| NoSQL Storage | Apache HBase |
| Workflow Orchestration | Apache Airflow |
| Containerization | Docker & Docker Compose |
| Monitoring | Prometheus & Grafana *(Optional)* |

---

# 📂 Project Structure

```text
fraud-detection-platform/
│
├── docker/
│   └── docker-compose.yml
│
├── ingestion/
│   ├── producer/
│   │   └── transaction_producer.py
│   │
│   └── schema/
│       └── transaction.avsc
│
├── processing/
│   └── streaming/
│       └── Spark_consumer.py
│
├── warehouse/
│   └── hive/
│       ├── ddl.sql
│       └── dml.sql
│
├── hbase/
│   └── create_tables.hql
│
├── airflow/
│   └── dags/
│       └── fraud_pipeline_dag.py
│
├── monitoring/
│   ├── prometheus/
│   └── grafana/
│
├── scripts/
│
├── docs/
│   └── architecture.png
│
└── README.md
```

---

# ⚡ Key Features

## Real-Time Transaction Ingestion

- Simulates banking transactions.
- Produces events continuously.
- Publishes messages to Kafka topics.

---

## Schema Enforcement

Uses **Apache Avro** to guarantee:

- Schema consistency
- Backward compatibility
- Structured event definitions

---

## Stream Processing

Spark Structured Streaming performs:

- Kafka consumption
- Data transformation
- Fraud evaluation
- Multi-destination persistence

---

## Fraud Detection Rules

### 1. High Amount Detection

Transactions exceeding predefined thresholds are flagged.

Example:

```python
amount > 5000
```

---

### 2. Velocity Detection

Detects unusually frequent transactions.

Example:

```python
transactions_per_customer > threshold
```

---

### 3. Geographic Anomaly Detection

Identifies transactions occurring from unexpected locations.

Example:

```python
country != expected_country
```

---

### 4. Time-Based Anomalies

Detects transactions outside normal customer behavior patterns.

Example:

```python
hour(transaction_time) NOT BETWEEN 6 AND 23
```

---

# 🏦 Hive Data Warehouse

Hive is used for:

- Historical analytics
- Reporting
- Aggregations
- Fraud investigations

Example query:

```sql
SELECT merchant_category,
       COUNT(*) AS fraud_cases
FROM fraud_transactions
GROUP BY merchant_category
ORDER BY fraud_cases DESC;
```

---

# ⚡ HBase Operational Store

HBase supports:

- Customer transaction lookups
- Real-time investigation
- Serving layer workloads

Example Row Key:

```text
customer_id_timestamp
```

Example HBase query:

```ruby
scan 'transactions'
```

---

# 🔄 Airflow Orchestration

Airflow manages:

- Hive jobs
- Data quality checks
- Scheduled pipelines
- Operational workflows

Example DAG responsibilities:

- Validate incoming data.
- Trigger Hive transformations.
- Generate reports.
- Monitor pipeline execution.

---

# 🐳 Deployment

## Clone Repository

```bash
git clone https://github.com/omareldosoky1999-del/fraud.git

cd fraud
```

---

## Start Infrastructure

```bash
docker-compose up -d
```

---

## Verify Running Containers

```bash
docker ps
```

Expected services:

- Zookeeper
- Kafka
- Spark Master
- Spark Worker
- Hive Metastore
- Hive Server
- HBase
- Airflow Scheduler
- Airflow Webserver

---

# 📥 Start Kafka Producer

```bash
python transaction_producer.py
```

This continuously generates transaction events.

---

# ⚙️ Run Spark Streaming Job

```bash
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
Spark_consumer.py
```

---

# 🏦 Hive Usage

Connect:

```bash
beeline -u jdbc:hive2://localhost:10000
```

Commands:

```sql
CREATE DATABASE fraud_dw;

USE fraud_dw;

SHOW TABLES;

SELECT *
FROM fraud_transactions
LIMIT 10;
```

---

# ⚡ HBase Usage

Open shell:

```bash
hbase shell
```

Examples:

```ruby
list

describe 'transactions'

scan 'transactions'

count 'transactions'
```

---

# 📊 End-to-End Data Flow

```text
Transaction Generator
         │
         ▼
Kafka Producer
         │
         ▼
Apache Kafka
         │
         ▼
Spark Structured Streaming
         │
         ▼
Fraud Detection Engine
         │
 ┌───────┼──────────┐
 │       │          │
 ▼       ▼          ▼
Hive   HBase    Alert Layer
 │       │
 ▼       ▼
Analytics Investigations
```

---

# 📈 Example Use Cases

### Fraud Investigation

Identify customers with multiple suspicious transactions.

---

### Merchant Analysis

Determine merchants associated with high fraud volumes.

---

### Customer Risk Monitoring

Track unusual behavioral patterns.

---

### Historical Reporting

Generate fraud trend reports over time.

---

# 🎓 Skills Demonstrated

This project showcases experience with:

- Event-Driven Architectures
- Big Data Ecosystems
- Distributed Stream Processing
- Data Warehousing
- NoSQL Databases
- Workflow Orchestration
- Containerized Infrastructure
- Fraud Analytics Concepts

---

# 🔮 Future Enhancements

Potential improvements include:

- Machine Learning Fraud Models
- Feature Store Integration
- Real-Time Alert Notifications
- REST Scoring APIs
- Kubernetes Deployment
- CI/CD Pipelines
- Automated Testing
- Great Expectations Data Validation
- Role-Based Access Control

---

# 📚 Learning Outcomes

Through this project you can understand:

- How Kafka enables real-time ingestion.
- How Spark processes streaming events.
- How Hive supports analytical workloads.
- How HBase provides operational access.
- How Airflow orchestrates enterprise pipelines.
- How fraud detection systems are architected.

---

# 👨‍💻 Author

**Omar Mohamed**

Data Engineer | Big Data Enthusiast | Building Production-Inspired Data Platforms

GitHub:

https://github.com/omareldosoky1999-del

LinkedIn:

Add your LinkedIn profile here.

---

# ⭐ Support

If you found this project useful:

- Give it a ⭐ on GitHub.
- Fork the repository.
- Share feedback and suggestions.

---

# 📜 License

This project is intended for **educational and portfolio purposes**.

Feel free to modify, extend, and experiment with the architecture.

---

<p align="center">
  Built with ❤️ using Kafka, Spark, Hive, HBase, Airflow, and Docker.
</p>
