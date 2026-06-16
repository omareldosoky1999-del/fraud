#!/bin/bash

# Create main transactions topic
docker exec -it kafka kafka-topics \
  --create \
  --topic transactions \
  --bootstrap-server kafka:9092 \
  --partitions 6 \
  --replication-factor 1

# Create Dead Letter Queue topic
docker exec -it kafka kafka-topics \
  --create \
  --topic transactions-dlq \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1
