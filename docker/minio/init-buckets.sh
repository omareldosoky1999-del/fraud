#!/bin/bash

# Bronze Layer (raw immutable data)
docker exec -it docker-minio-1 mc alias set local http://localhost:9000 minio minio123
docker exec -it docker-minio-1 mc mb local/bronze

# Silver Layer (cleaned validated data)
docker exec -it docker-minio-1 mc mb local/silver

# Gold Layer (aggregated business-ready data)
docker exec -it docker-minio-1 mc mb local/gold
