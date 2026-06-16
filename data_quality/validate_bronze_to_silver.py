import great_expectations as ge
import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

# Load Bronze data
df = ge.read_csv("s3a://bronze/transactions/*.csv")

# Load expectation suite
suite = ge.core.ExpectationSuite("transactions_suite")
results = df.validate(expectation_suite=suite)

# Save validation report
report_path = "s3a://silver/reports/validation_report.json"
with open("validation_report.json", "w") as f:
    f.write(results.to_json_dict())
os.system(f"mc cp validation_report.json local/silver/reports/")

# Promote valid data to Silver
if results.success:
    df.to_csv("s3a://silver/transactions/validated.csv", index=False)
else:
    df.to_csv("s3a://bronze/invalid/failed_validation.csv", index=False)
