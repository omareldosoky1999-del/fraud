import time
import os
import json
import threading
import fastavro
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ==============================
# CONFIG
# ==============================
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:29092',
    'acks': 'all',
}

SCHEMA_REGISTRY_CONF = {
    'url': 'http://localhost:8081'
}

TOPIC_NAME = 'transactions'

# مكان الـ script نفسه (للـ schema)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# مكان الـ checkpoint والـ avro files (Data Project folder)
DATA_DIR = r"D:/fraud-detection-platform/ingestion/data"

CHECKPOINT_FILE = os.path.join(DATA_DIR, "checkpoint.json")

# ==============================
# SCHEMA
# ==============================
schema_path = os.path.join(
    BASE_DIR, "../schema/transaction.avsc")  # جنب الـ script

# ==============================
# SCHEMA
# ==============================
schema_path = os.path.join(BASE_DIR, "../schema/transaction.avsc")

with open(schema_path, "r") as f:
    schema_str = f.read()

sr_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONF)

# ==============================
# FILES (READ ONLY)
# ==============================
files_to_process = [
    {
        "path": os.path.join("D:/fraud-detection-platform/ingestion/data/transactions_part1.avro"),
        "name": "CH_1"
    },
    {
        "path": os.path.join("D:/fraud-detection-platform/ingestion/data/transactions_part2.avro"),
        "name": "CH_2"
    },
    {
        "path": os.path.join("D:/fraud-detection-platform/ingestion/data/transactions_part3.avro"),
        "name": "CH_3"
    },
    {
        "path": os.path.join("D:/fraud-detection-platform/ingestion/data/transactions_part4.avro"),
        "name": "CH_4"
    },
    {
        "path": os.path.join("D:/fraud-detection-platform/ingestion/data/transactions_part5.avro"),
        "name": "CH_5"
    },
    {
        "path": os.path.join("D:/fraud-detection-platform/ingestion/data/transactions_part6.avro"),
        "name": "CH_6"
    },
    {
        "path": os.path.join("D:/fraud-detection-platform/ingestion/data/transactions_part7.avro"),
        "name": "CH_7"
    },
    {
        "path": os.path.join("D:/fraud-detection-platform/ingestion/data/transactions_part8.avro"),
        "name": "CH_8"
    },
    {
        "path": os.path.join("D:/fraud-detection-platform/ingestion/data/transactions_part9.avro"),
        "name": "CH_9"
    },
    {
        "path": os.path.join("D:/fraud-detection-platform/ingestion/data/transactions_part10.avro"),
        "name": "CH_10"
    }
]

# ==============================
# CHECKPOINT FUNCTIONS
# ==============================
checkpoint_lock = threading.Lock()


def load_checkpoint():
    """Load last processed Trans_id per channel."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {}


def save_checkpoint(checkpoints):
    """Save checkpoints safely (thread-safe)."""
    with checkpoint_lock:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoints, f, indent=2)

# ==============================
# PRODUCER
# ==============================


def create_producer():
    return Producer(KAFKA_CONF)

# ==============================
# DELIVERY CALLBACK
# ==============================


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")

# ==============================
# STREAM FUNCTION
# ==============================


def stream_avro_file(file_path, channel_name, checkpoints):

    producer = create_producer()
    avro_serializer = AvroSerializer(sr_client, schema_str)

    # get last processed Trans_id for this channel (0 = never processed)
    last_id = checkpoints.get(channel_name, 0)

    if last_id > 0:
        print(f"\n⏩ {channel_name} | Resuming after Trans_id={last_id}")
    else:
        print(f"\n📂 {channel_name} | Starting fresh: {file_path}")

    total_records = 0
    skipped = 0
    new_records = 0

    try:
        with open(file_path, 'rb') as f:

            reader = fastavro.reader(f)

            for record in reader:

                total_records += 1
                trans_id = record.get('Trans_id')

                # skip already-processed records
                if trans_id <= last_id:
                    skipped += 1
                    continue

                new_records += 1

                producer.produce(
                    topic=TOPIC_NAME,
                    key=str(trans_id),
                    value=avro_serializer(
                        record,
                        SerializationContext(
                            TOPIC_NAME,
                            MessageField.VALUE
                        )
                    ),
                    on_delivery=delivery_report
                )

                # display current row
                print(
                    f"📤 {channel_name} | "
                    f"NEW={new_records} | "
                    f"ID={trans_id}"
                )

                # save checkpoint every record
                checkpoints[channel_name] = trans_id
                save_checkpoint(checkpoints)

                time.sleep(0.5)  # simulate delay

                # prevent queue overflow
                if new_records % 100 == 0:
                    producer.poll(0)

        producer.flush()

        print(f"\n✅ {channel_name} FINISHED")
        print(f"📊 SKIPPED={skipped} | NEW SENT={new_records}")

    except Exception as e:
        print(f"❌ Error in {channel_name}: {e}")

# ==============================
# DLQ PRODUCER
# ==============================
DLQ_TOPIC = "fraud.dlq"

def send_to_dlq(event, error_message):
    producer = create_producer()
    dlq_event = {
        "original_event": str(event),
        "error_message": str(error_message),
        "timestamp": int(time.time())
    }
    try:
        producer.produce(
            topic=DLQ_TOPIC,
            key=str(event.get("Trans_id", "unknown")),
            value=json.dumps(dlq_event).encode("utf-8"),
            on_delivery=delivery_report
        )
        producer.flush()
        print(f"⚠️ Sent to DLQ: {dlq_event}")
    except Exception as e:
        print(f"❌ Failed to send to DLQ: {e}")

# ==============================
# RETRY WRAPPER
# ==============================
def safe_produce(producer, avro_serializer, record, retries=3, delay=2):
    for attempt in range(retries):
        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=str(record.get("Trans_id")),
                value=avro_serializer(
                    record,
                    SerializationContext(TOPIC_NAME, MessageField.VALUE)
                ),
                on_delivery=delivery_report
            )
            return True
        except Exception as e:
            print(f"Retry {attempt+1} failed: {e}")
            time.sleep(delay * (attempt+1))
    # لو فشل كل المحاولات → نرمي في DLQ
    send_to_dlq(record, "Max retries exceeded")
    return False

# ==============================
# THREADS
# ==============================
checkpoints = load_checkpoint()
print(f"📌 Loaded checkpoint: {checkpoints}")

threads = []

for file_info in files_to_process:

    t = threading.Thread(
        target=stream_avro_file,
        args=(
            file_info["path"],
            file_info["name"],
            checkpoints
        )
    )

    threads.append(t)
    t.start()

for t in threads:
    t.join()

print("\n🎉 ALL FILES FINISHED")
