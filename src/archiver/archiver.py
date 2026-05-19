"""
VoiceOps Archiver (v1)

Consumer group: "archiver"
Reads from voiceops.calls, batches events, writes Parquet to bronze layer.
Writes to S3 when S3_BUCKET is set, otherwise falls back to local disk.
"""

import io
import json
import logging
import os
import time
import uuid
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv(dotenv_path="../../.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("voiceops.archiver")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "voiceops.calls")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "archiver")
BRONZE_PATH = os.getenv("BRONZE_PATH", "./data/bronze/calls")
S3_BUCKET = os.getenv("S3_BUCKET", "")
AWS_REGION = os.getenv("AWS_REGION", "ca-central-1")
BATCH_SIZE = int(os.getenv("ARCHIVER_BATCH_SIZE", "500"))
FLUSH_INTERVAL = int(os.getenv("ARCHIVER_FLUSH_INTERVAL_SECS", "60"))

ARROW_SCHEMA = pa.schema([
    ("call_id", pa.string()),
    ("business_id", pa.string()),
    ("transcript", pa.string()),
    ("customer_phone", pa.string()),
    ("call_duration_secs", pa.int32()),
    ("status", pa.string()),
    ("stt_provider", pa.string()),
    ("call_started_at", pa.string()),
    ("call_ended_at", pa.string()),
    ("kafka_timestamp_ms", pa.int64()),
    ("archived_at", pa.string()),
])

# S3 client (lazy init)
_s3_client = None


def get_s3_client():
    global _s3_client
    if _s3_client is None:
        import boto3
        _s3_client = boto3.client("s3", region_name=AWS_REGION)
    return _s3_client


def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
    })


def parse_message(msg) -> dict | None:
    try:
        event = json.loads(msg.value().decode("utf-8"))
        event["kafka_timestamp_ms"] = msg.timestamp()[1]
        event["archived_at"] = datetime.utcnow().isoformat()
        return event
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(f"Failed to parse message: {e}")
        return None


def get_partition_key(event: dict) -> str:
    try:
        dt = datetime.fromisoformat(event["call_started_at"].replace("Z", "+00:00"))
    except (KeyError, ValueError):
        dt = datetime.utcnow()
    return f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"


def write_parquet_to_s3(events: list[dict], partition_key: str) -> str:
    """Write events as Parquet to S3. Returns the S3 path."""
    batch_id = uuid.uuid4().hex[:12]
    s3_key = f"calls/{partition_key}/batch_{batch_id}.parquet"

    columns = {field.name: [] for field in ARROW_SCHEMA}
    for event in events:
        for field_name in columns:
            columns[field_name].append(event.get(field_name))

    table = pa.table(columns, schema=ARROW_SCHEMA)

    # Write to in-memory buffer, upload to S3
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    s3 = get_s3_client()
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buf.getvalue())

    return f"s3://{S3_BUCKET}/{s3_key}"


def write_parquet_to_local(events: list[dict], partition_key: str) -> str:
    """Write events as Parquet to local disk. Returns the file path."""
    batch_id = uuid.uuid4().hex[:12]
    dir_path = f"{BRONZE_PATH}/{partition_key}"
    Path(dir_path).mkdir(parents=True, exist_ok=True)
    filepath = f"{dir_path}/batch_{batch_id}.parquet"

    columns = {field.name: [] for field in ARROW_SCHEMA}
    for event in events:
        for field_name in columns:
            columns[field_name].append(event.get(field_name))

    table = pa.table(columns, schema=ARROW_SCHEMA)
    pq.write_table(table, filepath, compression="snappy")

    return filepath


def flush_buffer(buffer: list[dict]) -> int:
    if not buffer:
        return 0

    # Group events by partition key
    partitions: dict[str, list[dict]] = {}
    for event in buffer:
        key = get_partition_key(event)
        partitions.setdefault(key, []).append(event)

    files_written = 0
    use_s3 = bool(S3_BUCKET)

    for partition_key, events in partitions.items():
        if use_s3:
            path = write_parquet_to_s3(events, partition_key)
        else:
            path = write_parquet_to_local(events, partition_key)

        files_written += 1
        dest = "S3" if use_s3 else "local"
        logger.info(f"Wrote {len(events)} events to {path} ({dest}, snappy)")

    return files_written


def run():
    consumer = create_consumer()
    consumer.subscribe([TOPIC])

    buffer: list[dict] = []
    last_flush = time.time()
    total_archived = 0

    dest = f"s3://{S3_BUCKET}" if S3_BUCKET else BRONZE_PATH
    logger.info(
        f"Starting VoiceOps archiver | group={GROUP_ID} | "
        f"topic={TOPIC} | batch={BATCH_SIZE} | flush={FLUSH_INTERVAL}s | "
        f"dest={dest}"
    )

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
            else:
                event = parse_message(msg)
                if event:
                    buffer.append(event)

            elapsed = time.time() - last_flush
            if len(buffer) >= BATCH_SIZE or (buffer and elapsed >= FLUSH_INTERVAL):
                count = len(buffer)
                flush_buffer(buffer)
                consumer.commit()
                total_archived += count
                buffer.clear()
                last_flush = time.time()
                logger.info(f"Archived {total_archived} total ({count} this batch)")

    except KeyboardInterrupt:
        logger.info("Archiver interrupted")
    finally:
        if buffer:
            flush_buffer(buffer)
            consumer.commit()
            total_archived += len(buffer)
        consumer.close()
        logger.info(f"Done. Total archived: {total_archived}")


if __name__ == "__main__":
    run()
