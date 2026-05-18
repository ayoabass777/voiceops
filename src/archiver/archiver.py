"""
VoiceOps Archiver (v1)

Consumer group: "archiver"
Reads from voiceops.calls, batches events, writes Parquet to bronze layer.
"""

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("voiceops.archiver")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "voiceops.calls")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "archiver")
BRONZE_PATH = os.getenv("BRONZE_PATH", "./data/bronze/calls")
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


def get_partition_path(event: dict) -> str:
    try:
        dt = datetime.fromisoformat(event["call_started_at"].replace("Z", "+00:00"))
    except (KeyError, ValueError):
        dt = datetime.utcnow()
    return f"{BRONZE_PATH}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"


def flush_buffer(buffer: list[dict]) -> int:
    if not buffer:
        return 0

    partitions: dict[str, list[dict]] = {}
    for event in buffer:
        path = get_partition_path(event)
        partitions.setdefault(path, []).append(event)

    files_written = 0
    for partition_path, events in partitions.items():
        Path(partition_path).mkdir(parents=True, exist_ok=True)
        batch_id = uuid.uuid4().hex[:12]
        filepath = f"{partition_path}/batch_{batch_id}.parquet"

        columns = {field.name: [] for field in ARROW_SCHEMA}
        for event in events:
            for field_name in columns:
                columns[field_name].append(event.get(field_name))

        table = pa.table(columns, schema=ARROW_SCHEMA)
        pq.write_table(table, filepath, compression="snappy")
        files_written += 1
        logger.info(f"Wrote {len(events)} events to {filepath}")

    return files_written


def run():
    consumer = create_consumer()
    consumer.subscribe([TOPIC])

    buffer: list[dict] = []
    last_flush = time.time()
    total_archived = 0

    logger.info(
        f"Starting VoiceOps archiver | group={GROUP_ID} | "
        f"topic={TOPIC} | batch={BATCH_SIZE} | flush={FLUSH_INTERVAL}s"
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
