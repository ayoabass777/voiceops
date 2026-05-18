"""
VoiceOps Extractor (v1)

Consumer group: "extractor"
Kafka → LLM extraction → validation → Postgres.
Failed events → voiceops.dlq.
Idempotent: ON CONFLICT (call_id) DO NOTHING.
"""

import json
import logging
import os
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer, Producer, KafkaError

from extraction import extract, validate_extraction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("voiceops.extractor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = os.getenv("KAFKA_TOPICS", "voiceops.calls,voiceops.backfill").split(",")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "extractor")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "voiceops.dlq")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DB = os.getenv("PG_DB", "voiceops")
PG_USER = os.getenv("PG_USER", "voiceops")
PG_PASSWORD = os.getenv("PG_PASSWORD", "voiceops")

INSERT_SQL = """
INSERT INTO call_extractions (
    call_id, business_id, call_duration_secs, status,
    call_started_at, call_ended_at, kafka_timestamp, ingested_at,
    intent, sentiment_score, resolved, summary, entities, confidence,
    llm_model, llm_latency_ms, s3_path
) VALUES (
    %(call_id)s, %(business_id)s, %(call_duration_secs)s, %(status)s,
    %(call_started_at)s, %(call_ended_at)s, %(kafka_timestamp)s, %(ingested_at)s,
    %(intent)s, %(sentiment_score)s, %(resolved)s, %(summary)s,
    %(entities)s, %(confidence)s,
    %(llm_model)s, %(llm_latency_ms)s, %(s3_path)s
)
ON CONFLICT (call_id) DO NOTHING
"""

DLQ_SQL = """
INSERT INTO dlq_events (
    call_id, business_id, error_type, error_message,
    raw_payload, failed_at
) VALUES (
    %(call_id)s, %(business_id)s, %(error_type)s, %(error_message)s,
    %(raw_payload)s, %(failed_at)s
)
"""


def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
    })


def create_dlq_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def route_to_dlq(dlq_producer, conn, event, error_type, error_message):
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=event.get("business_id", "unknown"),
        value=json.dumps(event).encode("utf-8"),
    )
    dlq_producer.flush()
    try:
        with conn.cursor() as cur:
            cur.execute(DLQ_SQL, {
                "call_id": event.get("call_id", "unknown"),
                "business_id": event.get("business_id"),
                "error_type": error_type,
                "error_message": error_message[:1000],
                "raw_payload": json.dumps(event),
                "failed_at": datetime.now(timezone.utc),
            })
        conn.commit()
    except Exception as e:
        logger.error(f"DLQ write failed: {e}")
        conn.rollback()


def process_message(msg, conn, dlq_producer) -> bool:
    try:
        event = json.loads(msg.value().decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(f"Parse failed: {e}")
        return False

    call_id = event.get("call_id", "unknown")
    transcript = event.get("transcript", "")

    if event.get("status") != "closed":
        return True

    # LLM extraction
    try:
        extraction, model_name, latency_ms = extract(transcript)
    except Exception as e:
        logger.error(f"Extraction failed {call_id}: {e}")
        route_to_dlq(dlq_producer, conn, event, "llm_timeout", str(e))
        return False

    # Layer 2: validation
    is_valid, error_msg = validate_extraction(extraction)
    if not is_valid:
        logger.warning(f"Validation failed {call_id}: {error_msg}")
        route_to_dlq(dlq_producer, conn, event, "schema_violation", error_msg)
        return False

    # Write to Postgres (idempotent)
    kafka_ts = msg.timestamp()[1]
    kafka_dt = datetime.fromtimestamp(kafka_ts / 1000, tz=timezone.utc)

    try:
        with conn.cursor() as cur:
            cur.execute(INSERT_SQL, {
                "call_id": call_id,
                "business_id": event.get("business_id"),
                "call_duration_secs": event.get("call_duration_secs"),
                "status": event.get("status", "closed"),
                "call_started_at": event.get("call_started_at"),
                "call_ended_at": event.get("call_ended_at"),
                "kafka_timestamp": kafka_dt,
                "ingested_at": datetime.now(timezone.utc),
                "intent": extraction["intent"],
                "sentiment_score": extraction["sentiment_score"],
                "resolved": extraction["resolved"],
                "summary": extraction["summary"],
                "entities": json.dumps(extraction.get("entities", {})),
                "confidence": extraction.get("confidence"),
                "llm_model": model_name,
                "llm_latency_ms": latency_ms,
                "s3_path": None,
            })
        conn.commit()
        logger.info(
            f"OK {call_id} | {extraction['intent']} | "
            f"sentiment={extraction['sentiment_score']} | "
            f"confidence={extraction.get('confidence')} | {latency_ms}ms"
        )
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"PG write failed {call_id}: {e}")
        route_to_dlq(dlq_producer, conn, event, "unknown", str(e))
        return False


def run():
    consumer = create_consumer()
    consumer.subscribe(TOPICS)
    dlq_producer = create_dlq_producer()
    conn = get_pg_connection()
    processed = 0
    failed = 0

    logger.info(f"Starting extractor | group={GROUP_ID} | topics={TOPICS}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            if process_message(msg, conn, dlq_producer):
                processed += 1
            else:
                failed += 1
            consumer.commit()

            if (processed + failed) % 25 == 0 and (processed + failed) > 0:
                total = processed + failed
                logger.info(f"{processed} ok, {failed} failed ({failed/total*100:.1f}%)")

    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        consumer.close()
        dlq_producer.flush()
        conn.close()
        total = processed + failed
        rate = (failed / total * 100) if total > 0 else 0
        logger.info(f"Done. {processed} ok, {failed} failed ({rate:.1f}%)")


if __name__ == "__main__":
    run()
