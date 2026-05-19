"""
VoiceOps Producer (v1)

Simulates call-completed events. Validates before publishing.
Rejected events → voiceops.dlq.
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

from templates import BUSINESS_TEMPLATES

from dotenv import load_dotenv
load_dotenv(dotenv_path="../../.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("voiceops.producer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "voiceops.calls")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "voiceops.dlq")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
BATCH_INTERVAL = int(os.getenv("BATCH_INTERVAL_SECS", "10"))
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "200"))

KNOWN_BUSINESS_IDS = set(BUSINESS_TEMPLATES.keys())


def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "retries": 3,
        "linger.ms": 50,
        "batch.num.messages": 100,
    })


def validate_event(event: dict) -> tuple[bool, str]:
    """Validate event before Kafka publish."""
    required = ["call_id", "business_id", "transcript", "call_duration_secs",
                "status", "call_started_at", "call_ended_at"]
    for field in required:
        if field not in event or event[field] is None:
            return False, f"missing_field: {field}"

    try:
        uuid.UUID(event["call_id"])
    except (ValueError, AttributeError):
        return False, f"invalid_uuid: call_id={event.get('call_id')}"

    if event["business_id"] not in KNOWN_BUSINESS_IDS:
        return False, f"unknown business_id={event['business_id']}"

    if not event["transcript"] or len(event["transcript"].strip()) == 0:
        return False, "transcript_empty"

    try:
        started = datetime.fromisoformat(event["call_started_at"])
        ended = datetime.fromisoformat(event["call_ended_at"])
        if ended < started:
            return False, "invalid_timestamp: ended before started"
    except (ValueError, TypeError) as e:
        return False, f"invalid_timestamp: {e}"

    expected_duration = (ended - started).total_seconds()
    actual_duration = event["call_duration_secs"]
    if actual_duration <= 0:
        return False, f"duration must be positive, got {actual_duration}"
    if abs(expected_duration - actual_duration) > 30:
        return False, f"duration_mismatch: expected ~{expected_duration:.0f}s, got {actual_duration}s"

    return True, ""


def generate_call_event() -> dict:
    business_id = random.choice(list(BUSINESS_TEMPLATES.keys()))
    templates = BUSINESS_TEMPLATES[business_id]
    template = random.choice(templates)

    call_id = str(uuid.uuid4())
    duration = random.randint(*template["duration_range"])

    ended_at = datetime.now(timezone.utc) - timedelta(minutes=random.randint(5, 360))
    started_at = ended_at - timedelta(seconds=duration)

    return {
        "call_id": call_id,
        "business_id": business_id,
        "transcript": template["transcript"],
        "customer_phone": f"+1604{random.randint(1000000, 9999999)}",
        "call_duration_secs": duration,
        "status": "closed",
        "stt_provider": random.choice(["deepgram", "whisper", "assemblyai"]),
        "call_started_at": started_at.isoformat(),
        "call_ended_at": ended_at.isoformat(),
    }


def delivery_callback(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for {msg.key()}: {err}")


def run():
    producer = create_producer()
    produced = 0
    rejected = 0

    logger.info(
        f"Starting VoiceOps producer | topic={TOPIC} | total={TOTAL_EVENTS}"
    )

    try:
        while produced < TOTAL_EVENTS:
            batch = min(BATCH_SIZE, TOTAL_EVENTS - produced)

            for _ in range(batch):
                event = generate_call_event()

                is_valid, error_msg = validate_event(event)
                if not is_valid:
                    logger.warning(f"REJECT {event.get('call_id', '?')}: {error_msg}")
                    producer.produce(
                        topic=DLQ_TOPIC,
                        key=event.get("business_id", "unknown"),
                        value=json.dumps({"event": event, "error": error_msg}).encode("utf-8"),
                    )
                    rejected += 1
                    continue

                producer.produce(
                    topic=TOPIC,
                    key=event["business_id"],
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_callback,
                    timestamp=int(time.time() * 1000),
                )
                produced += 1

            producer.flush()
            logger.info(f"Produced {produced}/{TOTAL_EVENTS} | rejected={rejected}")

            if produced < TOTAL_EVENTS:
                time.sleep(BATCH_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Producer interrupted")
    finally:
        producer.flush(timeout=10)
        logger.info(f"Done. Produced: {produced}, Rejected: {rejected}")


if __name__ == "__main__":
    run()
