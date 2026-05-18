# Design Decisions

Every non-obvious design choice in VoiceOps and the reasoning behind it. Written as interview reference — every section answers a "why?" a senior engineer would ask.

---

## 1. Decoupled Ingestion and Enrichment

### The Problem

The original design had one consumer doing two jobs: writing to Postgres (~5ms) and calling the LLM (~2s). The slow step throttles the fast step. Like a cashier who also cooks your food.

### The Solution: Dual Consumer Groups

Two independent consumer groups on the same Kafka topic:

| Consumer Group | Job | Bottleneck | Instances |
|---|---|---|---|
| `archiver` | Batch write raw events to S3 (Parquet) | S3 I/O (~50ms/batch) | 1-2 |
| `extractor` | LLM extraction → Postgres | LLM API (~2s/call) | 3-6 |

Kafka's consumer group model means they read the same partitions independently. The Archiver flies through at disk speed. The Extractor crawls at LLM speed. Neither affects the other.

**Analogy:** A DVR recording live TV. The Archiver watches in real-time. The Extractor watches on a 30-second delay to take notes. The DVR doesn't slow down for either viewer.

### Why Not CDC

We evaluated three decoupling options:

1. **Extra Kafka topic** — producer publishes to both `voiceops.calls` and `voiceops.ready`. More infrastructure.
2. **Readiness marker in Postgres** — `extraction_status` column, Extractor polls. Muddies the bronze layer.
3. **CDC (Debezium)** — watches Postgres INSERTs, streams to Extractor. Clean, but unnecessary once bronze moves to S3.

CDC solves "how does the Extractor know there's new work?" — but that problem only exists when both layers share the same database. Once bronze moves to S3, each consumer reads directly from Kafka. The trigger *is* the message. No intermediary needed.

---

## 2. S3 Bronze Layer

### The Insight

`raw_calls` was write-once, never updated, never queried by the Extractor in real-time, never joined by dbt. It was using Postgres as a filing cabinet. S3 *is* the filing cabinet.

### Storage Layer Map

| Layer | Store | Purpose | Access Pattern |
|---|---|---|---|
| **Bronze** | S3 (Parquet) | Immutable archive of every raw event | Write-once, bulk reads for backfill/audit |
| **Silver** | Postgres (`call_extractions`) | LLM-enriched structured data | Frequent reads by dbt, joins, aggregations |
| **Gold** | Postgres (dbt models) | Business metrics, tenant dashboards | API/dashboard reads |

### S3 Partitioning

```
s3://voiceops-bronze/
  └── calls/
      └── year=2026/
          └── month=01/
              └── day=15/
                  └── hour=14/
                      └── batch_001.parquet
```

Hive-style partitioning. Athena auto-discovers partitions. Horizontal partitioning by `call_started_at`.

### Archiver Batching

The Archiver batches 500 calls per Parquet file, flushing every 60 seconds. Fewer large files > many small files (classic S3 anti-pattern). Larger files = better compression, cheaper S3 reads, faster Athena scans.

```python
buffer = []
for message in consumer:
    buffer.append(message)
    if len(buffer) >= 500 or time_since_last_flush > 60:
        write_parquet(buffer, path=f"s3://voiceops-bronze/calls/date={date}/batch_{uuid}.parquet")
        consumer.commit()
        buffer = []
```

### Cost Comparison

A year of call transcripts: ~$0.023/GB/month in S3 vs. ~$0.115/GB/month in RDS. For archival data that's rarely queried, S3 wins by 5x.

### S3 Bridge

`call_extractions.s3_path` stores the Parquet file path in bronze. If you ever need the raw transcript for a specific extraction, follow the path. This bridges silver (Postgres) back to bronze (S3).

### For The Demo

Local Parquet files in `./data/bronze/` — same pattern, just local instead of S3.

---

## 3. Four-Timestamp Design

### The Problem

A single `event_timestamp` is ambiguous. A 45-minute call starting at 11:50pm ends at 12:35am — single timestamp assigns it to the wrong day in aggregations.

### The Four Timestamps

| Timestamp | What it records | Question it answers |
|---|---|---|
| `call_started_at` | When the customer connected | **Business time.** dbt partition key. Which day does this call belong to? |
| `call_ended_at` | When the hangup fired | **Session boundary.** `ended - started` cross-checks `call_duration_secs`. |
| `kafka_timestamp` | When Kafka received the event | **Ingestion latency** = `kafka_timestamp - call_ended_at` |
| `ingested_at` | When the Extractor wrote the row | **Processing latency** = `ingested_at - kafka_timestamp` (includes LLM call) |

**Analogy:** Package delivery. `call_started_at` = order placed. `call_ended_at` = shipped. `kafka_timestamp` = reached distribution center. `ingested_at` = delivered.

### Constraint

```sql
CONSTRAINT chk_call_time_order CHECK (call_ended_at >= call_started_at)
```

Catches bad upstream data at insert time.

---

## 4. Lateness Handling

### The Problem

A call ends at 11:45pm Tuesday. The Extractor is backed up. The row lands in Postgres at 12:15am Wednesday. dbt ran at midnight and missed it. Where does it show up?

### The Solution

dbt uses two timestamps for two jobs:

- **Watermark** (`ingested_at`): "What rows are new since my last run?" Finds late-arriving rows.
- **Bucket** (`call_started_at`): "Which day does this call belong to?" Places it correctly.

```sql
-- dbt incremental
{% if is_incremental() %}
WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}

GROUP BY business_id, call_started_at::date
```

### Supporting Indexes

```sql
CREATE INDEX idx_extractions_ingested ON call_extractions (ingested_at);
CREATE INDEX idx_extractions_biz_started ON call_extractions (business_id, call_started_at);
```

---

## 5. Session Lifecycle

### The Three States

| State | Meaning | Managed by | Cold path processes? |
|---|---|---|---|
| `active` | Call live, audio flowing | Hot path | No |
| `pending` | Soft end signal, grace period (15-30s) | Hot path | No |
| `closed` | Hard termination confirmed | Hot path | **Yes** |

Both Archiver and Extractor only process `closed` events. The `pending` state prevents premature extraction on incomplete transcripts.

For the demo: producer sets `status = 'closed'` on every event.

---

## 6. Kafka Partition Scaling

### The Rule

One partition = one consumer max within a consumer group. Partitions are the scaling ceiling.

### Partition Key: `business_id`

- **Ordering per tenant:** Call A before Call B for same business → processed in order
- **Even distribution:** Hash of `business_id` spreads tenants across partitions
- **No conflicts:** Two Extractors never write for the same tenant simultaneously

### Scaling Model

```
voiceops.calls (6 partitions)

Consumer Group: "archiver"          Consumer Group: "extractor"
  Archiver A → P0-P5                 Extractor A → P0, P1
  (1 handles all 6)                  Extractor B → P2, P3
                                     Extractor C → P4, P5
```

Both groups read every message from every partition. Independent offsets. The Archiver needs 1-2 instances. The Extractor needs 3-6. Adding a 4th Extractor → Kafka rebalances automatically.

### Throughput Math

| Extractors | Calls/Hour | Platform Size |
|---|---|---|
| 1 | ~1,800 | Small |
| 3 | ~5,400 | Medium |
| 6 | ~10,800 | 1000+ tenants |

### Fargate Autoscaling

```
Consumer lag > 1000 → scale Extractors 2 → 6
Consumer lag < 100  → scale back to 2
```

---

## 7. Backfill

### The Pattern

Read from S3 bronze → republish to `voiceops.backfill` topic → Extractor subscribes to both `voiceops.calls` (real-time) and `voiceops.backfill` (reprocessing).

```python
# Backfill script
for parquet_file in s3.list("s3://voiceops-bronze/calls/2026/01/"):
    for row in read_parquet(parquet_file):
        producer.send("voiceops.backfill", row)
```

Same Extractor code, different input topic. Throttle publish rate to avoid overwhelming the LLM API.

### Why This Works

- Bronze in S3 is the source of truth — every raw event ever received
- Backfill is self-serve: change the date range, republish, done
- No special tooling — same Kafka, same consumer, different topic

---

## 8. Multi-Tenancy

### Two Audiences, One Pipeline

| Audience | Model | View |
|---|---|---|
| Tenant | `agg_daily_business_summary` | "47 calls, 81% resolved, sentiment 3.6/5" |
| Platform | `agg_daily_vertical_summary` | "Plumbing: 74% resolution, auto: 82%" |

Every dbt model scopes by `business_id`. Composite index `(business_id, call_started_at)` ensures tenant-scoped queries hit the index first.

Production extension: Row-Level Security per `business_id`.

---

## 9. Sentiment: Numeric vs Categorical

`NUMERIC(2,1)` instead of `positive/neutral/negative` because:

- `AVG(sentiment_score)` — one expression, immediately actionable
- Threshold alerting: "alert if avg sentiment < 3.0 in last 24h"
- Trend detection: "sentiment dropped 0.3 points this week"
- LLMs produce more consistent output on a 1-5 scale than on category labels

Categorical text throws away gradient information.

---

## 10. JSONB Entities

Plumbing: `{service_type: "drain_clearing", urgency: "high"}`
Auto: `{vehicle_make: "Honda", mileage: 80000}`
Property: `{unit_number: "4B", issue_type: "water_damage"}`

Each vertical has different entity schemas. JSONB lets them evolve without migrations. dbt extracts via `entities->>'key'`. Separate nullable columns would create a sparse, confusing table.

---

## 11. LLM Guardrails (Three-Layer Defense)

### Layer 1: Structured Output (Prompt)

Force strict JSON with constrained values. Use tool use / function calling mode for guaranteed valid JSON.

```
Return ONLY valid JSON:
{
  "intent": one of ["booking","complaint","inquiry","emergency","cancellation","follow_up","other"],
  "sentiment_score": float between 1.0 and 5.0,
  "resolved": boolean,
  "summary": string max 500 chars,
  "confidence": float between 0.0 and 1.0,
  "entities": {...}
}
```

### Layer 2: Validation (Consumer)

```python
def validate_extraction(data: dict) -> bool:
    if data["intent"] not in VALID_INTENTS:
        return False
    if not (1.0 <= data["sentiment_score"] <= 5.0):
        return False
    if not isinstance(data["resolved"], bool):
        return False
    if len(data["summary"]) > 500:
        return False
    if not (0.0 <= data["confidence"] <= 1.0):
        return False
    return True
```

Fails → DLQ. No bad data reaches Postgres.

### Layer 3: Confidence Gating (dbt)

```sql
WHERE confidence >= 0.7
```

Low-confidence extractions excluded from aggregates or flagged separately.

### Model Tracking

`llm_model` + `llm_latency_ms` enable A/B testing, cost monitoring, regression detection.

---

## 12. DLQ Design

| Error Type | Cause | Recoverable? |
|---|---|---|
| `llm_timeout` | API too slow | Yes — retry |
| `invalid_json` | Non-JSON response | Sometimes |
| `schema_violation` | Valid JSON, wrong fields | Sometimes |
| `llm_refusal` | Content policy block | No |
| `unknown` | Unexpected error | Investigate |

`raw_payload` (JSONB) preserved for replay. DLQ rate is a pipeline health metric: < 2% healthy, > 5% alert.
