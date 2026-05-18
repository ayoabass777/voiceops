# VoiceOps

**Streaming analytics pipeline for voice agent conversations.**

Voice agents generate thousands of conversations daily. VoiceOps turns that unstructured data into structured business intelligence in near real-time — streaming call analytics that tell you which intents spike, where resolution drops, and which locations need attention.

---

## Architecture

```
Voice System (simulated)
    │
    ▼ call-completed event
┌──────────────────────────────────────────────────────┐
│  Producer (validates → publishes to Kafka)            │
│       ▼                                              │
│  Kafka: voiceops.calls (6 partitions, key=business_id│
│       │                                              │
│       ├──→ Archiver → S3 Parquet (bronze)            │
│       │                                              │
│       └──→ Extractor → LLM → Postgres (silver)      │
│                │                                     │
│                └→ DLQ on failure                      │
│                                                      │
│  dbt (gold: multi-tenant analytics)                  │
│                                                      │
│  [docker-compose up]                                 │
└──────────────────────────────────────────────────────┘
```

See `docs/architecture.html` for the full interactive diagram with AWAP, AWS, dbt, Scaling, and PIPEDA views.

---

## Quick Start

```bash
# Start infrastructure
docker-compose -f docker/docker-compose.yml up -d

# Terminal 1: produce events
cd src/producer && pip install -r requirements.txt && python producer.py

# Terminal 2: archive to bronze
cd src/archiver && pip install -r requirements.txt && python archiver.py

# Terminal 3: extract to silver
cd src/extractor && pip install -r requirements.txt && python extractor.py

# After events flow: run dbt
cd dbt && dbt seed && dbt run

# Verify
psql -h localhost -U voiceops -d voiceops -c "SELECT * FROM agg_daily_business_summary LIMIT 10;"
```

---

## Key Design Decisions

Full documentation in `docs/design-decisions.md`. Highlights:

| Decision | Rationale |
|---|---|
| **Dual consumer groups** | Archiver (fast, S3) and Extractor (slow, LLM) read the same topic independently |
| **S3 bronze layer** | Raw events are write-once, never queried in real-time. Keeps Postgres lean |
| **Four-timestamp design** | `call_started_at`, `call_ended_at`, `kafka_timestamp`, `ingested_at` — latency diagnosis + late event handling |
| **Three-layer LLM guardrails** | Structured prompt → consumer validation → confidence-gated dbt |
| **Multi-tenant dbt** | Every model scopes by `business_id`. Tenant view + platform view |
| **Backfill via S3** | Read bronze → republish to `voiceops.backfill` → same Extractor code |

---

## Storage Layers

| Layer | Store | Purpose |
|---|---|---|
| **Bronze** | S3 / local Parquet | Immutable archive of every raw event |
| **Silver** | Postgres (`call_extractions`) | LLM-enriched structured data |
| **Gold** | Postgres (dbt models) | Business-facing aggregations |

---

## Current State vs Production Roadmap

This repo demonstrates a working v1 pipeline. The architecture docs describe the full production design, including:

- **AWAP audit gates** at three boundaries — ingestion, archiver, extractor (`docs/architecture.html` → AWAP tab)
- **PIPEDA compliance** — consent gating, PII boundary, customer_hash, PII leak detection, data sovereignty (`→ PIPEDA tab`)
- **Schema Registry** enforcement at the Kafka layer (`→ Production tab`)
- **Observer consumer** + pipeline health monitoring (`→ dbt tab`)
- **Partition-based autoscaling** on ECS Fargate (`→ Scaling tab`)

The gap between v1 and production is operational hardening, not redesign. The data model, storage layers, and consumer architecture are the same.

---

## Project Structure

```
voiceops/
├── README.md
├── docs/
│   ├── architecture.html        # Interactive 5-view diagram
│   └── design-decisions.md      # 12 sections of rationale
├── sql/
│   ├── schema.sql               # Postgres DDL (silver layer)
│   └── seed.sql                 # 8 businesses across 3 verticals
├── src/
│   ├── producer/                # Event generator + validation
│   ├── archiver/                # Kafka → batched Parquet (bronze)
│   └── extractor/               # Kafka → LLM → Postgres (silver)
├── dbt/
│   ├── models/
│   │   ├── staging/             # stg_call_extractions
│   │   └── marts/               # fct_call_metrics, agg_daily_*, agg_dlq_health
│   └── seeds/                   # intents.csv
└── docker/
    └── docker-compose.yml       # Kafka, Zookeeper, Postgres
```

---

## Verticals

Built for service businesses:
- **Plumbing** — emergency calls, booking requests, complaint resolution
- **Auto Repair** — appointment scheduling, status inquiries, warranty questions
- **Property Management** — tenant maintenance requests, lease inquiries, emergency reports
