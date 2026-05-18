-- =============================================================
-- VOICEOPS SCHEMA (v1 — Demo)
-- =============================================================
-- Bronze: S3/local Parquet — immutable archive of raw events
-- Silver: Postgres — LLM-enriched structured data
-- Gold:   Postgres (dbt) — business-facing aggregations
-- =============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- =============================================================
-- 1. BUSINESSES (tenant dimension)
-- =============================================================
CREATE TABLE businesses (
    business_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(255) NOT NULL,
    vertical        VARCHAR(50)  NOT NULL CHECK (vertical IN ('plumbing', 'auto_repair', 'property_management')),
    region          VARCHAR(100),
    timezone        VARCHAR(50)  DEFAULT 'America/Vancouver',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);


-- =============================================================
-- 2. CALL_EXTRACTIONS (silver layer)
-- =============================================================
CREATE TABLE call_extractions (
    call_id             UUID PRIMARY KEY,
    business_id         UUID         NOT NULL REFERENCES businesses(business_id),
    call_duration_secs  INTEGER      NOT NULL CHECK (call_duration_secs > 0),
    status              VARCHAR(10)  NOT NULL DEFAULT 'closed'
                        CHECK (status IN ('active', 'pending', 'closed')),

    -- Four-timestamp design
    call_started_at     TIMESTAMPTZ  NOT NULL,
    call_ended_at       TIMESTAMPTZ  NOT NULL,
    kafka_timestamp     TIMESTAMPTZ  NOT NULL,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- LLM extraction output
    intent              VARCHAR(50)  NOT NULL
                        CHECK (intent IN (
                            'booking', 'complaint', 'inquiry',
                            'emergency', 'cancellation', 'follow_up', 'other'
                        )),
    sentiment_score     NUMERIC(2,1) NOT NULL CHECK (sentiment_score BETWEEN 1.0 AND 5.0),
    resolved            BOOLEAN      NOT NULL,
    summary             VARCHAR(500) NOT NULL,
    entities            JSONB        NOT NULL DEFAULT '{}',
    confidence          NUMERIC(3,2) CHECK (confidence BETWEEN 0.00 AND 1.00),

    -- LLM tracking
    llm_model           VARCHAR(100) NOT NULL,
    llm_latency_ms      INTEGER,

    -- Bronze reference
    s3_path             VARCHAR(500),

    CONSTRAINT chk_call_time_order CHECK (call_ended_at >= call_started_at)
);

CREATE INDEX idx_extractions_biz_started ON call_extractions (business_id, call_started_at);
CREATE INDEX idx_extractions_ingested ON call_extractions (ingested_at);
CREATE INDEX idx_extractions_confidence ON call_extractions (confidence) WHERE confidence >= 0.7;


-- =============================================================
-- 3. DLQ_EVENTS
-- =============================================================
CREATE TABLE dlq_events (
    dlq_id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    call_id             UUID         NOT NULL,
    business_id         UUID,
    error_type          VARCHAR(50)  NOT NULL
                        CHECK (error_type IN (
                            'llm_timeout', 'invalid_json', 'schema_violation',
                            'llm_refusal', 'unknown'
                        )),
    error_message       TEXT,
    raw_payload         JSONB        NOT NULL,
    failed_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    retried             BOOLEAN      NOT NULL DEFAULT FALSE,
    retried_at          TIMESTAMPTZ
);

CREATE INDEX idx_dlq_biz_failed ON dlq_events (business_id, failed_at);
CREATE INDEX idx_dlq_not_retried ON dlq_events (retried) WHERE retried = FALSE;
