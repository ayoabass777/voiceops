WITH extractions AS (
    SELECT
        call_id, business_id, call_duration_secs, status,
        call_started_at, call_ended_at, kafka_timestamp, ingested_at,
        intent, sentiment_score, resolved, summary, entities, confidence,
        llm_model, llm_latency_ms, s3_path,
        call_started_at::date AS call_date,
        EXTRACT(EPOCH FROM (kafka_timestamp - call_ended_at)) AS ingestion_latency_secs,
        EXTRACT(EPOCH FROM (ingested_at - kafka_timestamp)) AS processing_latency_secs
    FROM {{ source('voiceops', 'call_extractions') }}
    WHERE status = 'closed'
),

businesses AS (
    SELECT business_id, name AS business_name, vertical, region, timezone
    FROM {{ source('voiceops', 'businesses') }}
)

SELECT e.*, b.business_name, b.vertical, b.region, b.timezone
FROM extractions e
INNER JOIN businesses b USING (business_id)
