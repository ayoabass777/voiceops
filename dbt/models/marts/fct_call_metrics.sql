SELECT
    call_id, business_id, business_name, vertical, region,
    call_date, call_started_at, call_ended_at, call_duration_secs,
    intent, sentiment_score, resolved, summary, entities, confidence,
    llm_model, llm_latency_ms,
    ingestion_latency_secs, processing_latency_secs,
    CASE WHEN intent = 'emergency' THEN TRUE ELSE FALSE END AS is_emergency,
    entities->>'service_type' AS service_type,
    entities->>'urgency' AS urgency
FROM {{ ref('stg_call_extractions') }}
WHERE confidence >= 0.7
