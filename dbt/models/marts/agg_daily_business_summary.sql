-- agg_daily_business_summary.sql
-- Tenant view: per business_id, per day.

SELECT
    business_id, business_name, vertical, call_date,
    COUNT(*) AS total_calls,
    COUNT(*) FILTER (WHERE resolved) AS resolved_calls,
    ROUND(COUNT(*) FILTER (WHERE resolved)::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS resolution_rate_pct,
    ROUND(AVG(sentiment_score), 2) AS avg_sentiment,
    MIN(sentiment_score) AS min_sentiment,
    MAX(sentiment_score) AS max_sentiment,
    ROUND(AVG(call_duration_secs), 0) AS avg_duration_secs,
    COUNT(*) FILTER (WHERE intent = 'booking') AS booking_count,
    COUNT(*) FILTER (WHERE intent = 'complaint') AS complaint_count,
    COUNT(*) FILTER (WHERE intent = 'inquiry') AS inquiry_count,
    COUNT(*) FILTER (WHERE intent = 'emergency') AS emergency_count,
    COUNT(*) FILTER (WHERE intent = 'cancellation') AS cancellation_count,
    COUNT(*) FILTER (WHERE intent = 'follow_up') AS follow_up_count,
    COUNT(*) FILTER (WHERE is_emergency) AS total_emergencies,
    ROUND(AVG(llm_latency_ms), 0) AS avg_llm_latency_ms,
    ROUND(AVG(processing_latency_secs), 1) AS avg_processing_latency_secs
FROM {{ ref('fct_call_metrics') }}
GROUP BY business_id, business_name, vertical, call_date
ORDER BY call_date DESC, business_name
