-- agg_daily_vertical_summary.sql
-- Platform view: per vertical, per day.

SELECT
    vertical, call_date,
    COUNT(*) AS total_calls,
    COUNT(DISTINCT business_id) AS active_businesses,
    COUNT(*) FILTER (WHERE resolved) AS resolved_calls,
    ROUND(COUNT(*) FILTER (WHERE resolved)::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS resolution_rate_pct,
    ROUND(AVG(sentiment_score), 2) AS avg_sentiment,
    ROUND(AVG(call_duration_secs), 0) AS avg_duration_secs,
    COUNT(*) FILTER (WHERE intent = 'booking') AS booking_count,
    COUNT(*) FILTER (WHERE intent = 'complaint') AS complaint_count,
    COUNT(*) FILTER (WHERE intent = 'inquiry') AS inquiry_count,
    COUNT(*) FILTER (WHERE intent = 'emergency') AS emergency_count,
    ROUND(COUNT(*) FILTER (WHERE intent = 'complaint')::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS complaint_rate_pct,
    ROUND(COUNT(*) FILTER (WHERE is_emergency)::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS emergency_rate_pct,
    ROUND(AVG(llm_latency_ms), 0) AS avg_llm_latency_ms
FROM {{ ref('fct_call_metrics') }}
GROUP BY vertical, call_date
ORDER BY call_date DESC, vertical
