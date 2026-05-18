-- agg_dlq_health.sql
-- Pipeline health: DLQ failure rates by day and error type.

WITH extraction_counts AS (
    SELECT call_started_at::date AS call_date, COUNT(*) AS total_extracted
    FROM {{ source('voiceops', 'call_extractions') }}
    GROUP BY 1
),

dlq_counts AS (
    SELECT
        failed_at::date AS call_date,
        COUNT(*) AS total_failed,
        COUNT(*) FILTER (WHERE error_type = 'llm_timeout') AS llm_timeout_count,
        COUNT(*) FILTER (WHERE error_type = 'invalid_json') AS invalid_json_count,
        COUNT(*) FILTER (WHERE error_type = 'schema_violation') AS schema_violation_count,
        COUNT(*) FILTER (WHERE retried = FALSE) AS pending_retry
    FROM {{ source('voiceops', 'dlq_events') }}
    GROUP BY 1
)

SELECT
    COALESCE(e.call_date, d.call_date) AS call_date,
    COALESCE(e.total_extracted, 0) AS total_extracted,
    COALESCE(d.total_failed, 0) AS total_failed,
    COALESCE(e.total_extracted, 0) + COALESCE(d.total_failed, 0) AS total_events,
    ROUND(
        COALESCE(d.total_failed, 0)::numeric /
        NULLIF(COALESCE(e.total_extracted, 0) + COALESCE(d.total_failed, 0), 0) * 100, 2
    ) AS failure_rate_pct,
    CASE
        WHEN COALESCE(d.total_failed, 0)::numeric /
             NULLIF(COALESCE(e.total_extracted, 0) + COALESCE(d.total_failed, 0), 0) * 100 > 5 THEN 'ALERT'
        WHEN COALESCE(d.total_failed, 0)::numeric /
             NULLIF(COALESCE(e.total_extracted, 0) + COALESCE(d.total_failed, 0), 0) * 100 > 2 THEN 'INVESTIGATE'
        ELSE 'HEALTHY'
    END AS health_status,
    COALESCE(d.llm_timeout_count, 0) AS llm_timeout_count,
    COALESCE(d.invalid_json_count, 0) AS invalid_json_count,
    COALESCE(d.schema_violation_count, 0) AS schema_violation_count,
    COALESCE(d.pending_retry, 0) AS pending_retry
FROM extraction_counts e
FULL OUTER JOIN dlq_counts d ON e.call_date = d.call_date
ORDER BY call_date DESC
