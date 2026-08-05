-- Live Kafka inspection.
SELECT user_id, event_type, country, bytes_transferred
FROM kafka.default."security-events"
WHERE bytes_transferred >= 500000000
ORDER BY bytes_transferred DESC;

-- Historical BigQuery analysis after enabling bigquery.properties.
SELECT user_id, count(*) AS events, sum(bytes_transferred) AS total_bytes
FROM bigquery.insider_risk.normalized_events
WHERE event_time >= current_timestamp - INTERVAL '1' DAY
GROUP BY user_id
HAVING sum(bytes_transferred) >= 1000000000;

