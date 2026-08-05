-- Detection 1: high-risk individual events.
SELECT event_time, user_id, event_type, country, bytes_transferred,
       data_classification, risk_score
FROM `PROJECT_ID.insider_risk.normalized_events`
WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND risk_score >= 70
ORDER BY risk_score DESC, event_time DESC;

-- Detection 2: aggregate exfiltration that is individually below a threshold.
SELECT user_id, COUNT(*) AS event_count, SUM(bytes_transferred) AS total_bytes,
       COUNT(DISTINCT device_id) AS devices, MAX(risk_score) AS max_risk
FROM `PROJECT_ID.insider_risk.normalized_events`
WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND event_type IN ('file_download', 'file_upload')
GROUP BY user_id
HAVING total_bytes >= 1000000000 OR devices >= 3
ORDER BY total_bytes DESC;

