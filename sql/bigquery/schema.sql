CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.insider_risk`
OPTIONS(location = 'US');

CREATE TABLE IF NOT EXISTS `PROJECT_ID.insider_risk.normalized_events` (
  event_id STRING NOT NULL,
  event_time TIMESTAMP,
  user_id STRING,
  department STRING,
  event_type STRING,
  source_ip STRING,
  country STRING,
  device_id STRING,
  resource STRING,
  data_classification STRING,
  bytes_transferred INT64,
  privileged_account BOOL,
  is_known_device BOOL,
  label STRING,
  off_hours BOOL,
  risk_score INT64,
  severity STRING,
  processed_at TIMESTAMP
)
PARTITION BY DATE(event_time)
CLUSTER BY user_id, severity, department;

