-- Operations tracking + idempotency support (v1): store on MySQL leader/writer.
-- Apply after base schema (multi_server/mysql_instantiation.txt).

CREATE TABLE IF NOT EXISTS operations (
  operation_id CHAR(36) NOT NULL,
  topic VARCHAR(255) NOT NULL,
  status ENUM('queued','processing','succeeded','failed','dead') NOT NULL DEFAULT 'queued',
  envelope_json JSON NOT NULL,
  result_json JSON NULL,
  error_message TEXT NULL,
  kafka_partition INT NULL,
  kafka_offset BIGINT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (operation_id),
  KEY idx_operations_status_created (status, created_at),
  KEY idx_operations_topic_created (topic, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE operations
  ADD COLUMN IF NOT EXISTS result_json JSON NULL AFTER envelope_json;


CREATE TABLE IF NOT EXISTS processed_events (
  event_id CHAR(36) NOT NULL,
  consumer_group VARCHAR(255) NOT NULL,
  topic VARCHAR(255) NOT NULL,
  kafka_partition INT NOT NULL,
  kafka_offset BIGINT NOT NULL,
  status ENUM('received','applied','skipped_duplicate','failed') NOT NULL DEFAULT 'received',
  processed_at TIMESTAMP NULL DEFAULT NULL,
  error_message TEXT NULL,
  PRIMARY KEY (event_id, consumer_group),
  UNIQUE KEY uniq_consumer_partition_offset (consumer_group, topic, kafka_partition, kafka_offset),
  KEY idx_processed_events_processed_at (processed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
