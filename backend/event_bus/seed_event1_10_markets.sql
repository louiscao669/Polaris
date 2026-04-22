-- Seed 10 markets under event_id=1.
-- Safe to re-run: unique (event_id, question) prevents duplicates.

START TRANSACTION;

INSERT INTO market (event_id, question, initial_price, is_open, created_by)
SELECT
  1 AS event_id,
  q.question,
  50 AS initial_price,
  TRUE AS is_open,
  1 AS created_by
FROM (
  SELECT 'Will Team Alpha win Match 1?' AS question
  UNION ALL SELECT 'Will total points in Match 1 be over 210.5?'
  UNION ALL SELECT 'Will Player A score at least 25 points?'
  UNION ALL SELECT 'Will Team Beta lead at halftime?'
  UNION ALL SELECT 'Will Match 1 go into overtime?'
  UNION ALL SELECT 'Will Team Alpha make 12+ three-pointers?'
  UNION ALL SELECT 'Will Player B record a double-double?'
  UNION ALL SELECT 'Will Team Alpha commit fewer than 14 turnovers?'
  UNION ALL SELECT 'Will Team Beta score at least 105 points?'
  UNION ALL SELECT 'Will the winning margin be 10 points or more?'
) AS q
ON DUPLICATE KEY UPDATE
  initial_price = VALUES(initial_price),
  is_open = VALUES(is_open),
  created_by = VALUES(created_by);

COMMIT;
