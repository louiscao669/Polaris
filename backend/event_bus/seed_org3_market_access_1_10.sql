-- Grant org 3 role-based market access using market_as='better'.
-- engineer  -> markets 1-5
-- marketing -> markets 6-10
-- Safe to re-run.

START TRANSACTION;

INSERT INTO market_open_to_as (market_id, org_id, role_id, as_id)
SELECT m.market_id, 3, m.role_id, 'better'
FROM (
  SELECT 1 AS market_id, 'engineer' AS role_id
  UNION ALL SELECT 2, 'engineer'
  UNION ALL SELECT 3, 'engineer'
  UNION ALL SELECT 4, 'engineer'
  UNION ALL SELECT 5, 'engineer'
  UNION ALL SELECT 6, 'marketing'
  UNION ALL SELECT 7, 'marketing'
  UNION ALL SELECT 8, 'marketing'
  UNION ALL SELECT 9, 'marketing'
  UNION ALL SELECT 10, 'marketing'
) AS m
ON DUPLICATE KEY UPDATE
  as_id = VALUES(as_id);

COMMIT;
