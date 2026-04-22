-- Seed 100 users into org_id=3 and grant event_id=1 engineer access to first 50.
-- Safe to re-run: uses INSERT IGNORE / ON DUPLICATE KEY UPDATE.

START TRANSACTION;

-- Ensure roles exist in organization 3.
INSERT IGNORE INTO organization_role (org_id, role, description) VALUES
  (3, 'engineer', 'Can place bets on allowed events/markets'),
  (3, 'marketing', 'Organization member without engineer privileges');

-- Ensure event 1 is open to org 3 engineer role.
-- If event_id=1 belongs to a different org, this can still be valid because
-- event_open_to references organization_role(org_id, role) rather than events.org_id.
INSERT IGNORE INTO event_open_to (event_id, org_id, role_id)
VALUES (1, 3, 'engineer');

-- Set org 3 leader to user_id=1.
INSERT INTO organization_leader (org_id, user_id)
VALUES (3, 1)
ON DUPLICATE KEY UPDATE
  user_id = VALUES(user_id);

-- Create deterministic users: org3_event1_user_001 ... org3_event1_user_100.
INSERT IGNORE INTO users (first, last, age, email, username, password_hash)
WITH RECURSIVE seq AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 100
)
SELECT
  CONCAT('Org3', LPAD(n, 3, '0')) AS first,
  'Benchmark' AS last,
  25 AS age,
  CONCAT('org3_event1_user_', LPAD(n, 3, '0'), '@seed.local') AS email,
  CONCAT('org3_event1_user_', LPAD(n, 3, '0')) AS username,
  '$2b$12$abcdefghijklmnopqrstuv1234567890abcdefghijklmnopq' AS password_hash
FROM seq;

-- Assign all 100 users to org 3:
-- users 001-050 -> engineer (allowed on event 1)
-- users 051-100 -> marketing (not allowed on event 1)
INSERT INTO user_org_role (org_id, role_id, user_id)
SELECT
  3 AS org_id,
  CASE
    WHEN CAST(RIGHT(u.username, 3) AS UNSIGNED) <= 50 THEN 'engineer'
    ELSE 'marketing'
  END AS role_id,
  u.id AS user_id
FROM users u
WHERE u.username REGEXP '^org3_event1_user_[0-9]{3}$'
ON DUPLICATE KEY UPDATE
  role_id = VALUES(role_id);

COMMIT;
