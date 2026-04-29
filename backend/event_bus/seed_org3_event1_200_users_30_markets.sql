-- Scale seed for org_id=3 / event_id=1:
--   - 200 users total: 100 engineer + 100 marketing
--   - 30 markets total under event 1
--   - market access split: engineer -> markets 1..15, marketing -> markets 16..30
--   - token balances: engineers get ENGINEER_TOKEN, marketing gets MARKETING_TOKEN
--   - market token rules: markets 1..15 allow ENGINEER_TOKEN, 16..30 allow MARKETING_TOKEN
--
-- Safe to re-run (uses INSERT IGNORE / ON DUPLICATE KEY UPDATE).

START TRANSACTION;

-- ---------------------------------------------------------------------------
-- Roles + event access + leader
-- ---------------------------------------------------------------------------
INSERT IGNORE INTO organization_role (org_id, role, description) VALUES
  (3, 'engineer', 'Can place bets on allowed markets'),
  (3, 'marketing', 'Can place bets on allowed markets');

INSERT IGNORE INTO event_open_to (event_id, org_id, role_id)
VALUES (1, 3, 'engineer');

INSERT INTO organization_leader (org_id, user_id)
VALUES (3, 1)
ON DUPLICATE KEY UPDATE
  user_id = VALUES(user_id);

-- ---------------------------------------------------------------------------
-- Users: org3_event1_user_001 ... org3_event1_user_200
-- ---------------------------------------------------------------------------
INSERT IGNORE INTO users (first, last, age, email, username, password_hash)
WITH RECURSIVE seq AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 200
)
SELECT
  CONCAT('Org3', LPAD(n, 3, '0')) AS first,
  'Benchmark' AS last,
  25 AS age,
  CONCAT('org3_event1_user_', LPAD(n, 3, '0'), '@seed.local') AS email,
  CONCAT('org3_event1_user_', LPAD(n, 3, '0')) AS username,
  '$2b$12$abcdefghijklmnopqrstuv1234567890abcdefghijklmnopq' AS password_hash
FROM seq;

-- Assign first 100 as engineer, next 100 as marketing.
INSERT INTO user_org_role (org_id, role_id, user_id)
SELECT
  3 AS org_id,
  CASE
    WHEN CAST(RIGHT(u.username, 3) AS UNSIGNED) <= 100 THEN 'engineer'
    ELSE 'marketing'
  END AS role_id,
  u.id AS user_id
FROM users u
WHERE u.username REGEXP '^org3_event1_user_[0-9]{3}$'
  AND CAST(RIGHT(u.username, 3) AS UNSIGNED) BETWEEN 1 AND 200
ON DUPLICATE KEY UPDATE
  role_id = VALUES(role_id);

-- ---------------------------------------------------------------------------
-- Markets 1..30 under event_id=1 (explicit ids for deterministic benchmarking)
-- ---------------------------------------------------------------------------
INSERT INTO market (id, event_id, question, initial_price, is_open, created_by)
WITH RECURSIVE seq AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 30
)
SELECT
  n AS id,
  1 AS event_id,
  CONCAT('Benchmark Market ', LPAD(n, 2, '0'), ': Will metric ', LPAD(n, 2, '0'), ' beat target?') AS question,
  50 AS initial_price,
  TRUE AS is_open,
  1 AS created_by
FROM seq
ON DUPLICATE KEY UPDATE
  event_id = VALUES(event_id),
  question = VALUES(question),
  initial_price = VALUES(initial_price),
  is_open = VALUES(is_open),
  created_by = VALUES(created_by);

-- Role-based market access:
-- 1..15 engineer, 16..30 marketing
INSERT INTO market_open_to_as (market_id, org_id, role_id, as_id)
WITH RECURSIVE seq AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 30
)
SELECT
  n AS market_id,
  3 AS org_id,
  CASE WHEN n <= 15 THEN 'engineer' ELSE 'marketing' END AS role_id,
  'better' AS as_id
FROM seq
ON DUPLICATE KEY UPDATE
  role_id = VALUES(role_id),
  as_id = VALUES(as_id);

-- ---------------------------------------------------------------------------
-- Tokens + balances + market token rules
-- ---------------------------------------------------------------------------
INSERT IGNORE INTO organization_token (org_id, name, description) VALUES
  (3, 'ENGINEER_TOKEN', 'Benchmark token for engineer role'),
  (3, 'MARKETING_TOKEN', 'Benchmark token for marketing role');

SET @engineer_token_id := (
  SELECT token_id
  FROM organization_token
  WHERE org_id = 3 AND name = 'ENGINEER_TOKEN'
  LIMIT 1
);
SET @marketing_token_id := (
  SELECT token_id
  FROM organization_token
  WHERE org_id = 3 AND name = 'MARKETING_TOKEN'
  LIMIT 1
);

INSERT INTO user_token_stock (token_id, user_id, qty)
SELECT @engineer_token_id, uor.user_id, 1000000
FROM user_org_role uor
WHERE uor.org_id = 3
  AND uor.role_id = 'engineer'
ON DUPLICATE KEY UPDATE
  qty = VALUES(qty);

INSERT INTO user_token_stock (token_id, user_id, qty)
SELECT @marketing_token_id, uor.user_id, 1000000
FROM user_org_role uor
WHERE uor.org_id = 3
  AND uor.role_id = 'marketing'
ON DUPLICATE KEY UPDATE
  qty = VALUES(qty);

INSERT INTO market_tokens_allowed (market_id, token_id)
WITH RECURSIVE seq AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 30
)
SELECT
  n AS market_id,
  CASE WHEN n <= 15 THEN @engineer_token_id ELSE @marketing_token_id END AS token_id
FROM seq
ON DUPLICATE KEY UPDATE
  token_id = VALUES(token_id);

COMMIT;
