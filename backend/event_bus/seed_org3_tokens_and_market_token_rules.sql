-- Seed token balances and market token permissions for org 3 users/markets.
-- engineer users   -> org 3 token named 'ENGINEER_TOKEN' balance=1,000,000
-- marketing users  -> org 3 token named 'MARKETING_TOKEN' balance=1,000,000
-- market 1-5       -> allow ENGINEER_TOKEN
-- market 6-10      -> allow MARKETING_TOKEN
-- Safe to re-run.

START TRANSACTION;

-- Ensure org 3 has the two tokens we need.
INSERT IGNORE INTO organization_token (org_id, name, description) VALUES
  (3, 'ENGINEER_TOKEN', 'Benchmark token for engineer role'),
  (3, 'MARKETING_TOKEN', 'Benchmark token for marketing role');

-- Resolve token IDs dynamically (FK-safe, no hardcoded IDs).
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

-- Give engineer users engineer token.
INSERT INTO user_token_stock (token_id, user_id, qty)
SELECT
  @engineer_token_id AS token_id,
  uor.user_id,
  1000000 AS qty
FROM user_org_role uor
WHERE uor.org_id = 3
  AND uor.role_id = 'engineer'
ON DUPLICATE KEY UPDATE
  qty = VALUES(qty);

-- Give marketing users marketing token.
INSERT INTO user_token_stock (token_id, user_id, qty)
SELECT
  @marketing_token_id AS token_id,
  uor.user_id,
  1000000 AS qty
FROM user_org_role uor
WHERE uor.org_id = 3
  AND uor.role_id = 'marketing'
ON DUPLICATE KEY UPDATE
  qty = VALUES(qty);

-- Configure allowed token per market.
INSERT INTO market_tokens_allowed (market_id, token_id)
SELECT m.market_id, m.token_id
FROM (
  SELECT 1 AS market_id, @engineer_token_id AS token_id
  UNION ALL SELECT 2, @engineer_token_id
  UNION ALL SELECT 3, @engineer_token_id
  UNION ALL SELECT 4, @engineer_token_id
  UNION ALL SELECT 5, @engineer_token_id
  UNION ALL SELECT 6, @marketing_token_id
  UNION ALL SELECT 7, @marketing_token_id
  UNION ALL SELECT 8, @marketing_token_id
  UNION ALL SELECT 9, @marketing_token_id
  UNION ALL SELECT 10, @marketing_token_id
) AS m
ON DUPLICATE KEY UPDATE
  token_id = VALUES(token_id);

COMMIT;
