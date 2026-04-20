"""
Schema is defined in Database_Visualized/mysql_instantiation.txt (loaded by database.py).
This module holds the file path and table name constants for application SQL.
"""

from pathlib import Path

# Path to canonical MySQL DDL (relative to this package directory = single_server/)
MYSQL_DDL_PATH = Path(__file__).resolve().parent / "Database_Visualized" / "mysql_instantiation.txt"

# --- Table names (match mysql_instantiation.txt backtick names) ---

ORGANIZATION = "organization"
USERS = "users"
ORGANIZATION_ROLE = "organization_role"
ORGANIZATION_TOKEN = "organization_token"
ORGANIZATION_LEADER = "organization_leader"
USER_ORG_ROLE = "user_org_role"
USER_TOKEN_STOCK = "user_token_stock"
USER_SESSION = "user_session"
CONSTRAINT_TYPE = "constraint_type"
EVENTS = "events"
EVENT_TOKENS_ALLOWED = "event_tokens_allowed"
EVENT_CONSTRAINTS = "event_constraints"
EVENT_OPEN_TO = "event_open_to"
EVENT_MARKET_CREATORS = "event_market_creators"
MARKET = "market"
MARKET_CONSTRAINT = "market_constraint"
MARKET_AS = "market_as"
MARKET_OPEN_TO_AS = "market_open_to_as"
MARKET_TOKENS_ALLOWED = "market_tokens_allowed"
MARKET_TRANSACTION = "market_transaction"
MARKET_RESULT = "market_result"
USER_MARKET_SHARES = "user_market_shares"
MARKET_PRICE_SNAPSHOT = "market_price_snapshot"
