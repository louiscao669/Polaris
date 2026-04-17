"""MySQL access for the event_bus app (same env vars as multi_server/database.py)."""

from __future__ import annotations

import os
import re
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor

# Canonical DDL: multi_server schema, or optional copy at app/core/mysql_instantiation.txt
_APP_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _APP_DIR.parents[3]
_DEFAULT_DDL = (
    _REPO_ROOT / "multi_server" / "Database_Visualized" / "mysql_instantiation.txt"
)
_LOCAL_DDL = _APP_DIR / "core" / "mysql_instantiation.txt"

MYSQL_DDL_PATH: Path = _LOCAL_DDL if _LOCAL_DDL.is_file() else _DEFAULT_DDL

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "tmitch23")


def _mysql_connect(
    *, database: str | None, autocommit: bool
) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=database,
        charset="utf8mb4",
        autocommit=autocommit,
    )


def _strip_line_comments(sql: str) -> str:
    lines_out: list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        if "--" in line:
            line = line[: line.index("--")]
        lines_out.append(line)
    return "\n".join(lines_out)


def _split_mysql_statements(sql: str) -> list[str]:
    sql = _strip_line_comments(sql)
    parts = re.split(r";\s*", sql)
    return [p.strip() for p in parts if p.strip()]


def _load_mysql_ddl() -> str:
    if not MYSQL_DDL_PATH.is_file():
        raise FileNotFoundError(f"MySQL DDL file not found: {MYSQL_DDL_PATH}")
    return MYSQL_DDL_PATH.read_text(encoding="utf-8")


def create_db_and_tables() -> None:
    """Create database if needed, then run multi_server DDL (optional; call from lifespan)."""
    raw = _load_mysql_ddl()
    statements = _split_mysql_statements(raw)

    conn = _mysql_connect(database=None, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        conn.select_db(MYSQL_DATABASE)
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
    finally:
        conn.close()


@contextmanager
def get_connection() -> Generator[pymysql.connections.Connection, None, None]:
    """Open one connection; use as ``with get_connection() as conn:`` then commit yourself."""
    conn = _mysql_connect(database=MYSQL_DATABASE, autocommit=False)
    try:
        yield conn
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()


def get_db() -> Generator[pymysql.connections.Connection, None, None]:
    """FastAPI dependency: one connection per request."""
    with get_connection() as conn:
        yield conn


def get_cursor_dict(conn: pymysql.connections.Connection):
    return conn.cursor(DictCursor)
