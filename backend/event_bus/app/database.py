"""MySQL access — Aurora-style leader vs follower routing.

Canonical env (preferred):

- ``LEADER_DB_HOST`` — writer / primary cluster endpoint.
- ``FOLLOWER_DB_HOST`` — optional comma-separated replica hostnames (or a single host).
- ``DB_PORT``, ``DB_NAME``, ``DB_USER``, ``DB_PASSWORD`` — shared connection parameters.

Legacy fallbacks (still supported): ``MYSQL_WRITER_HOST``, ``MYSQL_HOST``,
``MYSQL_READER_HOSTS``, ``MYSQL_READER_HOST``, ``MYSQL_PORT``, ``MYSQL_DATABASE``,
``MYSQL_USER``, ``MYSQL_PASSWORD``.

If no followers are set, reader connections use the leader host (local dev).
"""

from __future__ import annotations

import os
import random
import re
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

import pymysql
from pymysql.cursors import DictCursor

_APP_DIR = Path(__file__).resolve().parent
# …/backend/event_bus/app → …/backend is parents[1]
_BACKEND_ROOT = _APP_DIR.parents[1]
_DEFAULT_DDL = (
    _BACKEND_ROOT / "multi_server" / "Database_Visualized" / "mysql_instantiation.txt"
)
_LOCAL_DDL = _APP_DIR / "core" / "mysql_instantiation.txt"
_OPERATIONS_EXT = _APP_DIR / "core" / "mysql_operations_tables.sql"

MYSQL_DDL_PATH: Path = _LOCAL_DDL if _LOCAL_DDL.is_file() else _DEFAULT_DDL
MYSQL_OPERATIONS_EXT_PATH: Path = _OPERATIONS_EXT


def _strip(key: str) -> str:
    return os.getenv(key, "").strip()


def _followers_list() -> list[str]:
    raw = _strip("FOLLOWER_DB_HOST") or _strip("MYSQL_READER_HOSTS")
    if raw:
        return [h.strip() for h in raw.split(",") if h.strip()]
    single = _strip("MYSQL_READER_HOST")
    return [single] if single else []


# Canonical names + legacy MYSQL_* aliases (same objects)
LEADER_DB_HOST = (
    _strip("LEADER_DB_HOST")
    or _strip("MYSQL_WRITER_HOST")
    or _strip("MYSQL_HOST")
    or "127.0.0.1"
)

FOLLOWER_DB_HOSTS: list[str] = _followers_list()

DB_PORT = int(os.getenv("DB_PORT") or os.getenv("MYSQL_PORT") or "3306")
DB_NAME = _strip("DB_NAME") or _strip("MYSQL_DATABASE") or "tmitch23"
DB_USER = _strip("DB_USER") or _strip("MYSQL_USER") or "root"
DB_PASSWORD = os.getenv("DB_PASSWORD", os.getenv("MYSQL_PASSWORD", ""))

MYSQL_WRITER_HOST = LEADER_DB_HOST
MYSQL_HOST = LEADER_DB_HOST  # oldest alias
MYSQL_READER_HOSTS = FOLLOWER_DB_HOSTS
MYSQL_PORT = DB_PORT
MYSQL_DATABASE = DB_NAME
MYSQL_USER = DB_USER
MYSQL_PASSWORD = DB_PASSWORD


def pick_reader_host() -> str:
    """Replica chosen at random; if none configured, use leader."""
    if not FOLLOWER_DB_HOSTS:
        return LEADER_DB_HOST
    return random.choice(FOLLOWER_DB_HOSTS)


def mysql_connect_host(
    host: str,
    *,
    database: str | None,
    autocommit: bool,
) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=host,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
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
    """DDL runs only against the leader (writer)."""
    raw = _load_mysql_ddl()
    statements = _split_mysql_statements(raw)

    conn = mysql_connect_host(LEADER_DB_HOST, database=None, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        conn.select_db(DB_NAME)
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
            if MYSQL_OPERATIONS_EXT_PATH.is_file():
                ext = MYSQL_OPERATIONS_EXT_PATH.read_text(encoding="utf-8")
                for stmt in _split_mysql_statements(ext):
                    cur.execute(stmt)
    finally:
        conn.close()


@contextmanager
def get_connection_writer() -> Generator[pymysql.connections.Connection, None, None]:
    """Leader / primary — mutations and strongly consistent reads."""
    conn = mysql_connect_host(LEADER_DB_HOST, database=DB_NAME, autocommit=False)
    try:
        yield conn
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()


@contextmanager
def get_connection_reader() -> Generator[pymysql.connections.Connection, None, None]:
    """Follower pool — eventually consistent SELECTs only."""
    host = pick_reader_host()
    conn = mysql_connect_host(host, database=DB_NAME, autocommit=False)
    try:
        yield conn
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()


@contextmanager
def get_connection() -> Generator[pymysql.connections.Connection, None, None]:
    """Alias for :func:`get_connection_writer` (backward compatible default)."""
    with get_connection_writer() as conn:
        yield conn


def get_db_writer() -> Generator[pymysql.connections.Connection, None, None]:
    """FastAPI dependency: one writer connection per request."""
    with get_connection_writer() as conn:
        yield conn


def get_db_reader() -> Generator[pymysql.connections.Connection, None, None]:
    """FastAPI dependency: one reader connection per request."""
    with get_connection_reader() as conn:
        yield conn


def get_db() -> Generator[pymysql.connections.Connection, None, None]:
    """FastAPI dependency — writer (matches historical ``get_db`` behavior)."""
    with get_connection_writer() as conn:
        yield conn


def get_cursor_dict(conn: pymysql.connections.Connection):
    return conn.cursor(DictCursor)
