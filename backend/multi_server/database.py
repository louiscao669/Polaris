import os
import re
from collections.abc import Generator
from contextlib import contextmanager
import pymysql
from pymysql.cursors import DictCursor

from models import MYSQL_DDL_PATH

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
    """Create database if needed, then run DDL from Database_Visualized/mysql_instantiation.txt."""
    raw = _load_mysql_ddl()
    statements = _split_mysql_statements(raw)

    conn = _mysql_connect(database=None, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        conn.select_db(MYSQL_DATABASE)
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
    finally:
        conn.close()


@contextmanager
def get_connection() -> Generator[pymysql.connections.Connection, None, None]:
    conn = _mysql_connect(database=MYSQL_DATABASE, autocommit=False)
    try:
        yield conn
    finally:
        # End any open transaction (success paths already committed; failures did not).
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
    """Raw SQL SELECTs as dict rows."""
    return conn.cursor(DictCursor)
