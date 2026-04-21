import os
import random
from contextlib import contextmanager
from typing import Any, Generator

import mysql.connector


def _strip_env(name: str) -> str:
    return os.getenv(name, "").strip()


# Canonical names first, then legacy single-host MYSQL_* fallback.
LEADER_DB_HOST = (
    _strip_env("LEADER_DB_HOST")
    or _strip_env("MYSQL_WRITER_HOST")
    or _strip_env("MYSQL_HOST")
    or "127.0.0.1"
)
FOLLOWER_DB_HOST_RAW = _strip_env("FOLLOWER_DB_HOST") or _strip_env("MYSQL_READER_HOSTS")
if FOLLOWER_DB_HOST_RAW:
    FOLLOWER_DB_HOSTS = [h.strip() for h in FOLLOWER_DB_HOST_RAW.split(",") if h.strip()]
else:
    single = _strip_env("MYSQL_READER_HOST")
    FOLLOWER_DB_HOSTS = [single] if single else []

MYSQL_PORT = int(os.getenv("DB_PORT") or os.getenv("MYSQL_PORT") or "3306")
MYSQL_USER = _strip_env("DB_USER") or _strip_env("MYSQL_USER") or "polarisDB"
MYSQL_PASSWORD = os.getenv("DB_PASSWORD", os.getenv("MYSQL_PASSWORD", ""))
MYSQL_DATABASE = _strip_env("DB_NAME") or _strip_env("MYSQL_DATABASE") or "polarisDB"


def _mysql_connect_host(host: str):
    return mysql.connector.connect(
        host=host,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
    )


@contextmanager
def get_connection_writer() -> Generator[Any, None, None]:
    conn = _mysql_connect_host(LEADER_DB_HOST)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_connection_reader() -> Generator[Any, None, None]:
    host = random.choice(FOLLOWER_DB_HOSTS) if FOLLOWER_DB_HOSTS else LEADER_DB_HOST
    conn = _mysql_connect_host(host)
    try:
        yield conn
    finally:
        conn.close()


def get_db_writer() -> Generator[Any, None, None]:
    with get_connection_writer() as conn:
        yield conn


def get_db_reader() -> Generator[Any, None, None]:
    with get_connection_reader() as conn:
        yield conn