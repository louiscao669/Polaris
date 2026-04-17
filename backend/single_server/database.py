import os
import mysql.connector
from contextlib import contextmanager
from typing import Any, Generator

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "tmitch23")

def _mysql_connect():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
    )

@contextmanager
def get_connection() -> Generator[Any, None, None]:
    conn = _mysql_connect()
    try:
        yield conn
    finally:
        conn.close()

def get_db() -> Generator[Any, None, None]:
    with get_connection() as conn:
        yield conn