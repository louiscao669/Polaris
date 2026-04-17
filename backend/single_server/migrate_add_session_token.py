#!/usr/bin/env python3
"""
Add user_session.session_token (+ unique index) using the same .env as the API.

Run from this directory (or anywhere, paths are resolved):

  cd backend/single_server
  python migrate_add_session_token.py

Optional if ALTER fails because of existing rows (drops all sessions):

  python migrate_add_session_token.py --truncate-sessions

Requires: mysql-connector-python, python-dotenv (see requirements.txt)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent

try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

import mysql.connector


def _has_column(cursor, table: str, column: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        LIMIT 1
        """,
        (table, column),
    )
    return cursor.fetchone() is not None


def main() -> int:
    parser = argparse.ArgumentParser(description="Add session_token column to user_session.")
    parser.add_argument(
        "--truncate-sessions",
        action="store_true",
        help="TRUNCATE user_session before migrating (only if NOT NULL ADD fails on existing rows).",
    )
    args = parser.parse_args()

    import os

    host = os.getenv("MYSQL_HOST")
    if not host:
        print(
            "MYSQL_HOST is not set. Put variables in backend/single_server/.env "
            "or export MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE.",
            file=sys.stderr,
        )
        return 1

    try:
        conn = mysql.connector.connect(
            host=host.strip(),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DATABASE", "tmitch23"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", ""),
            connection_timeout=15,
        )
    except mysql.connector.Error as e:
        print(f"Could not connect to MySQL: {e}", file=sys.stderr)
        return 1

    cur = conn.cursor(buffered=True)
    try:
        if _has_column(cur, "user_session", "session_token"):
            print("Column user_session.session_token already exists — nothing to do.")
            return 0

        if args.truncate_sessions:
            cur.execute("TRUNCATE TABLE user_session")
            conn.commit()
            print("Truncated user_session (requested).")

        cur.execute(
            """
            ALTER TABLE `user_session`
              ADD COLUMN `session_token` varchar(64) NOT NULL AFTER `user_id`,
              ADD UNIQUE KEY `uq_user_session_token` (`session_token`)
            """
        )
        conn.commit()
        print("Migration OK: added session_token and unique key uq_user_session_token.")
        return 0
    except mysql.connector.Error as e:
        conn.rollback()
        print(f"Migration failed: {e}", file=sys.stderr)
        print(
            "If MySQL complains about NOT NULL and existing rows, run again with:\n"
            "  python migrate_add_session_token.py --truncate-sessions\n"
            "(That deletes all rows in user_session; fine for dev.)",
            file=sys.stderr,
        )
        return 1
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
