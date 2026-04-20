"""Idempotency rows for v2 Kafka consumers."""

from __future__ import annotations

from typing import Literal

import pymysql.err

from .database import get_connection

Outcome = Literal["skip_done", "run"]


def classify_message(
    *,
    event_id: str,
    consumer_group: str,
    topic: str,
    partition: int,
    offset: int,
) -> Outcome:
    """Decide whether to skip or run sync handler."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status FROM processed_events
            WHERE event_id = %s AND consumer_group = %s
            """,
            (event_id, consumer_group),
        )
        row = cur.fetchone()
        if row and row[0] in ("applied", "skipped_duplicate"):
            conn.commit()
            return "skip_done"
        try:
            cur.execute(
                """
                INSERT INTO processed_events
                  (event_id, consumer_group, topic, kafka_partition, kafka_offset, status)
                VALUES (%s, %s, %s, %s, %s, 'received')
                """,
                (event_id, consumer_group, topic, partition, offset),
            )
            conn.commit()
            return "run"
        except pymysql.err.IntegrityError as e:
            conn.rollback()
            err_text = str(e.args)
            if "uniq_consumer_partition_offset" in err_text:
                return "skip_done"
            cur.execute(
                """
                SELECT status FROM processed_events
                WHERE event_id = %s AND consumer_group = %s
                """,
                (event_id, consumer_group),
            )
            row2 = cur.fetchone()
            if row2 and row2[0] in ("applied", "skipped_duplicate"):
                return "skip_done"
            return "run"


def mark_applied(*, event_id: str, consumer_group: str) -> None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE processed_events
            SET status = 'applied', processed_at = CURRENT_TIMESTAMP, error_message = NULL
            WHERE event_id = %s AND consumer_group = %s
            """,
            (event_id, consumer_group),
        )
        conn.commit()


def mark_failed(
    *,
    event_id: str,
    consumer_group: str,
    error_message: str,
) -> None:
    msg = error_message[:65535]
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE processed_events
            SET status = 'failed', processed_at = CURRENT_TIMESTAMP, error_message = %s
            WHERE event_id = %s AND consumer_group = %s
            """,
            (msg, event_id, consumer_group),
        )
        conn.commit()
