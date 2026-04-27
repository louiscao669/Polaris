"""Persist async operation lifecycle rows (MySQL on writer/leader)."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from pymysql.cursors import DictCursor

from .database import get_connection_reader, get_connection_writer


def insert_operation_pending(
    *,
    operation_id: UUID,
    topic: str,
    envelope: dict[str, Any],
) -> None:
    env_json = json.dumps(envelope, default=str)
    with get_connection_writer() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO operations
                (operation_id, topic, status, envelope_json)
            VALUES (%s, %s, 'queued', %s)
            """,
            (str(operation_id), topic, env_json),
        )
        conn.commit()


def update_operation_kafka_meta(
    *,
    operation_id: UUID,
    partition: int,
    offset: int,
) -> None:
    with get_connection_writer() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE operations
            SET kafka_partition = %s, kafka_offset = %s, status = 'queued'
            WHERE operation_id = %s
            """,
            (partition, offset, str(operation_id)),
        )
        conn.commit()


def update_operation_status(
    *,
    operation_id: UUID,
    status: str,
    error_message: str | None = None,
    result: Any | None = None,
) -> None:
    msg = error_message[:65535] if error_message else None
    result_json = json.dumps(result, default=str) if result is not None else None
    with get_connection_writer() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE operations
            SET status = %s, error_message = %s, result_json = %s
            WHERE operation_id = %s
            """,
            (status, msg, result_json, str(operation_id)),
        )
        conn.commit()


def fetch_operation(
    operation_id: UUID, *, use_writer: bool = True
) -> dict[str, Any] | None:
    """``use_writer=True`` (default) hits the primary for read-your-writes polling.

    Set ``use_writer=False`` to read from a replica (eventual consistency), e.g. when
    the client sends ``X-Force-Leader: false``.
    """
    ctx = get_connection_writer if use_writer else get_connection_reader
    with ctx() as conn:
        cur = conn.cursor(DictCursor)
        cur.execute(
            """
            SELECT operation_id, topic, status, envelope_json, result_json,
                   error_message, kafka_partition, kafka_offset,
                   created_at, updated_at
            FROM operations
            WHERE operation_id = %s
            """,
            (str(operation_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        out = dict(row)
        if out.get("envelope_json"):
            try:
                out["envelope"] = json.loads(out["envelope_json"])
            except json.JSONDecodeError:
                out["envelope"] = None
        if out.get("result_json"):
            try:
                out["result"] = json.loads(out["result_json"])
            except json.JSONDecodeError:
                out["result"] = None
        out.pop("envelope_json", None)
        out.pop("result_json", None)
        return out


def use_writer_for_operation_fetch(x_force_leader: str | None) -> bool:
    """Map ``X-Force-Leader`` to writer vs replica (default: writer / primary)."""
    if x_force_leader is None or not str(x_force_leader).strip():
        return True
    v = str(x_force_leader).strip().lower()
    if v in {"0", "false", "no", "off"}:
        return False
    return True
