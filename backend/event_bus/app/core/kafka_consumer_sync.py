"""Synchronous MySQL apply logic for Kafka messages (run via asyncio.to_thread)."""

from __future__ import annotations

from typing import Any

from backend.app.database import get_connection


def _rows(cursor) -> list[list[Any]]:
    rows = cursor.fetchall()
    if not rows:
        return []
    return [[*r] if isinstance(r, (list, tuple)) else r for r in rows]


def sync_create_o(data: dict[str, Any]) -> None:
    oid = int(data["organization_id"])
    name = data["name"]
    description = data["description"]
    user_id = int(data["user_id"])
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO organization (org_id, name, description)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description)
            """,
            (oid, name, description),
        )
        cur.execute(
            """
            INSERT INTO organization_leader (org_id, user_id)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE user_id = VALUES(user_id)
            """,
            (oid, user_id),
        )
        conn.commit()


def sync_create_o_role(data: dict[str, Any]) -> None:
    oid = int(data["organization_id"])
    role = data["name"]
    desc = data["desc"]
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO organization_role (org_id, role, description)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE description = VALUES(description)
            """,
            (oid, role, desc),
        )
        conn.commit()


def sync_create_o_token(data: dict[str, Any]) -> None:
    oid = int(data["organization_id"])
    tid = int(data["token_id"])
    tname = data["token_name"]
    desc = data.get("description")
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO organization_token (token_id, org_id, name, description)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description)
            """,
            (tid, oid, tname, desc),
        )
        conn.commit()


def sync_create_e(data: dict[str, Any]) -> None:
    event_id = int(data["event_id"])
    organization_id = int(data["organization_id"])
    user_id = int(data["user_id"])
    caption = data["caption"]
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM organization_leader
            WHERE org_id = %s AND user_id = %s
            """,
            (organization_id, user_id),
        )
        if cur.fetchone() is None:
            print(
                f"sync_create_e skipped (permission): user={user_id} org={organization_id}"
            )
            return
        cur.execute(
            """
            INSERT INTO events (event_id, org_id, caption, is_open)
            VALUES (%s, %s, %s, TRUE)
            ON DUPLICATE KEY UPDATE org_id = VALUES(org_id), caption = VALUES(caption)
            """,
            (event_id, organization_id, caption),
        )
        conn.commit()


def sync_designate_e_token(data: dict[str, Any]) -> None:
    event_id = int(data["event_id"])
    token_id = int(data["token_id"])
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT IGNORE INTO event_tokens_allowed (event_id, token_id)
            VALUES (%s, %s)
            """,
            (event_id, token_id),
        )
        conn.commit()


def sync_designate_e_market_creator(data: dict[str, Any]) -> None:
    event_id = int(data["event_id"])
    user_id = int(data["market_creator_id"])
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT IGNORE INTO event_market_creators (event_id, user_id)
            VALUES (%s, %s)
            """,
            (event_id, user_id),
        )
        conn.commit()


def sync_designate_e_contraint(data: dict[str, Any]) -> None:
    event_id = int(data["event_id"])
    constraint_id = int(data["constraint_id"])
    value = int(data["value"])
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO event_constraints (event_id, constraint_id, constraint_value)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE constraint_value = VALUES(constraint_value)
            """,
            (event_id, constraint_id, value),
        )
        conn.commit()


def sync_designate_e_open_to(data: dict[str, Any]) -> None:
    event_id = int(data["event_id"])
    role_id = str(data["role_id"])
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT org_id FROM events WHERE event_id = %s", (event_id,))
        row = cur.fetchone()
        if row is None:
            print(f"sync_designate_e_open_to skipped: unknown event_id={event_id}")
            return
        organization_id = row[0]
        cur.execute(
            """
            INSERT INTO event_open_to (event_id, org_id, role_id)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE org_id = VALUES(org_id)
            """,
            (event_id, organization_id, role_id),
        )
        conn.commit()


def sync_designate_e_closed(data: dict[str, Any]) -> None:
    event_id = int(data["event_id"])
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE events SET is_open = FALSE WHERE event_id = %s",
            (event_id,),
        )
        conn.commit()


def sync_create_m(data: dict[str, Any]) -> None:
    market_id = int(data["market_id"])
    event_id = int(data["event_id"])
    user_id = int(data["user_id"])
    question = data["question"]
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO market (id, event_id, question, created_by, is_open)
            VALUES (%s, %s, %s, %s, TRUE)
            ON DUPLICATE KEY UPDATE question = VALUES(question), event_id = VALUES(event_id)
            """,
            (market_id, event_id, question, user_id),
        )
        conn.commit()


def sync_designate_m_token(data: dict[str, Any]) -> None:
    market_id = int(data["market_id"])
    token_id = int(data["token_id"])
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT IGNORE INTO market_tokens_allowed (market_id, token_id)
            VALUES (%s, %s)
            """,
            (market_id, token_id),
        )
        conn.commit()


def sync_designate_m_result(data: dict[str, Any]) -> None:
    market_id = int(data["market_id"])
    result = data["result"]
    normalized = bool(result) if result not in (0, 1) else bool(int(result))
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO market_result (market_id, outcome)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE outcome = VALUES(outcome)
            """,
            (market_id, normalized),
        )
        cur.execute(
            """
            UPDATE market SET is_open = FALSE, close_at = NOW() WHERE id = %s
            """,
            (market_id,),
        )
        conn.commit()


def sync_designate_m_contraint(data: dict[str, Any]) -> None:
    market_id = int(data["market_id"])
    constraint_id = int(data["constraint_id"])
    value = int(data["value"])
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO market_constraint (constraint_id, market_id, constraint_value)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE constraint_value = VALUES(constraint_value)
            """,
            (constraint_id, market_id, value),
        )
        conn.commit()


def sync_designate_m_open_to_as(data: dict[str, Any]) -> None:
    market_id = int(data["market_id"])
    role_id = str(data["role_id"])
    as_id = str(data["as_id"])
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT e.org_id
            FROM market m
            JOIN events e ON m.event_id = e.event_id
            WHERE m.id = %s
            """,
            (market_id,),
        )
        row = cur.fetchone()
        if row is None:
            print(f"sync_designate_m_open_to_as skipped: unknown market_id={market_id}")
            return
        organization_id = row[0]
        cur.execute(
            """
            INSERT INTO market_open_to_as (market_id, org_id, role_id, as_id)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE as_id = VALUES(as_id)
            """,
            (market_id, organization_id, role_id, as_id),
        )
        conn.commit()


def sync_do_m_transaction(data: dict[str, Any]) -> None:
    user_id = int(data["user_id"])
    market_id = int(data["market_id"])
    token_id = int(data["token_id"])
    price = int(data["type"])
    side = bool(data["side"])
    qty = int(data["qty"])
    transaction_id = int(data["transaction_id"])
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1 FROM market_transaction
            WHERE market_id = %s AND transaction_id = %s
            """,
            (market_id, transaction_id),
        )
        if cur.fetchone() is not None:
            return
        cur.execute(
            """
            INSERT INTO market_transaction
            (transaction_id, market_id, token_id, amt, user_id, price, side)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (transaction_id, market_id, token_id, qty, user_id, price, side),
        )
        cur.execute(
            """
            INSERT INTO user_token_stock (token_id, user_id, qty)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)
            """,
            (token_id, user_id, qty),
        )
        cur.execute(
            """
            INSERT INTO user_market_shares (user_id, market_id, outcome, shares, avg_price)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                shares = shares + VALUES(shares),
                avg_price = VALUES(avg_price)
            """,
            (user_id, market_id, side, qty, price),
        )
        cur.execute(
            """
            INSERT INTO market_price_snapshot (market_id, outcome_id, price, liquidity)
            VALUES (%s, %s, %s, %s)
            """,
            (market_id, side, price, qty),
        )
        conn.commit()


def sync_do_m_payout(data: dict[str, Any]) -> None:
    user_id = int(data["user_id"])
    market_id = int(data["market_id"])
    token_id = int(data["token_id"])
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT mr.outcome
            FROM market m
            JOIN market_result mr ON m.id = mr.market_id
            WHERE m.id = %s AND m.is_open = FALSE
            """,
            (market_id,),
        )
        result_row = cur.fetchone()
        if result_row is None:
            print(f"sync_do_m_payout skipped: market {market_id} not resolved/closed")
            return
        winning_outcome = result_row[0]
        cur.execute(
            """
            SELECT user_id, shares
            FROM user_market_shares
            WHERE market_id = %s AND outcome = %s AND shares > 0
            """,
            (market_id, winning_outcome),
        )
        winners = cur.fetchall()
        for winner_user_id, shares in winners:
            cur.execute(
                """
                INSERT INTO user_token_stock (token_id, user_id, qty)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)
                """,
                (token_id, winner_user_id, shares),
            )
        conn.commit()


def _stats_permission(cur, user_id: int, market_id: int) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM market m
        JOIN events e ON m.event_id = e.event_id
        LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
        LEFT JOIN market_open_to_as mota ON m.id = mota.market_id
        LEFT JOIN user_org_role uor ON mota.org_id = uor.org_id
            AND mota.role_id = uor.role_id AND uor.user_id = %s
        WHERE m.id = %s
          AND (ol.user_id IS NOT NULL OR uor.user_id IS NOT NULL OR m.created_by = %s)
        """,
        (user_id, user_id, market_id, user_id),
    )
    return cur.fetchone() is not None


def sync_stats_m_liquidity(data: dict[str, Any]) -> None:
    user_id = int(data["user_id"])
    market_id = int(data["market_id"])
    with get_connection() as conn:
        cur = conn.cursor()
        if not _stats_permission(cur, user_id, market_id):
            print(f"sync_stats_m_liquidity skipped (permission): user={user_id}")
            return
        cur.execute(
            """
            SELECT COALESCE(SUM(liquidity), 0)
            FROM market_price_snapshot
            WHERE market_id = %s
            """,
            (market_id,),
        )
        print(f"STATS_LIQUIDITY market={market_id} liquidity={cur.fetchone()[0]}")


def sync_stats_m_time_focus(data: dict[str, Any]) -> None:
    user_id = int(data["user_id"])
    market_id = int(data["market_id"])
    with get_connection() as conn:
        cur = conn.cursor()
        if not _stats_permission(cur, user_id, market_id):
            print(f"sync_stats_m_time_focus skipped (permission): user={user_id}")
            return
        cur.execute(
            """
            SELECT ts, price, liquidity
            FROM market_price_snapshot
            WHERE market_id = %s
            ORDER BY ts DESC
            LIMIT 10
            """,
            (market_id,),
        )
        print(f"STATS_TIME_FOCUS market={market_id} rows={_rows(cur)}")


def sync_stats_m_whales(data: dict[str, Any]) -> None:
    user_id = int(data["user_id"])
    market_id = int(data["market_id"])
    with get_connection() as conn:
        cur = conn.cursor()
        if not _stats_permission(cur, user_id, market_id):
            print(f"sync_stats_m_whales skipped (permission): user={user_id}")
            return
        cur.execute(
            """
            SELECT user_id, SUM(shares) AS total_shares
            FROM user_market_shares
            WHERE market_id = %s
            GROUP BY user_id
            ORDER BY total_shares DESC
            LIMIT 5
            """,
            (market_id,),
        )
        print(f"STATS_WHALES market={market_id} rows={_rows(cur)}")


def sync_points_m(data: dict[str, Any]) -> None:
    user_id = int(data["user_id"])
    market_id = int(data["market_id"])
    span = int(data["span"])
    with get_connection() as conn:
        cur = conn.cursor()
        if not _stats_permission(cur, user_id, market_id):
            print(f"sync_points_m skipped (permission): user={user_id}")
            return
        cur.execute(
            """
            SELECT ts, outcome_id, price, liquidity
            FROM market_price_snapshot
            WHERE market_id = %s
            ORDER BY ts DESC
            LIMIT %s
            """,
            (market_id, span),
        )
        print(f"POINTS market={market_id} rows={_rows(cur)}")


def sync_user_account_message(data: dict[str, Any]) -> None:
    """Apply user.account domain messages (extend as handlers are added)."""
    action = data.get("action")
    if action == "TEST_PING":
        return
    raise ValueError(f"unsupported user.account action: {action!r}")
