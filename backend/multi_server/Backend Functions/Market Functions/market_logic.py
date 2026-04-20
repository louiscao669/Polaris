"""Market domain logic. Expected failures are returned as dicts, not raised."""

from __future__ import annotations

from typing import Any


def _ok(**extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": True}
    out.update(extra)
    return out


def _fail(error: str, message: str) -> dict[str, Any]:
    """
    error: permission | duplicate | validation | precondition |
           not_open (market not tradeable) | not_closed (payout not ready)
    """
    return {"ok": False, "error": error, "message": message}


def _rows(cursor) -> list[list[Any]]:
    rows = cursor.fetchall()
    if not rows:
        return []
    return [[*r] if isinstance(r, (list, tuple)) else r for r in rows]


def create_m(cursor, db, user_id, event_id, question, description):
    del description  # reserved for future use
    cursor.execute(
        """
        SELECT e.org_id
        FROM events e
        LEFT JOIN event_market_creators emc ON e.event_id = emc.event_id AND emc.user_id = %s
        LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
        WHERE e.event_id = %s AND (emc.user_id IS NOT NULL OR ol.user_id IS NOT NULL)
        """,
        (user_id, user_id, event_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} cannot create a market for event {event_id}.",
        )

    cursor.execute(
        """
        SELECT id
        FROM market
        WHERE event_id = %s AND question = %s
        """,
        (event_id, question),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"A market with this question already exists for event {event_id}.",
        )

    cursor.execute(
        """
        INSERT INTO market (event_id, question, created_by)
        VALUES (%s, %s, %s)
        """,
        (event_id, question, user_id),
    )
    db.commit()

    return _ok(market_id=cursor.lastrowid)


def designate_m_token(cursor, db, user_id, market_id, token_id):
    cursor.execute(
        """
        SELECT e.org_id
        FROM market m
        JOIN events e ON m.event_id = e.event_id
        LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
        WHERE m.id = %s AND (m.created_by = %s OR ol.user_id IS NOT NULL)
        """,
        (user_id, market_id, user_id),
    )
    market = cursor.fetchone()
    if market is None:
        return _fail(
            "permission",
            f"User {user_id} cannot designate tokens for market {market_id}.",
        )

    organization_id = market[0]

    cursor.execute(
        """
        SELECT 1
        FROM organization_token
        WHERE token_id = %s AND org_id = %s
        """,
        (token_id, organization_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "validation",
            f"Token {token_id} is invalid for this organization.",
        )

    cursor.execute(
        """
        SELECT 1
        FROM market_tokens_allowed
        WHERE market_id = %s AND token_id = %s
        """,
        (market_id, token_id),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"Token {token_id} is already allowed for market {market_id}.",
        )

    cursor.execute(
        """
        INSERT INTO market_tokens_allowed (market_id, token_id)
        VALUES (%s, %s)
        """,
        (market_id, token_id),
    )
    db.commit()

    return _ok()


def designate_m_result(cursor, db, user_id, market_id, result):
    cursor.execute(
        """
        SELECT e.org_id
        FROM market m
        JOIN events e ON m.event_id = e.event_id
        LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
        WHERE m.id = %s AND (m.created_by = %s OR ol.user_id IS NOT NULL)
        """,
        (user_id, market_id, user_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} cannot set the result for market {market_id}.",
        )

    if result not in (True, False, 0, 1):
        return _fail("validation", f"Result {result!r} is not valid (use boolean or 0/1).")

    cursor.execute(
        """
        SELECT 1
        FROM market_result
        WHERE market_id = %s
        """,
        (market_id,),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"Market {market_id} already has a recorded result.",
        )

    normalized_result = bool(result)
    cursor.execute(
        """
        INSERT INTO market_result (market_id, outcome)
        VALUES (%s, %s)
        """,
        (market_id, normalized_result),
    )
    # Close market before payout so do_m_payout's query (is_open = FALSE) succeeds
    cursor.execute(
        """
        UPDATE market
        SET is_open = FALSE, close_at = NOW()
        WHERE id = %s
        """,
        (market_id,),
    )

    cursor.execute(
        """
        SELECT token_id
        FROM market_tokens_allowed
        WHERE market_id = %s
        """,
        (market_id,),
    )
    token_rows = cursor.fetchall()
    for token_row in token_rows:
        payout = do_m_payout(cursor, db, user_id, market_id, token_row[0])
        if isinstance(payout, dict) and payout.get("ok") is False:
            return payout

    db.commit()

    return _ok()


def designate_m_contraint(cursor, db, user_id, market_id, constraint_id, value):
    cursor.execute(
        """
        SELECT e.org_id
        FROM market m
        JOIN events e ON m.event_id = e.event_id
        LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
        WHERE m.id = %s AND (m.created_by = %s OR ol.user_id IS NOT NULL)
        """,
        (user_id, market_id, user_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} cannot set constraints for market {market_id}.",
        )

    cursor.execute(
        """
        SELECT 1
        FROM constraint_type
        WHERE constraint_id = %s
        """,
        (constraint_id,),
    )
    if cursor.fetchone() is None:
        return _fail("validation", f"Constraint {constraint_id} is not valid.")

    cursor.execute(
        """
        SELECT 1
        FROM market_constraint
        WHERE market_id = %s AND constraint_id = %s
        """,
        (market_id, constraint_id),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"Constraint {constraint_id} is already set for market {market_id}.",
        )

    cursor.execute(
        """
        INSERT INTO market_constraint (constraint_id, market_id, constraint_value)
        VALUES (%s, %s, %s)
        """,
        (constraint_id, market_id, value),
    )
    db.commit()

    return _ok()


def designate_m_open_to_as(cursor, db, user_id, market_id, role_id, as_id):
    cursor.execute(
        """
        SELECT e.org_id
        FROM market m
        JOIN events e ON m.event_id = e.event_id
        LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
        WHERE m.id = %s AND (m.created_by = %s OR ol.user_id IS NOT NULL)
        """,
        (user_id, market_id, user_id),
    )
    market = cursor.fetchone()
    if market is None:
        return _fail(
            "permission",
            f"User {user_id} cannot configure open-to-AS for market {market_id}.",
        )

    organization_id = market[0]

    cursor.execute(
        """
        SELECT 1
        FROM organization_role
        WHERE org_id = %s AND role = %s
        """,
        (organization_id, role_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "validation",
            f"Role {role_id!r} is not valid for this organization.",
        )

    cursor.execute(
        """
        SELECT 1
        FROM market_as
        WHERE as_code = %s
        """,
        (as_id,),
    )
    if cursor.fetchone() is None:
        return _fail("validation", f"AS code {as_id!r} is not valid.")

    cursor.execute(
        """
        SELECT 1
        FROM market_open_to_as
        WHERE market_id = %s AND org_id = %s AND role_id = %s
        """,
        (market_id, organization_id, role_id),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"This role is already configured for market {market_id} (same org/role row).",
        )

    cursor.execute(
        """
        INSERT INTO market_open_to_as (market_id, org_id, role_id, as_id)
        VALUES (%s, %s, %s, %s)
        """,
        (market_id, organization_id, role_id, as_id),
    )
    db.commit()

    return _ok()


def do_m_transaction(cursor, db, user_id, market_id, token_id, type, side, qty):
    cursor.execute(
        """
        SELECT e.org_id
        FROM market m
        JOIN events e ON m.event_id = e.event_id
        LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
        LEFT JOIN market_open_to_as mota ON m.id = mota.market_id
        LEFT JOIN user_org_role uor ON mota.org_id = uor.org_id AND mota.role_id = uor.role_id AND uor.user_id = %s
        WHERE m.id = %s AND (ol.user_id IS NOT NULL OR uor.user_id IS NOT NULL)
        """,
        (user_id, user_id, market_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} cannot trade in market {market_id}.",
        )

    cursor.execute(
        """
        SELECT 1
        FROM market
        WHERE id = %s AND is_open = TRUE
        """,
        (market_id,),
    )
    if cursor.fetchone() is None:
        return _fail("not_open", f"Market {market_id} is not open for trading.")

    cursor.execute(
        """
        SELECT 1
        FROM market_tokens_allowed
        WHERE market_id = %s AND token_id = %s
        """,
        (market_id, token_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "validation",
            f"Token {token_id} is not allowed for market {market_id}.",
        )

    if qty <= 0:
        return _fail("validation", f"Quantity must be positive (got {qty}).")

    cursor.execute(
        """
        SELECT COALESCE(MAX(transaction_id), 0) + 1
        FROM market_transaction
        WHERE market_id = %s
        """,
        (market_id,),
    )
    transaction_id = cursor.fetchone()[0]

    price = int(type)
    normalized_side = bool(side)

    cursor.execute(
        """
        INSERT INTO market_transaction (transaction_id, market_id, token_id, amt, user_id, price, side)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (transaction_id, market_id, token_id, qty, user_id, price, normalized_side),
    )

    cursor.execute(
        """
        INSERT INTO user_token_stock (token_id, user_id, qty)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)
        """,
        (token_id, user_id, qty),
    )

    cursor.execute(
        """
        INSERT INTO user_market_shares (user_id, market_id, outcome, shares, avg_price)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE shares = shares + VALUES(shares), avg_price = VALUES(avg_price)
        """,
        (user_id, market_id, normalized_side, qty, price),
    )

    cursor.execute(
        """
        INSERT INTO market_price_snapshot (market_id, outcome_id, price, liquidity)
        VALUES (%s, %s, %s, %s)
        """,
        (market_id, normalized_side, price, qty),
    )
    db.commit()

    return _ok(transaction_id=transaction_id)


def do_m_payout(cursor, db, user_id, market_id, token_id):
    cursor.execute(
        """
        SELECT e.org_id
        FROM market m
        JOIN events e ON m.event_id = e.event_id
        LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
        WHERE m.id = %s AND (m.created_by = %s OR ol.user_id IS NOT NULL)
        """,
        (user_id, market_id, user_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} cannot run payout for market {market_id}.",
        )

    cursor.execute(
        """
        SELECT mr.outcome
        FROM market m
        JOIN market_result mr ON m.id = mr.market_id
        WHERE m.id = %s AND m.is_open = FALSE
        """,
        (market_id,),
    )
    result_row = cursor.fetchone()
    if result_row is None:
        return _fail(
            "not_closed",
            f"Market {market_id} must be resolved and closed before payout.",
        )

    cursor.execute(
        """
        SELECT 1
        FROM market_tokens_allowed
        WHERE market_id = %s AND token_id = %s
        """,
        (market_id, token_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "validation",
            f"Token {token_id} is not allowed for market {market_id}.",
        )

    winning_outcome = result_row[0]

    cursor.execute(
        """
        SELECT user_id, shares
        FROM user_market_shares
        WHERE market_id = %s AND outcome = %s AND shares > 0
        """,
        (market_id, winning_outcome),
    )
    winners = cursor.fetchall()

    for winner_user_id, shares in winners:
        cursor.execute(
            """
            INSERT INTO user_token_stock (token_id, user_id, qty)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)
            """,
            (token_id, winner_user_id, shares),
        )

    db.commit()

    return _ok()


def stats_m_liquidity(cursor, db, user_id, market_id):
    cursor.execute(
        """
        SELECT 1
        FROM market m
        JOIN events e ON m.event_id = e.event_id
        LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
        LEFT JOIN market_open_to_as mota ON m.id = mota.market_id
        LEFT JOIN user_org_role uor ON mota.org_id = uor.org_id AND mota.role_id = uor.role_id AND uor.user_id = %s
        WHERE m.id = %s AND (ol.user_id IS NOT NULL OR uor.user_id IS NOT NULL OR m.created_by = %s)
        """,
        (user_id, user_id, market_id, user_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} cannot view liquidity stats for market {market_id}.",
        )

    cursor.execute(
        """
        SELECT COALESCE(SUM(liquidity), 0)
        FROM market_price_snapshot
        WHERE market_id = %s
        """,
        (market_id,),
    )
    return _ok(liquidity=cursor.fetchone()[0])


def stats_m_time_focus(cursor, db, user_id, market_id):
    cursor.execute(
        """
        SELECT 1
        FROM market m
        JOIN events e ON m.event_id = e.event_id
        LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
        LEFT JOIN market_open_to_as mota ON m.id = mota.market_id
        LEFT JOIN user_org_role uor ON mota.org_id = uor.org_id AND mota.role_id = uor.role_id AND uor.user_id = %s
        WHERE m.id = %s AND (ol.user_id IS NOT NULL OR uor.user_id IS NOT NULL OR m.created_by = %s)
        """,
        (user_id, user_id, market_id, user_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} cannot view time-focus stats for market {market_id}.",
        )

    cursor.execute(
        """
        SELECT ts, price, liquidity
        FROM market_price_snapshot
        WHERE market_id = %s
        ORDER BY ts DESC
        LIMIT 10
        """,
        (market_id,),
    )
    return _ok(rows=_rows(cursor))


def stats_m_whales(cursor, db, user_id, market_id):
    cursor.execute(
        """
        SELECT 1
        FROM market m
        JOIN events e ON m.event_id = e.event_id
        LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
        LEFT JOIN market_open_to_as mota ON m.id = mota.market_id
        LEFT JOIN user_org_role uor ON mota.org_id = uor.org_id AND mota.role_id = uor.role_id AND uor.user_id = %s
        WHERE m.id = %s AND (ol.user_id IS NOT NULL OR uor.user_id IS NOT NULL OR m.created_by = %s)
        """,
        (user_id, user_id, market_id, user_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} cannot view whale stats for market {market_id}.",
        )

    cursor.execute(
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
    return _ok(rows=_rows(cursor))


def points_m(cursor, db, user_id, market_id, span):
    cursor.execute(
        """
        SELECT 1
        FROM market m
        JOIN events e ON m.event_id = e.event_id
        LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
        LEFT JOIN market_open_to_as mota ON m.id = mota.market_id
        LEFT JOIN user_org_role uor ON mota.org_id = uor.org_id AND mota.role_id = uor.role_id AND uor.user_id = %s
        WHERE m.id = %s AND (ol.user_id IS NOT NULL OR uor.user_id IS NOT NULL OR m.created_by = %s)
        """,
        (user_id, user_id, market_id, user_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} cannot view points for market {market_id}.",
        )

    cursor.execute(
        """
        SELECT ts, outcome_id, price, liquidity
        FROM market_price_snapshot
        WHERE market_id = %s
        ORDER BY ts DESC
        LIMIT %s
        """,
        (market_id, span),
    )
    return _ok(rows=_rows(cursor))
