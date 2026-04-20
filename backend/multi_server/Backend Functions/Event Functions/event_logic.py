"""Event domain logic. Expected failures are returned as dicts, not raised."""

from __future__ import annotations

from typing import Any


def _ok(**extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": True}
    out.update(extra)
    return out


def _fail(error: str, message: str) -> dict[str, Any]:
    """error: permission | duplicate | validation | precondition"""
    return {"ok": False, "error": error, "message": message}


def create_e(cursor, db, user_id, organization_id, caption):
    cursor.execute(
        """
        SELECT 1
        FROM organization_leader
        WHERE org_id = %s AND user_id = %s
        """,
        (organization_id, user_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} cannot create events for this organization.",
        )

    cursor.execute(
        """
        SELECT event_id
        FROM events
        WHERE org_id = %s AND caption = %s
        """,
        (organization_id, caption),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"An event with caption '{caption}' already exists in this organization.",
        )

    cursor.execute(
        """
        INSERT INTO events (org_id, caption)
        VALUES (%s, %s)
        """,
        (organization_id, caption),
    )
    db.commit()

    return _ok(event_id=cursor.lastrowid)


def designate_e_token(cursor, db, user_id, event_id, token_id):
    cursor.execute(
        """
        SELECT e.org_id
        FROM events e
        JOIN organization_leader ol ON e.org_id = ol.org_id
        WHERE e.event_id = %s AND ol.user_id = %s
        """,
        (event_id, user_id),
    )
    event = cursor.fetchone()
    if event is None:
        return _fail(
            "permission",
            f"User {user_id} cannot designate tokens for this event.",
        )

    organization_id = event[0]

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
            f"Token {token_id} is invalid or does not belong to this organization.",
        )

    cursor.execute(
        """
        SELECT 1
        FROM event_tokens_allowed
        WHERE event_id = %s AND token_id = %s
        """,
        (event_id, token_id),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"Token {token_id} is already designated for this event.",
        )

    cursor.execute(
        """
        INSERT INTO event_tokens_allowed (event_id, token_id)
        VALUES (%s, %s)
        """,
        (event_id, token_id),
    )
    db.commit()

    return _ok()


def designate_e_market_creator(cursor, db, user_id, event_id, market_creator_id):
    cursor.execute(
        """
        SELECT e.org_id
        FROM events e
        JOIN organization_leader ol ON e.org_id = ol.org_id
        WHERE e.event_id = %s AND ol.user_id = %s
        """,
        (event_id, user_id),
    )
    event = cursor.fetchone()
    if event is None:
        return _fail(
            "permission",
            f"User {user_id} cannot designate market creators for this event.",
        )

    organization_id = event[0]

    cursor.execute(
        """
        SELECT 1
        FROM user_org_role
        WHERE org_id = %s AND user_id = %s
        """,
        (organization_id, market_creator_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "validation",
            f"User {market_creator_id} is not valid for this organization.",
        )

    cursor.execute(
        """
        SELECT 1
        FROM event_market_creators
        WHERE event_id = %s AND user_id = %s
        """,
        (event_id, market_creator_id),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"User {market_creator_id} is already a market creator for this event.",
        )

    cursor.execute(
        """
        INSERT INTO event_market_creators (event_id, user_id)
        VALUES (%s, %s)
        """,
        (event_id, market_creator_id),
    )
    db.commit()

    return _ok()


def designate_e_contraint(cursor, db, user_id, event_id, constraint_id, value):
    cursor.execute(
        """
        SELECT 1
        FROM events e
        JOIN organization_leader ol ON e.org_id = ol.org_id
        WHERE e.event_id = %s AND ol.user_id = %s
        """,
        (event_id, user_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} cannot designate constraints for this event.",
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
        FROM event_constraints
        WHERE event_id = %s AND constraint_id = %s
        """,
        (event_id, constraint_id),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"Constraint {constraint_id} is already set for this event.",
        )

    cursor.execute(
        """
        INSERT INTO event_constraints (event_id, constraint_id, constraint_value)
        VALUES (%s, %s, %s)
        """,
        (event_id, constraint_id, value),
    )
    db.commit()

    return _ok()


def designate_e_open_to(cursor, db, user_id, event_id, role_id):
    cursor.execute(
        """
        SELECT e.org_id
        FROM events e
        JOIN organization_leader ol ON e.org_id = ol.org_id
        WHERE e.event_id = %s AND ol.user_id = %s
        """,
        (event_id, user_id),
    )
    event = cursor.fetchone()
    if event is None:
        return _fail(
            "permission",
            f"User {user_id} cannot configure open-to roles for this event.",
        )

    organization_id = event[0]

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
            f"Role '{role_id}' is not valid for this organization.",
        )

    cursor.execute(
        """
        SELECT 1
        FROM event_open_to
        WHERE event_id = %s AND org_id = %s AND role_id = %s
        """,
        (event_id, organization_id, role_id),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "duplicate",
            f"Role '{role_id}' is already designated for this event.",
        )

    cursor.execute(
        """
        INSERT INTO event_open_to (event_id, org_id, role_id)
        VALUES (%s, %s, %s)
        """,
        (event_id, organization_id, role_id),
    )
    db.commit()

    return _ok()


def designate_e_closed(cursor, db, user_id, event_id):
    cursor.execute(
        """
        SELECT 1
        FROM events e
        JOIN organization_leader ol ON e.org_id = ol.org_id
        WHERE e.event_id = %s AND ol.user_id = %s
        """,
        (event_id, user_id),
    )
    if cursor.fetchone() is None:
        return _fail(
            "permission",
            f"User {user_id} cannot close this event.",
        )

    cursor.execute(
        """
        SELECT 1
        FROM market
        WHERE event_id = %s AND is_open = TRUE
        """,
        (event_id,),
    )
    if cursor.fetchone() is not None:
        return _fail(
            "precondition",
            "Close all markets for this event before closing the event.",
        )

    cursor.execute(
        """
        UPDATE events
        SET is_open = FALSE
        WHERE event_id = %s
        """,
        (event_id,),
    )
    db.commit()

    return _ok()
