"""Read-only queries for the user dashboard (organizations + bettable events)."""

from __future__ import annotations

from typing import Any


def list_user_organizations(cursor, db, user_id: int) -> list[dict[str, Any]]:
    """Organizations the user leads or belongs to via user_org_role."""
    del db
    seen: dict[int, dict[str, Any]] = {}

    cursor.execute(
        """
        SELECT o.org_id, o.name, o.description, 'leader' AS kind, NULL AS role_id
        FROM organization o
        INNER JOIN organization_leader ol ON o.org_id = ol.org_id
        WHERE ol.user_id = %s
        """,
        (user_id,),
    )
    for row in cursor.fetchall():
        oid, name, desc, kind, role_id = row
        seen[int(oid)] = {
            "org_id": int(oid),
            "name": name,
            "description": desc or "",
            "membership": "leader",
            "role_id": None,
        }

    cursor.execute(
        """
        SELECT o.org_id, o.name, o.description, uor.role_id
        FROM organization o
        INNER JOIN user_org_role uor ON o.org_id = uor.org_id
        WHERE uor.user_id = %s
        """,
        (user_id,),
    )
    for row in cursor.fetchall():
        oid, name, desc, role_id = row
        oid = int(oid)
        if oid in seen:
            # Already leader; keep leader row but we could merge role info
            continue
        seen[oid] = {
            "org_id": oid,
            "name": name,
            "description": desc or "",
            "membership": "member",
            "role_id": role_id,
        }

    return list(seen.values())


def list_org_events_for_user(cursor, db, org_id: int, user_id: int) -> list[dict[str, Any]]:
    """Open events in org the user may bet on (leader sees all open; others respect event_open_to)."""
    del db

    cursor.execute(
        """
        SELECT 1 FROM organization_leader
        WHERE org_id = %s AND user_id = %s
        LIMIT 1
        """,
        (org_id, user_id),
    )
    is_leader = cursor.fetchone() is not None

    if is_leader:
        cursor.execute(
            """
            SELECT event_id, caption, is_open
            FROM events
            WHERE org_id = %s AND is_open = TRUE
            ORDER BY event_id DESC
            """,
            (org_id,),
        )
    else:
        cursor.execute(
            """
            SELECT e.event_id, e.caption, e.is_open
            FROM events e
            WHERE e.org_id = %s AND e.is_open = TRUE
            AND (
                NOT EXISTS (SELECT 1 FROM event_open_to x WHERE x.event_id = e.event_id)
                OR EXISTS (
                    SELECT 1
                    FROM event_open_to eo
                    INNER JOIN user_org_role uor
                        ON uor.org_id = eo.org_id
                        AND uor.role_id = eo.role_id
                        AND uor.user_id = %s
                    WHERE eo.event_id = e.event_id
                )
            )
            ORDER BY e.event_id DESC
            """,
            (org_id, user_id),
        )

    out: list[dict[str, Any]] = []
    for row in cursor.fetchall():
        eid, caption, is_open = row
        out.append(
            {
                "event_id": int(eid),
                "caption": caption,
                "is_open": bool(is_open),
            }
        )
    return out


def user_belongs_to_org(cursor, db, org_id: int, user_id: int) -> bool:
    del db
    cursor.execute(
        """
        SELECT 1 FROM organization_leader WHERE org_id = %s AND user_id = %s
        """,
        (org_id, user_id),
    )
    if cursor.fetchone() is not None:
        return True
    cursor.execute(
        """
        SELECT 1 FROM user_org_role WHERE org_id = %s AND user_id = %s
        """,
        (org_id, user_id),
    )
    return cursor.fetchone() is not None


def get_organization(cursor, db, org_id: int) -> dict[str, Any] | None:
    del db
    cursor.execute(
        """
        SELECT org_id, name, description
        FROM organization
        WHERE org_id = %s
        LIMIT 1
        """,
        (org_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    oid, name, description = row
    return {
        "org_id": int(oid),
        "name": name,
        "description": description or "",
    }


def list_organization_events(cursor, db, org_id: int) -> list[dict[str, Any]]:
    del db
    cursor.execute(
        """
        SELECT event_id, caption, is_open
        FROM events
        WHERE org_id = %s
        ORDER BY event_id DESC
        """,
        (org_id,),
    )
    out: list[dict[str, Any]] = []
    for row in cursor.fetchall():
        eid, caption, is_open = row
        out.append(
            {
                "event_id": int(eid),
                "caption": caption,
                "is_open": bool(is_open),
            }
        )
    return out


def get_event(cursor, db, event_id: int) -> dict[str, Any] | None:
    del db
    cursor.execute(
        """
        SELECT e.event_id, e.org_id, e.caption, e.is_open, o.name
        FROM events e
        INNER JOIN organization o ON o.org_id = e.org_id
        WHERE e.event_id = %s
        LIMIT 1
        """,
        (event_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    eid, oid, caption, is_open, org_name = row
    return {
        "event_id": int(eid),
        "organization_id": int(oid),
        "organization_name": org_name,
        "caption": caption,
        "is_open": bool(is_open),
    }

def get_num_participants(cursor, db, org_id: int) -> int:
    del db
    cursor.execute(
        """
        SELECT COUNT(*) FROM user_org_role WHERE org_id = %s
        """,
        (org_id,),
    )
    return cursor.fetchone()[0]

def get_num_events(cursor, db, org_id: int) -> int:
    del db
    cursor.execute(
        """
        SELECT COUNT(*) FROM events WHERE org_id = %s
        """,
        (org_id,),
    )
    return cursor.fetchone()[0]

def get_num_open_markets(cursor, db, event_id: int) -> int:
    del db
    cursor.execute(
        """
        SELECT COUNT(*) FROM market WHERE event_id = %s AND is_open = TRUE
        """,
        (event_id,),
    )
    return cursor.fetchone()[0]


def get_tokens_allowed_org(cursor, db, org_id: int) -> list[int]:
    del db
    cursor.execute(
        """
        SELECT DISTINCT eta.token_id
        FROM event_tokens_allowed eta
        INNER JOIN events e ON e.event_id = eta.event_id
        WHERE e.org_id = %s
        """,
        (org_id,),
    )
    return [token_id for token_id, in cursor.fetchall()]


def get_tokens_allowed_market(cursor, db, market_id: int) -> list[int]:
    del db
    cursor.execute(
        """
        SELECT token_id
        FROM market_tokens_allowed
        WHERE market_id = %s
        """,
        (market_id,),
    )
    return [token_id for token_id, in cursor.fetchall()]

def get_token_name(cursor, db, org_id: int, token_id: int) -> str:
    del db
    cursor.execute(
        """
        SELECT name
        FROM organization_token
        WHERE org_id = %s AND token_id = %s
        """,
        (org_id, token_id),
    )
    row = cursor.fetchone()
    return row[0] if row is not None else "Unknown Token"

def get_token_description(cursor, db, org_id: int, token_id: int) -> str:
    del db
    cursor.execute(
        """
        SELECT description
        FROM organization_token
        WHERE org_id = %s AND token_id = %s
        """,
        (org_id, token_id),
    )
    row = cursor.fetchone()
    if row is None:
        return "No description available"
    return row[0] or "No description available"

def get_tokens_quantity(cursor, db, token_id: int, org_id: int, user_id: int | None = None) -> int:
    del db
    if user_id is None:
        cursor.execute(
            """
            SELECT COALESCE(SUM(uts.qty), 0)
            FROM user_token_stock uts
            INNER JOIN organization_token ot ON ot.token_id = uts.token_id
            WHERE uts.token_id = %s AND ot.org_id = %s
            """,
            (token_id, org_id),
        )
    else:
        cursor.execute(
            """
            SELECT COALESCE(uts.qty, 0)
            FROM user_token_stock uts
            INNER JOIN organization_token ot ON ot.token_id = uts.token_id
            WHERE uts.token_id = %s AND ot.org_id = %s AND uts.user_id = %s
            LIMIT 1
            """,
            (token_id, org_id, user_id),
        )
    row = cursor.fetchone()
    return int(row[0]) if row is not None else 0


def get_total_volume_org(cursor, db, org_id: int) -> int:
    del db
    cursor.execute(
        """
        SELECT COALESCE(SUM(mt.amt), 0)
        FROM market_transaction mt
        INNER JOIN market m ON m.id = mt.market_id
        INNER JOIN events e ON e.event_id = m.event_id
        WHERE e.org_id = %s
        """,
        (org_id,),
    )
    row = cursor.fetchone()
    return int(row[0]) if row is not None else 0