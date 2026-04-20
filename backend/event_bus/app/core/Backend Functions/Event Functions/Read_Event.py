from typing import Any

import pymysql
from fail import _fail
try:
    from app.database import get_connection
except ImportError:
    from backend.event_bus.app.database import get_connection

def read_o_events(data: dict[str, Any]):
    user_id = data.get("user_id")
    organization_id = data.get("organization_id")

    if user_id is None:
        return _fail("validation", "read_o_events payload is missing required field 'user_id'.")

    if organization_id is None:
        return _fail("validation", "read_o_events payload is missing required field 'organization_id'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _read_o_events(cursor, db, user_id, organization_id)


def _read_o_events(cursor, db, user_id, organization_id):
    try:
        # Check if the user is the organization leader or a member of the organization
        cursor.execute(
            """
            SELECT
                MAX(CASE WHEN ol.user_id = %s THEN 1 ELSE 0 END) AS is_leader,
                MAX(uor.role_id) AS role_id
            FROM organization o
            LEFT JOIN organization_leader ol ON o.org_id = ol.org_id
            LEFT JOIN user_org_role uor ON o.org_id = uor.org_id AND uor.user_id = %s
            WHERE o.org_id = %s
            GROUP BY o.org_id
            """,
            (user_id, user_id, organization_id),
        )
        access = cursor.fetchone()
        if access is None:
            return _fail("validation", "That organization does not exist.")

        if access[0] == 0 and access[1] is None:
            return _fail("permission", "You do not have permission to read events in that organization.")

        # Read every event in the organization that is visible to the user
        cursor.execute(
            """
            SELECT
                e.event_id,
                e.caption,
                e.is_open
            FROM events e
            WHERE e.org_id = %s
              AND (
                    %s = 1
                    OR NOT EXISTS (
                        SELECT 1
                        FROM event_open_to eto
                        WHERE eto.event_id = e.event_id
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM event_open_to eto
                        WHERE eto.event_id = e.event_id
                          AND eto.org_id = %s
                          AND eto.role_id = %s
                    )
              )
            ORDER BY e.event_id ASC
            """,
            (organization_id, access[0], organization_id, access[1]),
        )

        return [
            {
                "event_id": row[0],
                "caption": row[1],
                "is_open": bool(row[2]),
            }
            for row in cursor.fetchall()
        ]

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to read the organization's events: {e}")

def read_e(data: dict[str, Any]):
    user_id = data.get("user_id")
    event_id = data.get("event_id")

    if user_id is None:
        return _fail("validation", "read_e payload is missing required field 'user_id'.")

    if event_id is None:
        return _fail("validation", "read_e payload is missing required field 'event_id'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _read_e(cursor, db, user_id, event_id)


def _read_e(cursor, db, user_id, event_id):
    try:
        # Check if the user is the organization leader or a member allowed to view the event
        cursor.execute(
            """
            SELECT
                e.org_id,
                e.caption,
                e.is_open,
                MAX(CASE WHEN ol.user_id = %s THEN 1 ELSE 0 END) AS is_leader,
                MAX(uor.role_id) AS role_id
            FROM events e
            LEFT JOIN organization_leader ol ON e.org_id = ol.org_id
            LEFT JOIN user_org_role uor ON e.org_id = uor.org_id AND uor.user_id = %s
            WHERE e.event_id = %s
            GROUP BY e.event_id, e.org_id, e.caption, e.is_open
            """,
            (user_id, user_id, event_id),
        )
        event = cursor.fetchone()
        if event is None:
            return _fail("validation", "That event does not exist.")

        if event[3] == 0 and event[4] is None:
            return _fail("permission", "You do not have permission to read that event.")

        cursor.execute(
            """
            SELECT 1
            FROM event_open_to
            WHERE event_id = %s
            LIMIT 1
            """,
            (event_id,),
        )
        event_has_visibility_rules = cursor.fetchone() is not None

        if event[3] == 0 and event_has_visibility_rules:
            cursor.execute(
                """
                SELECT 1
                FROM event_open_to
                WHERE event_id = %s AND org_id = %s AND role_id = %s
                """,
                (event_id, event[0], event[4]),
            )
            if cursor.fetchone() is None:
                return _fail("permission", "You do not have permission to read that event.")

        # Read event tokens, constraints, and designated market creators
        cursor.execute(
            """
            SELECT token_id
            FROM event_tokens_allowed
            WHERE event_id = %s
            ORDER BY token_id ASC
            """,
            (event_id,),
        )
        tokens_allowed = [row[0] for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT constraint_id, constraint_value
            FROM event_constraints
            WHERE event_id = %s
            ORDER BY constraint_id ASC
            """,
            (event_id,),
        )
        constraints = [
            {
                "constraint_id": row[0],
                "value": row[1],
            }
            for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT user_id
            FROM event_market_creators
            WHERE event_id = %s
            ORDER BY user_id ASC
            """,
            (event_id,),
        )
        market_creators = [row[0] for row in cursor.fetchall()]

        return {
            "event_id": event_id,
            "organization_id": event[0],
            "caption": event[1],
            "is_open": bool(event[2]),
            "is_leader": bool(event[3]),
            "role_id": event[4],
            "tokens_allowed": tokens_allowed,
            "constraints": constraints,
            "market_creators": market_creators,
        }

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to read the event: {e}")
