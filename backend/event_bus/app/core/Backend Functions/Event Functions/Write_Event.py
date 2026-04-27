from typing import Any, Optional

import pymysql
from fail import _fail, _log_result
try:
    from app.read_cache import (
        invalidate_event_markets_cache,
        invalidate_market_detail_cache,
        invalidate_org_events_cache,
    )
except ImportError:
    from backend.event_bus.app.read_cache import (
        invalidate_event_markets_cache,
        invalidate_market_detail_cache,
        invalidate_org_events_cache,
    )
try:
    from app.database import get_connection
except ImportError:
    from backend.event_bus.app.database import get_connection


def _invalidate_event_reads(cursor, event_id: int, organization_id: Optional[int] = None) -> None:
    invalidate_event_markets_cache(int(event_id))
    if organization_id is None:
        cursor.execute(
            """
            SELECT org_id
            FROM events
            WHERE event_id = %s
            """,
            (event_id,),
        )
        row = cursor.fetchone()
        organization_id = row[0] if row is not None else None
    if organization_id is not None:
        invalidate_org_events_cache(int(organization_id))
    cursor.execute(
        """
        SELECT id
        FROM market
        WHERE event_id = %s
        """,
        (event_id,),
    )
    for row in cursor.fetchall():
        invalidate_market_detail_cache(int(row[0]))

def create_e(data: dict[str, Any]): 
    user_id = data.get("user_id")
    organization_id = data.get("organization_id")
    caption = data.get("caption")

    if user_id is None:
        result = _fail("validation", "create_e payload is missing required field 'user_id'.")
        _log_result("create_e", result)
        return result

    if organization_id is None:
        result = _fail("validation", "create_e payload is missing required field 'organization_id'.")
        _log_result("create_e", result)
        return result

    if caption is None:
        result = _fail("validation", "create_e payload is missing required field 'caption'.")
        _log_result("create_e", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _create_e(cursor, db, user_id, organization_id, caption)
    _log_result("create_e", result)
    return result


def _create_e(cursor, db, user_id, organization_id, caption, explicit_event_id=None): 
    try:
        # Check if user is the orgnization leader
        cursor.execute(
            """
            SELECT 1
            FROM organization_leader
            WHERE org_id = %s AND user_id = %s
            """,
            (organization_id, user_id),
        )
        if cursor.fetchone() is None:
            return _fail("permission", "Only the organization leader can create events.")

        if explicit_event_id is not None:
            eid = int(explicit_event_id)
            cursor.execute(
                """
                INSERT INTO events (event_id, org_id, caption, is_open)
                VALUES (%s, %s, %s, TRUE)
                ON DUPLICATE KEY UPDATE
                    org_id = VALUES(org_id),
                    caption = VALUES(caption)
                """,
                (eid, organization_id, caption),
            )
            db.commit()
            _invalidate_event_reads(cursor, eid, organization_id)
            return eid

        # Check if event with the same caption already exists in the organization
        cursor.execute(
            """
            SELECT event_id
            FROM events
            WHERE org_id = %s AND caption = %s
            """,
            (organization_id, caption),
        )
        current_event = cursor.fetchone()
        if current_event is not None:
            return current_event[0]

        # Create event and insert it into the database, returning the event ID
        cursor.execute(
            """
            INSERT INTO events (org_id, caption)
            VALUES (%s, %s)
            """,
            (organization_id, caption),
        )
        db.commit()
        _invalidate_event_reads(cursor, cursor.lastrowid, organization_id)

        return cursor.lastrowid

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            cursor.execute(
                """
                SELECT event_id
                FROM events
                WHERE org_id = %s AND caption = %s
                """,
                (organization_id, caption),
            )
            current_event = cursor.fetchone()
            if current_event is not None:
                return current_event[0]

        return _fail("validation", "Unable to create event.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to create event: {e}")

def designate_e_token(data: dict[str, Any]): 
    user_id = data.get("user_id")
    event_id = data.get("event_id")
    token_id = data.get("token_id")

    if user_id is None:
        result = _fail("validation", "designate_e_token payload is missing required field 'user_id'.")
        _log_result("designate_e_token", result)
        return result

    if event_id is None:
        result = _fail("validation", "designate_e_token payload is missing required field 'event_id'.")
        _log_result("designate_e_token", result)
        return result

    if token_id is None:
        result = _fail("validation", "designate_e_token payload is missing required field 'token_id'.")
        _log_result("designate_e_token", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _designate_e_token(cursor, db, user_id, event_id, token_id)
    _log_result("designate_e_token", result)
    return result


def _designate_e_token(cursor, db, user_id, event_id, token_id): 
    try:
        # Check if user is the organization leader and lock the event row
        cursor.execute(
            """
            SELECT e.org_id, e.is_open
            FROM events e
            JOIN organization_leader ol ON e.org_id = ol.org_id
            WHERE e.event_id = %s AND ol.user_id = %s
            FOR UPDATE
            """,
            (event_id, user_id),
        )
        event = cursor.fetchone()
        if event is None:
            return _fail("permission", "Only the organization leader can designate event tokens.")

        organization_id = event[0]

        # Check if the event is open
        if event[1] == 0:
            return _fail("not_open", "That event is already closed.")

        # Check if the token is valid and belongs to the organization
        cursor.execute(
            """
            SELECT 1
            FROM organization_token
            WHERE token_id = %s AND org_id = %s
            """,
            (token_id, organization_id),
        )
        if cursor.fetchone() is None:
            return _fail("validation", "That token does not belong to the organization.")

        # Designate the token for the event in the database
        cursor.execute(
            """
            SELECT 1
            FROM event_tokens_allowed
            WHERE event_id = %s AND token_id = %s
            """,
            (event_id, token_id),
        )
        if cursor.fetchone() is not None:
            return True

        cursor.execute(
            """
            INSERT INTO event_tokens_allowed (event_id, token_id)
            VALUES (%s, %s)
            """,
            (event_id, token_id),
        )
        db.commit()
        _invalidate_event_reads(cursor, event_id, organization_id)

        return True

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            return True

        return _fail("validation", "Unable to designate the event token.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to designate the event token: {e}")

def designate_e_market_creator(data: dict[str, Any]): 
    user_id = data.get("user_id")
    event_id = data.get("event_id")
    market_creator_id = data.get("market_creator_id")

    if user_id is None:
        result = _fail("validation", "designate_e_market_creator payload is missing required field 'user_id'.")
        _log_result("designate_e_market_creator", result)
        return result

    if event_id is None:
        result = _fail("validation", "designate_e_market_creator payload is missing required field 'event_id'.")
        _log_result("designate_e_market_creator", result)
        return result

    if market_creator_id is None:
        result = _fail("validation", "designate_e_market_creator payload is missing required field 'market_creator_id'.")
        _log_result("designate_e_market_creator", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _designate_e_market_creator(cursor, db, user_id, event_id, market_creator_id)
    _log_result("designate_e_market_creator", result)
    return result


def _designate_e_market_creator(cursor, db, user_id, event_id, market_creator_id): 
    try:
        # Check if user is the organization leader and lock the event row
        cursor.execute(
            """
            SELECT e.org_id, e.is_open
            FROM events e
            JOIN organization_leader ol ON e.org_id = ol.org_id
            WHERE e.event_id = %s AND ol.user_id = %s
            FOR UPDATE
            """,
            (event_id, user_id),
        )
        event = cursor.fetchone()
        if event is None:
            return _fail("permission", "Only the organization leader can designate market creators.")

        organization_id = event[0]

        # Check if the event is open
        if event[1] == 0:
            return _fail("not_open", "That event is already closed.")

        # Check if the market creator is valid and belongs to the organization
        cursor.execute(
            """
            SELECT 1
            FROM user_org_role
            WHERE org_id = %s AND user_id = %s
            """,
            (organization_id, market_creator_id),
        )
        if cursor.fetchone() is None:
            return _fail("validation", "That user is not a member of the organization.")

        # Designate the market creator for the event in the database
        cursor.execute(
            """
            SELECT 1
            FROM event_market_creators
            WHERE event_id = %s AND user_id = %s
            """,
            (event_id, market_creator_id),
        )
        if cursor.fetchone() is not None:
            return True

        cursor.execute(
            """
            INSERT INTO event_market_creators (event_id, user_id)
            VALUES (%s, %s)
            """,
            (event_id, market_creator_id),
        )
        db.commit()
        _invalidate_event_reads(cursor, event_id, event[0])

        return True

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            return True

        return _fail("validation", "Unable to designate the market creator.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to designate the market creator: {e}")

def designate_e_contraint(data: dict[str, Any]): 
    user_id = data.get("user_id")
    event_id = data.get("event_id")
    constraint_id = data.get("constraint_id")
    value = data.get("value")

    if user_id is None:
        result = _fail("validation", "designate_e_contraint payload is missing required field 'user_id'.")
        _log_result("designate_e_contraint", result)
        return result

    if event_id is None:
        result = _fail("validation", "designate_e_contraint payload is missing required field 'event_id'.")
        _log_result("designate_e_contraint", result)
        return result

    if constraint_id is None:
        result = _fail("validation", "designate_e_contraint payload is missing required field 'constraint_id'.")
        _log_result("designate_e_contraint", result)
        return result

    if value is None:
        result = _fail("validation", "designate_e_contraint payload is missing required field 'value'.")
        _log_result("designate_e_contraint", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _designate_e_contraint(cursor, db, user_id, event_id, constraint_id, value)
    _log_result("designate_e_contraint", result)
    return result


def _designate_e_contraint(cursor, db, user_id, event_id, constraint_id, value): 
    try:
        # Check if user is the organization leader and lock the event row
        cursor.execute(
            """
            SELECT e.is_open
            FROM events e
            JOIN organization_leader ol ON e.org_id = ol.org_id
            WHERE e.event_id = %s AND ol.user_id = %s
            FOR UPDATE
            """,
            (event_id, user_id),
        )
        event = cursor.fetchone()
        if event is None:
            return _fail("permission", "Only the organization leader can designate event constraints.")

        # Check if the event is open
        if event[0] == 0:
            return _fail("not_open", "That event is already closed.")

        # Check if the constraint is valid
        cursor.execute(
            """
            SELECT 1
            FROM constraint_type
            WHERE constraint_id = %s
            """,
            (constraint_id,),
        )
        if cursor.fetchone() is None:
            return _fail("validation", "That constraint type does not exist.")

        # Designate the constraint for the event in the database
        cursor.execute(
            """
            SELECT constraint_value
            FROM event_constraints
            WHERE event_id = %s AND constraint_id = %s
            """,
            (event_id, constraint_id),
        )
        existing_constraint = cursor.fetchone()
        if existing_constraint is not None:
            if existing_constraint[0] == value:
                return True

            return _fail("duplicate", "That event constraint already exists with a different value.")

        cursor.execute(
            """
            INSERT INTO event_constraints (event_id, constraint_id, constraint_value)
            VALUES (%s, %s, %s)
            """,
            (event_id, constraint_id, value),
        )
        db.commit()
        _invalidate_event_reads(cursor, event_id, event[0])

        return True

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            cursor.execute(
                """
                SELECT constraint_value
                FROM event_constraints
                WHERE event_id = %s AND constraint_id = %s
                """,
                (event_id, constraint_id),
            )
            existing_constraint = cursor.fetchone()

            if existing_constraint is not None and existing_constraint[0] == value:
                return True

        return _fail("validation", "Unable to designate the event constraint.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to designate the event constraint: {e}")

def designate_e_open_to(data: dict[str, Any]): 
    user_id = data.get("user_id")
    event_id = data.get("event_id")
    role_id = data.get("role_id")

    if user_id is None:
        result = _fail("validation", "designate_e_open_to payload is missing required field 'user_id'.")
        _log_result("designate_e_open_to", result)
        return result

    if event_id is None:
        result = _fail("validation", "designate_e_open_to payload is missing required field 'event_id'.")
        _log_result("designate_e_open_to", result)
        return result

    if role_id is None:
        result = _fail("validation", "designate_e_open_to payload is missing required field 'role_id'.")
        _log_result("designate_e_open_to", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _designate_e_open_to(cursor, db, user_id, event_id, role_id)
    _log_result("designate_e_open_to", result)
    return result


def _designate_e_open_to(cursor, db, user_id, event_id, role_id): 
    try:
        # Check if user is the organization leader and lock the event row
        cursor.execute(
            """
            SELECT e.org_id, e.is_open
            FROM events e
            JOIN organization_leader ol ON e.org_id = ol.org_id
            WHERE e.event_id = %s AND ol.user_id = %s
            FOR UPDATE
            """,
            (event_id, user_id),
        )
        event = cursor.fetchone()
        if event is None:
            return _fail("permission", "Only the organization leader can change event visibility.")

        organization_id = event[0]

        # Check if the event is open
        if event[1] == 0:
            return _fail("not_open", "That event is already closed.")

        # Check if the role is valid
        cursor.execute(
            """
            SELECT 1
            FROM organization_role
            WHERE org_id = %s AND role = %s
            """,
            (organization_id, role_id),
        )
        if cursor.fetchone() is None:
            return _fail("validation", "That role does not exist in the organization.")

        # Designate the event to be open to the specified role in the database
        cursor.execute(
            """
            SELECT 1
            FROM event_open_to
            WHERE event_id = %s AND org_id = %s AND role_id = %s
            """,
            (event_id, organization_id, role_id),
        )
        if cursor.fetchone() is not None:
            return True

        cursor.execute(
            """
            INSERT INTO event_open_to (event_id, org_id, role_id)
            VALUES (%s, %s, %s)
            """,
            (event_id, organization_id, role_id),
        )
        db.commit()
        _invalidate_event_reads(cursor, event_id, organization_id)

        return True

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            return True

        return _fail("validation", "Unable to designate event visibility.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to designate event visibility: {e}")

def designate_e_closed(data: dict[str, Any]): 
    user_id = data.get("user_id")
    event_id = data.get("event_id")

    if user_id is None:
        result = _fail("validation", "designate_e_closed payload is missing required field 'user_id'.")
        _log_result("designate_e_closed", result)
        return result

    if event_id is None:
        result = _fail("validation", "designate_e_closed payload is missing required field 'event_id'.")
        _log_result("designate_e_closed", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _designate_e_closed(cursor, db, user_id, event_id)
    _log_result("designate_e_closed", result)
    return result


def _designate_e_closed(cursor, db, user_id, event_id): 
    try:
        # Check if user is the organization leader and lock the event row
        cursor.execute(
            """
            SELECT e.is_open
            FROM events e
            JOIN organization_leader ol ON e.org_id = ol.org_id
            WHERE e.event_id = %s AND ol.user_id = %s
            FOR UPDATE
            """,
            (event_id, user_id),
        )
        event = cursor.fetchone()
        if event is None:
            return _fail("permission", "Only the organization leader can close events.")

        # Check if event is already closed
        if event[0] == 0:
            return True

        # Check if markets are all closed for the event
        cursor.execute(
            """
            SELECT 1
            FROM market
            WHERE event_id = %s AND is_open = TRUE
            LIMIT 1
            """,
            (event_id,),
        )
        if cursor.fetchone() is not None:
            return _fail("precondition", "All markets in the event must be closed first.")

        # Close the event in the database
        cursor.execute(
            """
            UPDATE events
            SET is_open = FALSE
            WHERE event_id = %s
            """,
            (event_id,),
        )
        db.commit()
        _invalidate_event_reads(cursor, event_id, event[0])

        return True

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to close the event: {e}")


def delete_e(data: dict[str, Any]):
    user_id = data.get("user_id")
    event_id = data.get("event_id")

    if user_id is None:
        result = _fail("validation", "delete_e payload is missing required field 'user_id'.")
        _log_result("delete_e", result)
        return result

    if event_id is None:
        result = _fail("validation", "delete_e payload is missing required field 'event_id'.")
        _log_result("delete_e", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _delete_e(cursor, db, user_id, event_id)
    _log_result("delete_e", result)
    return result


def _delete_e(cursor, db, user_id, event_id):
    try:
        cursor.execute(
            """
            SELECT e.org_id
            FROM events e
            JOIN organization_leader ol ON e.org_id = ol.org_id
            WHERE e.event_id = %s AND ol.user_id = %s
            FOR UPDATE
            """,
            (event_id, user_id),
        )
        event = cursor.fetchone()
        if event is None:
            return _fail("permission", "Only the organization leader can delete events.")

        organization_id = int(event[0])

        cursor.execute(
            """
            SELECT id
            FROM market
            WHERE event_id = %s
            """,
            (event_id,),
        )
        market_ids = [int(row[0]) for row in cursor.fetchall()]

        cursor.execute(
            """
            DELETE FROM events
            WHERE event_id = %s
            """,
            (event_id,),
        )
        if cursor.rowcount != 1:
            return _fail("validation", "That event does not exist.")

        db.commit()
        _invalidate_event_reads(cursor, int(event_id), organization_id)
        for market_id in market_ids:
            invalidate_market_detail_cache(market_id)

        return True

    except Exception as e:
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")
        return _fail("validation", f"Unable to delete the event: {e}")
