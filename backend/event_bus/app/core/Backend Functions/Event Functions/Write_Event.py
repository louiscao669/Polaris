from typing import Any

import pymysql
from fail import _fail
try:
    from ..database import get_connection
except ImportError:
    from backend.event_bus.app.database import get_connection

def create_e(data: dict[str, Any]): 
    user_id = data.get("user_id")
    organization_id = data.get("organization_id")
    caption = data.get("caption")

    if user_id is None:
        return _fail("validation", "create_e payload is missing required field 'user_id'.")

    if organization_id is None:
        return _fail("validation", "create_e payload is missing required field 'organization_id'.")

    if caption is None:
        return _fail("validation", "create_e payload is missing required field 'caption'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _create_e(cursor, db, user_id, organization_id, caption)


def _create_e(cursor, db, user_id, organization_id, caption): 
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
        return _fail("validation", "designate_e_token payload is missing required field 'user_id'.")

    if event_id is None:
        return _fail("validation", "designate_e_token payload is missing required field 'event_id'.")

    if token_id is None:
        return _fail("validation", "designate_e_token payload is missing required field 'token_id'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _designate_e_token(cursor, db, user_id, event_id, token_id)


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
        return _fail("validation", "designate_e_market_creator payload is missing required field 'user_id'.")

    if event_id is None:
        return _fail("validation", "designate_e_market_creator payload is missing required field 'event_id'.")

    if market_creator_id is None:
        return _fail("validation", "designate_e_market_creator payload is missing required field 'market_creator_id'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _designate_e_market_creator(cursor, db, user_id, event_id, market_creator_id)


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
        return _fail("validation", "designate_e_contraint payload is missing required field 'user_id'.")

    if event_id is None:
        return _fail("validation", "designate_e_contraint payload is missing required field 'event_id'.")

    if constraint_id is None:
        return _fail("validation", "designate_e_contraint payload is missing required field 'constraint_id'.")

    if value is None:
        return _fail("validation", "designate_e_contraint payload is missing required field 'value'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _designate_e_contraint(cursor, db, user_id, event_id, constraint_id, value)


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
        return _fail("validation", "designate_e_open_to payload is missing required field 'user_id'.")

    if event_id is None:
        return _fail("validation", "designate_e_open_to payload is missing required field 'event_id'.")

    if role_id is None:
        return _fail("validation", "designate_e_open_to payload is missing required field 'role_id'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _designate_e_open_to(cursor, db, user_id, event_id, role_id)


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
        return _fail("validation", "designate_e_closed payload is missing required field 'user_id'.")

    if event_id is None:
        return _fail("validation", "designate_e_closed payload is missing required field 'event_id'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _designate_e_closed(cursor, db, user_id, event_id)


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

        return True

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to close the event: {e}")
