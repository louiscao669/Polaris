from typing import Any

import pymysql
from fail import _fail
try:
    from app.database import get_connection
except ImportError:
    from backend.event_bus.app.database import get_connection

def update_e(data: dict[str, Any]):
    user_id = data.get("user_id")
    event_id = data.get("event_id")
    caption = data.get("caption")

    if user_id is None:
        return _fail("validation", "update_e payload is missing required field 'user_id'.")

    if event_id is None:
        return _fail("validation", "update_e payload is missing required field 'event_id'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _update_e(cursor, db, user_id, event_id, caption)


def _update_e(cursor, db, user_id, event_id, caption=None):
    try:
        # Check if user is the organization leader
        cursor.execute(
            """
            SELECT e.org_id, e.caption, e.is_open
            FROM events e
            JOIN organization_leader ol ON e.org_id = ol.org_id
            WHERE e.event_id = %s AND ol.user_id = %s
            """,
            (event_id, user_id),
        )
        event = cursor.fetchone()
        if event is None:
            return _fail("permission", "Only the organization leader can update events.")

        organization_id = event[0]

        # Check if the event is open
        if event[2] == 0:
            return _fail("not_open", "That event is already closed.")

        next_caption = event[1] if caption is None else caption

        # Check if the event is already set to that caption
        if event[1] == next_caption:
            return event_id

        # Check if another event in the organization already uses that caption
        cursor.execute(
            """
            SELECT event_id
            FROM events
            WHERE org_id = %s AND caption = %s AND event_id <> %s
            """,
            (organization_id, next_caption, event_id),
        )
        if cursor.fetchone() is not None:
            return _fail("duplicate", "An event with that caption already exists in the organization.")

        # Update the event in the database
        cursor.execute(
            """
            UPDATE events
            SET caption = %s
            WHERE event_id = %s
            """,
            (next_caption, event_id),
        )
        db.commit()

        return event_id

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            return _fail("duplicate", "An event with that caption already exists in the organization.")

        return _fail("validation", "Unable to update the event.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to update the event: {e}")
