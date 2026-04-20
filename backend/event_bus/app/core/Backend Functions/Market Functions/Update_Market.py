from typing import Any

import pymysql, datetime
from market_logic_helpers import _market_side_pools, _current_side_price, _average_fill_from_logs
from fail import _fail
try:
    from ..database import get_connection
except ImportError:
    from backend.event_bus.app.database import get_connection

def update_m(data: dict[str, Any]):
    user_id = data.get("user_id")
    market_id = data.get("market_id")
    question = data.get("question")

    if user_id is None:
        return _fail("validation", "update_m payload is missing required field 'user_id'.")

    if market_id is None:
        return _fail("validation", "update_m payload is missing required field 'market_id'.")

    with get_connection() as db:
        cursor = db.cursor()
        return _update_m(cursor, db, user_id, market_id, question)


def _update_m(cursor, db, user_id, market_id, question=None):
    try:
        # Check if user is the market creator or organization leader
        cursor.execute(
            """
            SELECT m.event_id, m.question, m.is_open
            FROM market m
            JOIN events e ON m.event_id = e.event_id
            LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
            WHERE m.id = %s AND (m.created_by = %s OR ol.user_id IS NOT NULL)
            """,
            (user_id, market_id, user_id),
        )
        market = cursor.fetchone()
        if market is None:
            return _fail("permission", "Only the market creator or organization leader can update markets.")

        # Check if the market is open
        if market[2] == 0:
            return _fail("not_open", "That market is already closed.")

        next_question = market[1] if question is None else question

        # Check if the market is already set to that question
        if market[1] == next_question:
            return market_id

        # Check if another market in the event already uses that question
        cursor.execute(
            """
            SELECT id
            FROM market
            WHERE event_id = %s AND question = %s AND id <> %s
            """,
            (market[0], next_question, market_id),
        )
        if cursor.fetchone() is not None:
            return _fail("duplicate", "A market with that question already exists in the event.")

        # Update the market in the database
        cursor.execute(
            """
            UPDATE market
            SET question = %s
            WHERE id = %s
            """,
            (next_question, market_id),
        )
        db.commit()

        return market_id

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:
            return _fail("duplicate", "A market with that question already exists in the event.")

        return _fail("validation", "Unable to update the market.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to update the market: {e}")
