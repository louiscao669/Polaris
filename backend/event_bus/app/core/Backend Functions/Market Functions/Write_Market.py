from typing import Any, Optional

import pymysql, datetime
from market_logic_helpers import _market_side_pools, _current_side_price, _average_fill_from_logs
from fail import _fail, _log_result
try:
    from app.read_cache import (
        invalidate_event_markets_cache,
        invalidate_market_detail_cache,
        invalidate_market_stats_cache,
    )
except ImportError:
    from backend.event_bus.app.read_cache import (
        invalidate_event_markets_cache,
        invalidate_market_detail_cache,
        invalidate_market_stats_cache,
    )
try:
    from app.database import get_connection
except ImportError:
    from backend.event_bus.app.database import get_connection


def _invalidate_market_reads(cursor, market_id: int, event_id: Optional[int] = None) -> None:
    invalidate_market_stats_cache(int(market_id))
    invalidate_market_detail_cache(int(market_id))
    if event_id is None:
        cursor.execute(
            """
            SELECT event_id
            FROM market
            WHERE id = %s
            """,
            (market_id,),
        )
        row = cursor.fetchone()
        event_id = row[0] if row is not None else None
    if event_id is not None:
        invalidate_event_markets_cache(int(event_id))

def create_m(data: dict[str, Any]): 
    user_id = data.get("user_id")
    event_id = data.get("event_id")
    question = data.get("question")
    description = data.get("description")

    if user_id is None:
        result = _fail("validation", "create_m payload is missing required field 'user_id'.")
        _log_result("create_m", result)
        return result

    if event_id is None:
        result = _fail("validation", "create_m payload is missing required field 'event_id'.")
        _log_result("create_m", result)
        return result

    if question is None:
        result = _fail("validation", "create_m payload is missing required field 'question'.")
        _log_result("create_m", result)
        return result

    if description is None:
        result = _fail("validation", "create_m payload is missing required field 'description'.")
        _log_result("create_m", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _create_m(cursor, db, user_id, event_id, question, description)
    _log_result("create_m", result)
    return result


def _create_m(cursor, db, user_id, event_id, question, description, explicit_market_id=None): 

    try:
        # Check if user has permission to create market in the event (or is organization leader) and lock the event row
        cursor.execute(
            """
            SELECT e.org_id, e.is_open
            FROM events e
            LEFT JOIN event_market_creators emc ON e.event_id = emc.event_id AND emc.user_id = %s
            LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
            WHERE e.event_id = %s AND (emc.user_id IS NOT NULL OR ol.user_id IS NOT NULL)
            FOR UPDATE
            """,
            (user_id, user_id, event_id),
        )

        event = cursor.fetchone()

        if event is None:
            return _fail("permission", "You do not have permission to create a market in that event.")

        # Check if the event is open
        if event[1] == 0:
            return _fail("not_open", "That event is already closed.")

        if explicit_market_id is not None:
            mid = int(explicit_market_id)
            cursor.execute(
                """
                INSERT INTO market (id, event_id, question, created_by)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    question = VALUES(question),
                    event_id = VALUES(event_id)
                """,
                (mid, event_id, question, user_id),
            )
            db.commit()
            _invalidate_market_reads(cursor, mid, event_id)
            return mid
        
        # Check if any markets with the same question already exist in the event
        cursor.execute(
            """
            SELECT id
            FROM market
            WHERE event_id = %s AND question = %s
            """,
            (event_id, question),
        )

        current_market = cursor.fetchone()
        if current_market is not None:

            return current_market[0]

        # Create market and insert it into the database, returning the market ID
        cursor.execute(
            """
            INSERT INTO market (event_id, question, created_by)
            VALUES (%s, %s, %s)
            """,
            (event_id, question, user_id),
        )

        db.commit()
        _invalidate_market_reads(cursor, cursor.lastrowid, event_id)

        return cursor.lastrowid

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to create market: {e}")

def designate_m_token(data: dict[str, Any]): 
    user_id = data.get("user_id")
    market_id = data.get("market_id")
    token_id = data.get("token_id")

    if user_id is None:
        result = _fail("validation", "designate_m_token payload is missing required field 'user_id'.")
        _log_result("designate_m_token", result)
        return result

    if market_id is None:
        result = _fail("validation", "designate_m_token payload is missing required field 'market_id'.")
        _log_result("designate_m_token", result)
        return result

    if token_id is None:
        result = _fail("validation", "designate_m_token payload is missing required field 'token_id'.")
        _log_result("designate_m_token", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _designate_m_token(cursor, db, user_id, market_id, token_id)
    _log_result("designate_m_token", result)
    return result


def _designate_m_token(cursor, db, user_id, market_id, token_id): 

    try:
        # Check if user is the market creator or organization leader
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
            return _fail("permission", "Only the market creator or organization leader can designate market tokens.")

        organization_id = market[0]

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

        # Check if the token is already designated for the market
        cursor.execute(
            """
            SELECT 1
            FROM market_tokens_allowed
            WHERE market_id = %s AND token_id = %s
            """,
            (market_id, token_id),
        )

        if cursor.fetchone() is not None:

            return True

        # Designate the token for the market in the database
        cursor.execute(
            """
            INSERT INTO market_tokens_allowed (market_id, token_id)
            VALUES (%s, %s)
            """,
            (market_id, token_id),
        )

        db.commit()
        _invalidate_market_reads(cursor, market_id)

        return True

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:

            cursor.execute(
                """
                SELECT 1
                FROM market_tokens_allowed
                WHERE market_id = %s AND token_id = %s
                """,
                (market_id, token_id),
            )

            existing_row = cursor.fetchone()

            if existing_row is not None:

                return True

        return _fail("validation", "Unable to designate the market token.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to designate the market token: {e}")

def designate_m_result(data: dict[str, Any]): 
    user_id = data.get("user_id")
    market_id = data.get("market_id")
    result = data.get("result")

    if user_id is None:
        outcome = _fail("validation", "designate_m_result payload is missing required field 'user_id'.")
        _log_result("designate_m_result", outcome)
        return outcome

    if market_id is None:
        outcome = _fail("validation", "designate_m_result payload is missing required field 'market_id'.")
        _log_result("designate_m_result", outcome)
        return outcome

    if result is None:
        outcome = _fail("validation", "designate_m_result payload is missing required field 'result'.")
        _log_result("designate_m_result", outcome)
        return outcome

    with get_connection() as db:
        cursor = db.cursor()
        outcome = _designate_m_result(cursor, db, user_id, market_id, result)
    _log_result("designate_m_result", outcome)
    return outcome


def _designate_m_result(cursor, db, user_id, market_id, result): 

    try:
        # Check if user is the market creator or organization leader and lock the market row
        cursor.execute(
            """
            SELECT m.is_open
            FROM market m
            JOIN events e ON m.event_id = e.event_id
            LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
            WHERE m.id = %s AND (m.created_by = %s OR ol.user_id IS NOT NULL)
            FOR UPDATE
            """,
            (user_id, market_id, user_id),
        )

        market = cursor.fetchone()

        if market is None:
            return _fail("permission", "Only the market creator or organization leader can designate market results.")

        # Check if the result is valid
        if result not in (True, False):
            return _fail("validation", "Market result must be True or False.")

        # Check if the market is still open
        if market[0] == 0:
            cursor.execute(
                """
                SELECT outcome
                FROM market_result
                WHERE market_id = %s
                """,
                (market_id,),
            )

            existing_result = cursor.fetchone()

            if existing_result is None:
                return _fail("not_open", "That market is already closed.")

            if bool(existing_result[0]) == bool(result):
                return True

            return _fail("duplicate", "That market already has a different result.")

        # Check if the result has already been designated for the market
        cursor.execute(
            """
            SELECT outcome
            FROM market_result
            WHERE market_id = %s
            """,
            (market_id,),
        )

        result_ex = cursor.fetchone()

        if result_ex:
            result_in = bool(result_ex[0])

            if result_in == result:

                return True

            else:

                return _fail("duplicate", "That market already has a different result.")

        # Designate the result for the market in the database
        normalized_result = bool(result)

        cursor.execute(
            """
            INSERT INTO market_result (market_id, outcome)
            VALUES (%s, %s)
            """,
            (market_id, normalized_result),
        )

        # Close the market in the database
        cursor.execute(
            """
            UPDATE market
            SET is_open = FALSE, close_at = NOW()
            WHERE id = %s
            """,
            (market_id,),
        )

        # Call do_m_payout for all tokens in the market to distribute winnings
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
            payout_result = _do_m_payout(cursor, db, user_id, market_id, token_row[0])
            if isinstance(payout_result, dict) and payout_result.get("ok") is False:
                return payout_result

        db.commit()
        _invalidate_market_reads(cursor, market_id)

        return True

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to designate the market result: {e}")

def designate_m_contraint(data: dict[str, Any]):
    user_id = data.get("user_id")
    market_id = data.get("market_id")
    constraint_id = data.get("constraint_id")
    value = data.get("value")

    if user_id is None:
        result = _fail("validation", "designate_m_contraint payload is missing required field 'user_id'.")
        _log_result("designate_m_contraint", result)
        return result

    if market_id is None:
        result = _fail("validation", "designate_m_contraint payload is missing required field 'market_id'.")
        _log_result("designate_m_contraint", result)
        return result

    if constraint_id is None:
        result = _fail("validation", "designate_m_contraint payload is missing required field 'constraint_id'.")
        _log_result("designate_m_contraint", result)
        return result

    if value is None:
        result = _fail("validation", "designate_m_contraint payload is missing required field 'value'.")
        _log_result("designate_m_contraint", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _designate_m_contraint(cursor, db, user_id, market_id, constraint_id, value)
    _log_result("designate_m_contraint", result)
    return result


def _designate_m_contraint(cursor, db, user_id, market_id, constraint_id, value):

    try:
        # Check if user is the market creator or organization leader
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
            return _fail("permission", "Only the market creator or organization leader can designate market constraints.")

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

        # Check if the constraint has already been designated for the market
        cursor.execute(
            """
            SELECT constraint_value
            FROM market_constraint
            WHERE market_id = %s AND constraint_id = %s
            """,
            (market_id, constraint_id),
        )

        existing_constraint = cursor.fetchone()

        if existing_constraint is not None:
            if existing_constraint[0] == value:

                return True

            return _fail("duplicate", "That market constraint already exists with a different value.")

        # Designate the constraint for the market in the database 
        cursor.execute(
            """
            INSERT INTO market_constraint (constraint_id, market_id, constraint_value)
            VALUES (%s, %s, %s)
            """,
            (constraint_id, market_id, value),
        )

        db.commit()
        _invalidate_market_reads(cursor, market_id)

        return True

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:

            cursor.execute(
                """
                SELECT constraint_value
                FROM market_constraint
                WHERE market_id = %s AND constraint_id = %s
                """,
                (market_id, constraint_id),
            )

            existing_constraint = cursor.fetchone()

            if existing_constraint[0] == value:

                return True

        return _fail("validation", "Unable to designate the market constraint.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to designate the market constraint: {e}")

def designate_m_open_to_as(data: dict[str, Any]): 
    user_id = data.get("user_id")
    market_id = data.get("market_id")
    role_id = data.get("role_id")
    as_id = data.get("as_id")

    if user_id is None:
        result = _fail("validation", "designate_m_open_to_as payload is missing required field 'user_id'.")
        _log_result("designate_m_open_to_as", result)
        return result

    if market_id is None:
        result = _fail("validation", "designate_m_open_to_as payload is missing required field 'market_id'.")
        _log_result("designate_m_open_to_as", result)
        return result

    if role_id is None:
        result = _fail("validation", "designate_m_open_to_as payload is missing required field 'role_id'.")
        _log_result("designate_m_open_to_as", result)
        return result

    if as_id is None:
        result = _fail("validation", "designate_m_open_to_as payload is missing required field 'as_id'.")
        _log_result("designate_m_open_to_as", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _designate_m_open_to_as(cursor, db, user_id, market_id, role_id, as_id)
    _log_result("designate_m_open_to_as", result)
    return result


def _designate_m_open_to_as(cursor, db, user_id, market_id, role_id, as_id): 
    organization_id = None
    try:
        # Check if user is the market creator or organization leader
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
            return _fail("permission", "Only the market creator or organization leader can change market access.")

        organization_id = market[0]

        # Check if the role and AS are valid
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

        cursor.execute(
            """
            SELECT 1
            FROM market_as
            WHERE as_code = %s
            """,
            (as_id,),
        )

        if cursor.fetchone() is None:
            return _fail("validation", "That access level does not exist.")

        # Check if the market is already open to the role as an AS
        cursor.execute(
            """
            SELECT as_id
            FROM market_open_to_as
            WHERE market_id = %s AND org_id = %s AND role_id = %s
            """,
            (market_id, organization_id, role_id),
        )

        existing_row = cursor.fetchone()

        if existing_row is not None:

            if existing_row[0] == as_id:

                return True
                
            return _fail("duplicate", "That market role is already assigned a different access level.")

        # Designate the market to be open to the specified role as an AS in the database
        cursor.execute(
            """
            INSERT INTO market_open_to_as (market_id, org_id, role_id, as_id)
            VALUES (%s, %s, %s, %s)
            """,
            (market_id, organization_id, role_id, as_id),
        )

        db.commit()
        _invalidate_market_reads(cursor, market_id)
        
        return True

    except pymysql.err.IntegrityError as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()

        if e.args and e.args[0] == 1062:

            cursor.execute(
                """
                SELECT as_id
                FROM market_open_to_as
                WHERE market_id = %s AND org_id = %s AND role_id = %s
                """,
                (market_id, organization_id, role_id),
            )

            existing_row = cursor.fetchone()

            if existing_row is not None and existing_row[0] == as_id:

                return True

        return _fail("validation", "Unable to designate market access.")

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to designate market access: {e}")

def do_m_transaction(data: dict[str, Any]): 
    user_id = data.get("user_id")
    market_id = data.get("market_id")
    token_id = data.get("token_id")
    side = data.get("side")
    qty = data.get("qty")
    transaction_id = data.get("transaction_id")
    transaction_type = data.get("transaction_type")
    if transaction_type is None and isinstance(data.get("type"), str):
        transaction_type = data["type"]

    if user_id is None:
        result = _fail("validation", "do_m_transaction payload is missing required field 'user_id'.")
        _log_result("do_m_transaction", result)
        return result

    if market_id is None:
        result = _fail("validation", "do_m_transaction payload is missing required field 'market_id'.")
        _log_result("do_m_transaction", result)
        return result

    if token_id is None:
        result = _fail("validation", "do_m_transaction payload is missing required field 'token_id'.")
        _log_result("do_m_transaction", result)
        return result

    if side is None:
        result = _fail("validation", "do_m_transaction payload is missing required field 'side'.")
        _log_result("do_m_transaction", result)
        return result

    if qty is None:
        result = _fail("validation", "do_m_transaction payload is missing required field 'qty'.")
        _log_result("do_m_transaction", result)
        return result

    if transaction_id is None:
        result = _fail("validation", "do_m_transaction payload is missing required field 'transaction_id'.")
        _log_result("do_m_transaction", result)
        return result

    if transaction_type is None:
        result = _fail("validation", "do_m_transaction payload is missing required field 'transaction_type'.")
        _log_result("do_m_transaction", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _do_m_transaction(cursor, db, user_id, market_id, token_id, side, qty, transaction_id, transaction_type)
    _log_result("do_m_transaction", result)
    return result


def _do_m_transaction(cursor, db, user_id, market_id, token_id, side, qty, transaction_id, transaction_type): 

    try:
        if qty <= 0:
            return _fail("validation", "Trade quantity must be greater than zero.")

        if transaction_type not in ("BUY", "SELL"):
            return _fail("validation", "Transaction type must be BUY or SELL.")

        normalized_side = bool(side)

        # Lock the market row before pricing and updating balances
        cursor.execute(
            """
            SELECT is_open
            FROM market
            WHERE id = %s
            FOR UPDATE
            """,
            (market_id,),
        )

        market_row = cursor.fetchone()

        if market_row is None or market_row[0] == 0:
            return _fail("not_open", "That market is not open for trading.")

        # Check if user has permission to trade in the market (or is organization leader)
        cursor.execute(
            """
            SELECT 1
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
            return _fail("permission", "You do not have permission to trade in that market.")

        cursor.execute(
            """
            SELECT 1
            FROM market_tokens_allowed
            WHERE market_id = %s AND token_id = %s
            """,
            (market_id, token_id),
        )

        if cursor.fetchone() is None:
            return _fail("validation", "That token is not allowed in the market.")

        # Check if the transaction_id is unique for the market
        cursor.execute(
            """
            SELECT transaction_id
            FROM market_transaction
            WHERE market_id = %s AND transaction_id = %s
            """,
            (market_id, transaction_id),
        )

        if cursor.fetchone() is not None:

            return transaction_id

        total_token_value, normalized_price = _average_fill_from_logs(
            cursor, market_id, normalized_side, qty, transaction_type
        )

        if transaction_type == "BUY":
            # Deduct token stock in the database only if the user has enough
            cursor.execute(
                """
                UPDATE user_token_stock
                SET qty = qty - %s
                WHERE token_id = %s AND user_id = %s AND qty >= %s
                """,
                (total_token_value, token_id, user_id, total_token_value),
            )

            if cursor.rowcount != 1:
                return _fail("precondition", "The user does not have enough token stock to buy those shares.")

        else:
            # Deduct market tickets in the database only if the user has enough
            cursor.execute(
                """
                UPDATE user_market_ticket
                SET qty = qty - %s
                WHERE user_id = %s AND market_id = %s AND side = %s AND qty >= %s
                """,
                (qty, user_id, market_id, normalized_side, qty),
            )

            if cursor.rowcount != 1:
                return _fail("precondition", "The user does not have enough market tickets to sell.")

        # Insert the market transaction into the database
        cursor.execute(
            """
            INSERT INTO market_transaction (transaction_id, market_id, token_id, type, amt, user_id, price, side)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                transaction_id,
                market_id,
                token_id,
                transaction_type,
                qty,
                user_id,
                normalized_price,
                normalized_side,
            ),
        )

        if transaction_type == "BUY":
            # Add bought market tickets to the user's balance
            cursor.execute(
                """
                INSERT INTO user_market_ticket (user_id, market_id, side, qty)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)
                """,
                (user_id, market_id, normalized_side, qty),
            )
        else:
            # Add sold token value back to the user's stock
            cursor.execute(
                """
                INSERT INTO user_token_stock (token_id, user_id, qty)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)
                """,
                (token_id, user_id, total_token_value),
            )

        db.commit()
        invalidate_market_stats_cache(int(market_id))
        
        return transaction_id

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to execute the market transaction: {e}")

def do_m_payout(data: dict[str, Any]): 
    user_id = data.get("user_id")
    market_id = data.get("market_id")
    token_id = data.get("token_id")

    if user_id is None:
        result = _fail("validation", "do_m_payout payload is missing required field 'user_id'.")
        _log_result("do_m_payout", result)
        return result

    if market_id is None:
        result = _fail("validation", "do_m_payout payload is missing required field 'market_id'.")
        _log_result("do_m_payout", result)
        return result

    if token_id is None:
        result = _fail("validation", "do_m_payout payload is missing required field 'token_id'.")
        _log_result("do_m_payout", result)
        return result

    with get_connection() as db:
        cursor = db.cursor()
        result = _do_m_payout(cursor, db, user_id, market_id, token_id)
    _log_result("do_m_payout", result)
    return result


def _do_m_payout(cursor, db, user_id, market_id, token_id): 

    try:
        # Lock the market row before checking payout status
        cursor.execute(
            """
            SELECT is_open
            FROM market
            WHERE id = %s
            FOR UPDATE
            """,
            (market_id,),
        )

        market_row = cursor.fetchone()

        if market_row is None:
            return _fail("validation", "That market does not exist.")

        # Check if user has permission to execute payout (or is organization leader)
        cursor.execute(
            """
            SELECT 1
            FROM market m
            JOIN events e ON m.event_id = e.event_id
            LEFT JOIN organization_leader ol ON e.org_id = ol.org_id AND ol.user_id = %s
            WHERE m.id = %s AND (m.created_by = %s OR ol.user_id IS NOT NULL)
            """,
            (user_id, market_id, user_id),
        )

        if cursor.fetchone() is None:
            return _fail("permission", "Only the market creator or organization leader can execute payouts.")

        # Check if the market is closed and the token is valid
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
            return _fail("not_closed", "That market must be resolved before payout can run.")

        # Check if the token is designated for the market
        cursor.execute(
            """
            SELECT 1
            FROM market_tokens_allowed
            WHERE market_id = %s AND token_id = %s
            """,
            (market_id, token_id),
        )

        if cursor.fetchone() is None:
            return _fail("validation", "That token is not allowed in the market.")

        winning_outcome = result_row[0]

        # Claim payout for this market/token pair before crediting winners
        try:
            cursor.execute(
                """
                INSERT INTO market_payout (market_id, token_id)
                VALUES (%s, %s)
                """,
                (market_id, token_id),
            )
        except pymysql.err.IntegrityError as e:
            db.rollback()

            if e.args and e.args[0] == 1062:
                return True

            raise

        # Find all winning tickets that still exist for this market
        cursor.execute(
            """
            SELECT user_id, qty
            FROM user_market_ticket
            WHERE market_id = %s AND side = %s AND qty > 0
            """,
            (market_id, winning_outcome),
        )

        winners = cursor.fetchall()

        for winner_user_id, ticket_qty in winners:
            payout_amount = ticket_qty * 100

            # Credit one full token for each winning ticket
            cursor.execute(
                """
                INSERT INTO user_token_stock (token_id, user_id, qty)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)
                """,
                (token_id, winner_user_id, payout_amount),
            )

        # Check if payout has been completed for every allowed token in the market
        cursor.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM market_tokens_allowed WHERE market_id = %s),
                (SELECT COUNT(*) FROM market_payout WHERE market_id = %s)
            """,
            (market_id, market_id),
        )

        allowed_token_count, paid_token_count = cursor.fetchone()

        if paid_token_count >= allowed_token_count:
            # Clear settled ticket inventory after every token payout is complete
            cursor.execute(
                """
                DELETE FROM user_market_ticket
                WHERE market_id = %s
                """,
                (market_id,),
            )

        db.commit()
        invalidate_market_stats_cache(int(market_id))

        return True

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to execute the market payout: {e}")
