import pymysql, datetime
from market_logic_helpers import _market_side_pools, _current_side_price, _average_fill_from_logs
from fail import _fail

def create_m(cursor, db, user_id, event_id, question, description): 

    try:
        # Check if user has permission to create market in the event (or is organization leader)
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
            return _fail("permission", "You do not have permission to create a market in that event.")
        
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

        return cursor.lastrowid

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to create market: {e}")

def designate_m_token(cursor, db, user_id, market_id, token_id): 

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

def designate_m_result(cursor, db, user_id, market_id, result): 

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
            return _fail("permission", "Only the market creator or organization leader can designate market results.")

        # Check if the result is valid
        if result not in (True, False):
            return _fail("validation", "Market result must be True or False.")

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
            payout_result = do_m_payout(cursor, db, user_id, market_id, token_row[0])
            if isinstance(payout_result, dict) and payout_result.get("ok") is False:
                return payout_result

        db.commit()

        return True

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to designate the market result: {e}")

def designate_m_contraint(cursor, db, user_id, market_id, constraint_id, value):

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

def designate_m_open_to_as(cursor, db, user_id, market_id, role_id, as_id): 
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

def do_m_transaction(cursor, db, user_id, market_id, token_id, side, qty, transaction_id, transaction_type): 

    try:

        # Check if user has permission to trade in the market (or is organization leader)
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
            return _fail("permission", "You do not have permission to trade in that market.")

        # Check if the market is open and the token is valid
        cursor.execute(
            """
            SELECT 1
            FROM market
            WHERE id = %s AND is_open = TRUE
            """,
            (market_id,),
        )

        if cursor.fetchone() is None:
            return _fail("not_open", "That market is not open for trading.")

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

        # Check if the user is following event/market constraints
        if qty <= 0:
            return _fail("validation", "Trade quantity must be greater than zero.")

        if transaction_type not in ("BUY", "SELL"):
            return _fail("validation", "Transaction type must be BUY or SELL.")

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

        # Execute the transaction in the database, updating liquidity and user balances accordingly
        normalized_side = bool(side)

        total_token_value, normalized_price = _average_fill_from_logs(
            cursor, market_id, normalized_side, qty, transaction_type
        )

        token_delta = total_token_value
        stock_delta = -token_delta if transaction_type == "BUY" else token_delta
        ticket_delta = qty if transaction_type == "BUY" else -qty

        if transaction_type == "BUY":
            cursor.execute(
                """
                SELECT qty
                FROM user_token_stock
                WHERE token_id = %s AND user_id = %s
                """,
                (token_id, user_id),
            )

            current_stock = cursor.fetchone()

            if current_stock is None or current_stock[0] < token_delta:
                return _fail("precondition", "The user does not have enough token stock to buy those shares.")

        else:

            cursor.execute(
                """
                SELECT qty
                FROM user_market_ticket
                WHERE user_id = %s AND market_id = %s AND side = %s
                """,
                (user_id, market_id, normalized_side),
            )

            current_tickets = cursor.fetchone()

            if current_tickets is None or current_tickets[0] < qty:
                return _fail("precondition", "The user does not have enough market tickets to sell.")

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

        # Trade execution updates both spendable tokens and current ticket inventory.
        cursor.execute(
            """
            INSERT INTO user_token_stock (token_id, user_id, qty)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)
            """,
            (token_id, user_id, stock_delta),
        )

        cursor.execute(
            """
            INSERT INTO user_market_ticket (user_id, market_id, side, qty)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)
            """,
            (user_id, market_id, normalized_side, ticket_delta),
        )

        db.commit()
        
        return transaction_id

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to execute the market transaction: {e}")

def do_m_payout(cursor, db, user_id, market_id, token_id): 

    try:
        # Check if user has permission to execute payout (or is organization leader)
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

        # Check if payout has already been completed for this market/token pair.
        cursor.execute(
            """
            SELECT 1
            FROM market_payout
            WHERE market_id = %s AND token_id = %s
            """,
            (market_id, token_id),
        )

        if cursor.fetchone() is not None:

            return True

        # Pay one full token (100 units) for every winning ticket still held at settlement.
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

            cursor.execute(
                """
                INSERT INTO user_token_stock (token_id, user_id, qty)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)
                """,
                (token_id, winner_user_id, payout_amount),
            )

        # Clear out settled ticket inventory so resolved markets do not retain active positions.
        cursor.execute(
            """
            DELETE FROM user_market_ticket
            WHERE market_id = %s
            """,
            (market_id,),
        )

        cursor.execute(
            """
            INSERT INTO market_payout (market_id, token_id)
            VALUES (%s, %s)
            """,
            (market_id, token_id),
        )

        db.commit()

        return True

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to execute the market payout: {e}")
