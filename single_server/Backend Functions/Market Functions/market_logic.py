import pymysql, datetime
from market_logic_helpers import _market_side_pools, _current_side_price, _average_fill_from_logs

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

            return None
        
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

        return None

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

            return None

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

            return None

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

        return False

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return False

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
            return False

        # Check if the result is valid
        if result not in (True, False):

            return False

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

                return False

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
            do_m_payout(cursor, db, user_id, market_id, token_row[0])

        db.commit()

        return True

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return False


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

            return False

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

            return False

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

            return False

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

        return False

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return False

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

            return False

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

            return False

        cursor.execute(
            """
            SELECT 1
            FROM market_as
            WHERE as_code = %s
            """,
            (as_id,),
        )

        if cursor.fetchone() is None:

            return False

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
                
            return False

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

        return False

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return False


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

            return False

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

            return False

        cursor.execute(
            """
            SELECT 1
            FROM market_tokens_allowed
            WHERE market_id = %s AND token_id = %s
            """,
            (market_id, token_id),
        )

        if cursor.fetchone() is None:

            return False

        # Check if the user is following event/market constraints
        if qty <= 0:

            return False

        if transaction_type not in ("BUY", "SELL"):

            return False

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

                return False

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

                return False

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

        return False


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

            return False

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

            return False

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

            return False

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

        return False


def stats_m_liquidity(cursor, db, user_id, market_id): 

    try:
        # Check if user has permission to view market statistics (or is organization leader)
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

            return None

        # Derive current side support and price directly from the transaction log.
        yes_pool, no_pool = _market_side_pools(cursor, market_id)
        yes_price = _current_side_price(yes_pool, no_pool, True)
        no_price = _current_side_price(yes_pool, no_pool, False)

        cursor.execute(
            """
            SELECT COALESCE(SUM(qty), 0)
            FROM user_market_ticket
            WHERE market_id = %s
            """,
            (market_id,),
        )
        open_tickets = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(amt * price), 0)
            FROM market_transaction
            WHERE market_id = %s
            """,
            (market_id,),
        )
        trade_count, gross_volume = cursor.fetchone()

        return {
            "market_id": market_id,
            "yes_pool": yes_pool,
            "no_pool": no_pool,
            "total_pool": yes_pool + no_pool,
            "yes_price": yes_price,
            "no_price": no_price,
            "open_tickets": open_tickets,
            "trade_count": trade_count,
            "gross_volume": gross_volume,
        }

    except Exception as e:
        print(f"Failed to retrieve market liquidity statistics: {e}")

        return None


def stats_m_time_focus(cursor, db, user_id, market_id): 

    try:
        # Check if user has permission to view market statistics (or is organization leader)
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

            return None

        # Surface the most recent transaction activity so charting clients can follow short-term shifts.
        cursor.execute(
            """
            SELECT transaction_id, ts, type, side, amt, price, (amt * price) AS token_value
            FROM market_transaction
            WHERE market_id = %s
            ORDER BY ts DESC, transaction_id DESC
            LIMIT 10
            """,
            (market_id,),
        )

        recent_trades = [
            {
                "transaction_id": row[0],
                "ts": row[1],
                "type": row[2],
                "side": bool(row[3]),
                "qty": row[4],
                "price": row[5],
                "token_value": row[6],
            }
            for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT
                COUNT(*),
                COALESCE(SUM(CASE WHEN type = 'BUY' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN type = 'SELL' THEN 1 ELSE 0 END), 0),
                COALESCE(AVG(price), 0),
                COALESCE(SUM(amt * price), 0)
            FROM market_transaction
            WHERE market_id = %s
              AND ts >= (NOW() - INTERVAL 24 HOUR)
            """,
            (market_id,),
        )
        total_trades, buy_trades, sell_trades, avg_price, traded_value = cursor.fetchone()

        return {
            "market_id": market_id,
            "window_hours": 24,
            "recent_trades": recent_trades,
            "trade_count": total_trades,
            "buy_trades": buy_trades,
            "sell_trades": sell_trades,
            "avg_price": int(avg_price) if avg_price is not None else 0,
            "traded_value": traded_value,
        }

    except Exception as e:
        print(f"Failed to retrieve market time focus statistics: {e}")

        return None


def stats_m_whales(cursor, db, user_id, market_id): 

    try:
        # Check if user has permission to view market statistics (or is organization leader)
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

            return None

        # Rank participants by current ticket concentration and total token volume committed.
        cursor.execute(
            """
            SELECT
                umt.user_id,
                COALESCE(SUM(umt.qty), 0) AS total_tickets,
                COALESCE(SUM(CASE WHEN umt.side = TRUE THEN umt.qty ELSE 0 END), 0) AS yes_tickets,
                COALESCE(SUM(CASE WHEN umt.side = FALSE THEN umt.qty ELSE 0 END), 0) AS no_tickets,
                COALESCE((
                    SELECT SUM(mt.amt * mt.price)
                    FROM market_transaction mt
                    WHERE mt.market_id = umt.market_id
                      AND mt.user_id = umt.user_id
                ), 0) AS traded_value
            FROM user_market_ticket umt
            WHERE umt.market_id = %s
            GROUP BY umt.user_id
            ORDER BY total_tickets DESC, traded_value DESC
            LIMIT 5
            """,
            (market_id,),
        )

        whales = [
            {
                "user_id": row[0],
                "total_tickets": row[1],
                "yes_tickets": row[2],
                "no_tickets": row[3],
                "traded_value": row[4],
            }
            for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT COALESCE(SUM(qty), 0)
            FROM user_market_ticket
            WHERE market_id = %s
            """,
            (market_id,),
        )
        total_open_tickets = cursor.fetchone()[0]

        return {
            "market_id": market_id,
            "total_open_tickets": total_open_tickets,
            "whales": whales,
        }

    except Exception as e:
        print(f"Failed to retrieve market whale statistics: {e}")

        return None


def points_m(cursor, db, user_id, market_id, span): 

    try:
        # Check if user has permission to view market points (or is organization leader)
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

            return None

        # Build chart points directly from executed trades so graph visualizations follow market history.
        cursor.execute(
            """
            SELECT transaction_id, ts, side, type, amt, price
            FROM market_transaction
            WHERE market_id = %s
            ORDER BY ts ASC, transaction_id ASC
            LIMIT %s
            """,
            (market_id, span),
        )

        points = []
        yes_pool = 0
        no_pool = 0

        for transaction_id, ts, side_value, transaction_type, amt, price in cursor.fetchall():
            trade_value = amt * price

            if bool(side_value):
                if transaction_type == "BUY":
                    yes_pool += trade_value
                else:
                    yes_pool = max(0, yes_pool - trade_value)
            else:
                if transaction_type == "BUY":
                    no_pool += trade_value
                else:
                    no_pool = max(0, no_pool - trade_value)

            points.append(
                {
                    "transaction_id": transaction_id,
                    "ts": ts,
                    "side": bool(side_value),
                    "type": transaction_type,
                    "qty": amt,
                    "price": price,
                    "yes_price": _current_side_price(yes_pool, no_pool, True),
                    "no_price": _current_side_price(yes_pool, no_pool, False),
                    "yes_pool": yes_pool,
                    "no_pool": no_pool,
                }
            )

        return points

    except Exception as e:
        print(f"Failed to retrieve market points: {e}")

        return None


def stats_m_trade_distribution(cursor, db, user_id, market_id):

    try:
        # Check if user has permission to view market statistics (or is organization leader)
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

            return None

        # Bucket trades by token value so analysts can compare small, medium, and whale-sized orders.
        cursor.execute(
            """
            SELECT
                CASE
                    WHEN (amt * price) < 100 THEN 'small'
                    WHEN (amt * price) < 500 THEN 'medium'
                    ELSE 'large'
                END AS size_bucket,
                COUNT(*),
                COALESCE(SUM(amt * price), 0),
                COALESCE(AVG(amt * price), 0)
            FROM market_transaction
            WHERE market_id = %s
            GROUP BY size_bucket
            """,
            (market_id,),
        )

        return {
            row[0]: {
                "trade_count": row[1],
                "total_value": row[2],
                "avg_value": int(row[3]) if row[3] is not None else 0,
            }
            for row in cursor.fetchall()
        }

    except Exception as e:
        print(f"Failed to retrieve market trade distribution statistics: {e}")

        return None


def stats_m_window_comparison(cursor, db, user_id, market_id, hours):

    try:
        # Check if user has permission to view market statistics (or is organization leader)
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

            return None

        # Compare the current activity window to the immediately preceding window for trend analysis.
        now = datetime.datetime.now()
        current_start = now - datetime.timedelta(hours=hours)
        previous_start = current_start - datetime.timedelta(hours=hours)

        def _window_summary(start_ts, end_ts):
            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(CASE WHEN type = 'BUY' THEN amt * price ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN type = 'SELL' THEN amt * price ELSE 0 END), 0),
                    COALESCE(AVG(price), 0)
                FROM market_transaction
                WHERE market_id = %s AND ts >= %s AND ts < %s
                """,
                (market_id, start_ts, end_ts),
            )
            trade_count, buy_value, sell_value, avg_price = cursor.fetchone()
            return {
                "trade_count": trade_count,
                "buy_value": buy_value,
                "sell_value": sell_value,
                "avg_price": int(avg_price) if avg_price is not None else 0,
            }

        return {
            "market_id": market_id,
            "window_hours": hours,
            "current_window": _window_summary(current_start, now),
            "previous_window": _window_summary(previous_start, current_start),
        }

    except Exception as e:
        print(f"Failed to retrieve market window comparison statistics: {e}")

        return None
