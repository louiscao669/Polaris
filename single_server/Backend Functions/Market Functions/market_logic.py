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

        if cursor.fetchone() is not None:

            return True

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
        if result not in (True, False, 0, 1):

            return False

        # Check if the result has already been designated for the market
        cursor.execute(
            """
            SELECT 1
            FROM market_result
            WHERE market_id = %s
            """,
            (market_id,),
        )

        if cursor.fetchone() is not None:

            return True

        # Designate the result for the market in the database
        normalized_result = bool(result)

        cursor.execute(
            """
            INSERT INTO market_result (market_id, outcome)
            VALUES (%s, %s)
            """,
            (market_id, normalized_result),
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

        # Close the market in the database
        cursor.execute(
            """
            UPDATE market
            SET is_open = FALSE, close_at = NOW()
            WHERE id = %s
            """,
            (market_id,),
        )

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
            SELECT 1
            FROM market_constraint
            WHERE market_id = %s AND constraint_id = %s
            """,
            (market_id, constraint_id),
        )

        if cursor.fetchone() is not None:

            return True

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

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return False

def designate_m_open_to_as(cursor, db, user_id, market_id, role_id, as_id): 

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
            SELECT 1
            FROM market_open_to_as
            WHERE market_id = %s AND org_id = %s AND role_id = %s
            """,
            (market_id, organization_id, role_id),
        )

        if cursor.fetchone() is not None:

            return True

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

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return False


def do_m_transaction(cursor, db, user_id, market_id, token_id, type, side, qty): 

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

        # Execute the transaction in the database, updating liquidity and user balances accordingly
        cursor.execute(
            """
            SELECT COALESCE(MAX(transaction_id), 0) + 1
            FROM market_transaction
            WHERE market_id = %s
            """,
            (market_id,),
        )

        transaction_id = cursor.fetchone()[0]
        price = int(type)
        normalized_side = bool(side)

        ### get live current timestamp
        ts = datetime.now()

        cursor.execute(
            """
            INSERT INTO market_transaction (transaction_id, market_id, token_id, amt, ts, user_id, price, side)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (transaction_id, market_id, token_id, qty, ts, user_id, price, normalized_side),
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

        # Check if the user has already been paid out for the market and token
        cursor.execute(
            """
            SELECT 1
            FROM market_payout
            WHERE market_id = %s AND token_id = %s AND user_id = %s
            """,
            (market_id, token_id, user_id),
        )

        if cursor.fetchone() is not None:

            return True

        # Calculate and distribute winnings to users based on their token holdings
        cursor.execute(
            """
            SELECT user_id, shares
            FROM user_market_shares
            WHERE market_id = %s AND outcome = %s AND shares > 0
            """,
            (market_id, winning_outcome),
        )

        winners = cursor.fetchall()
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

        # Retrieve and return liquidity statistics for the market
        cursor.execute(
            """
            SELECT COALESCE(SUM(liquidity), 0)
            FROM market_price_snapshot
            WHERE market_id = %s
            """,
            (market_id,),
        )

        return cursor.fetchone()[0]

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

        # Retrieve and return time focus statistics for the market
        cursor.execute(
            """
            SELECT ts, price, liquidity
            FROM market_price_snapshot
            WHERE market_id = %s
            ORDER BY ts DESC
            LIMIT 10
            """,
            (market_id,),
        )

        return cursor.fetchall()

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

        # Retrieve and return whale statistics for the market
        cursor.execute(
            """
            SELECT user_id, SUM(shares) AS total_shares
            FROM user_market_shares
            WHERE market_id = %s
            GROUP BY user_id
            ORDER BY total_shares DESC
            LIMIT 5
            """,
            (market_id,),
        )

        return cursor.fetchall()

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

        # Retrieve and return points for the market over the specified time span
        cursor.execute(
            """
            SELECT ts, outcome_id, price, liquidity
            FROM market_price_snapshot
            WHERE market_id = %s
            ORDER BY ts DESC
            LIMIT %s
            """,
            (market_id, span),
        )

        return cursor.fetchall()

    except Exception as e:
        print(f"Failed to retrieve market points: {e}")

        return None