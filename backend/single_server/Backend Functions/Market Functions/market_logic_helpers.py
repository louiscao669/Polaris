def _market_side_pools(cursor, market_id):
    # Derive each side's active support directly from executed transaction logs.
    cursor.execute(
        """
        SELECT side, type, COALESCE(SUM(amt * price), 0) AS total_value
        FROM market_transaction
        WHERE market_id = %s
        GROUP BY side, type
        """,
        (market_id,),
    )

    yes_pool = 0
    no_pool = 0

    for side_value, transaction_type, total_value in cursor.fetchall():
        signed_value = total_value if transaction_type == "BUY" else -total_value

        if bool(side_value):
            yes_pool += signed_value
        else:
            no_pool += signed_value

    return max(yes_pool, 0), max(no_pool, 0)


def _current_side_price(yes_pool, no_pool, side):
    # When there is no existing support on either side, the market opens at 50/50.
    total_pool = yes_pool + no_pool
    if total_pool <= 0:
        return 50

    yes_price = (100 * yes_pool + (total_pool // 2)) // total_pool
    yes_price = max(1, min(99, yes_price))
    no_price = 100 - yes_price

    return yes_price if bool(side) else no_price


def _average_fill_from_logs(cursor, market_id, side, qty, transaction_type):
    # Simulate the fill one ticket at a time so larger trades move the ratio-based price.
    yes_pool, no_pool = _market_side_pools(cursor, market_id)
    total_value = 0

    for _ in range(qty):
        ticket_price = _current_side_price(yes_pool, no_pool, side)
        total_value += ticket_price

        if bool(side):
            if transaction_type == "BUY":
                yes_pool += ticket_price
            else:
                yes_pool = max(0, yes_pool - ticket_price)
        else:
            if transaction_type == "BUY":
                no_pool += ticket_price
            else:
                no_pool = max(0, no_pool - ticket_price)

    average_price = (total_value + (qty // 2)) // qty
    return total_value, average_price
