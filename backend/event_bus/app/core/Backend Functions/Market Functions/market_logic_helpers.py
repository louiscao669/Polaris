"""Shared pricing helpers for market reads/writes."""

from __future__ import annotations

# Small symmetric prior in the same units as pool totals (amt * price sums).
# Stops implied YES/NO from pegging at 99/1 after the first one-sided trade while
# converging to the raw pool ratio as real liquidity grows.
_POOL_PRIOR = 25


def coerce_market_side_bool(value) -> bool:
    """Interpret ``side`` from MySQL/API.

    PyMySQL may return BOOLEAN as ``int`` 0/1 or as ``bytes`` (e.g. ``b'\\x00'``).
    Never use bare ``bool(value)`` on DB values: ``bool(b'\\x00')`` is ``True``.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (bytes, bytearray)):
        if len(value) == 0:
            return False
        return int.from_bytes(value, byteorder="big", signed=False) != 0
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        s = value.strip().lower()
        if s in ("0", "false", "no", "n"):
            return False
        if s in ("1", "true", "yes", "y"):
            return True
    return bool(value)


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

        if coerce_market_side_bool(side_value):
            yes_pool += signed_value
        else:
            no_pool += signed_value

    return max(yes_pool, 0), max(no_pool, 0)


def _current_side_price(yes_pool, no_pool, side):
    # When there is no existing support on either side, the market opens at 50/50.
    raw_y = max(int(yes_pool), 0)
    raw_n = max(int(no_pool), 0)
    if raw_y + raw_n <= 0:
        return 50

    y = raw_y + _POOL_PRIOR
    n = raw_n + _POOL_PRIOR
    total = y + n
    yes_price = (100 * y + (total // 2)) // total
    yes_price = max(1, min(99, yes_price))
    no_price = 100 - yes_price

    return yes_price if coerce_market_side_bool(side) else no_price


def _average_fill_from_logs(cursor, market_id, side, qty, transaction_type):
    # Simulate the fill one ticket at a time so larger trades move the ratio-based price.
    yes_pool, no_pool = _market_side_pools(cursor, market_id)
    total_value = 0

    for _ in range(qty):
        ticket_price = _current_side_price(yes_pool, no_pool, side)
        total_value += ticket_price

        if side:
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
