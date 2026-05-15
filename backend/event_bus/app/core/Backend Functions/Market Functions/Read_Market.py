from typing import Any, Optional

import pymysql, datetime
from market_logic_helpers import (
    _average_fill_from_logs,
    _current_side_price,
    _market_side_pools,
    coerce_market_side_bool,
)
from fail import _fail, _log_result
try:
    from app.read_cache import (
        cache_enabled_for_request,
        event_markets_key,
        market_detail_key,
        market_read_cache,
        market_stats_key,
        metadata_read_cache,
    )
except ImportError:
    from backend.event_bus.app.read_cache import (
        cache_enabled_for_request,
        event_markets_key,
        market_detail_key,
        market_read_cache,
        market_stats_key,
        metadata_read_cache,
    )
try:
    from app.database import get_connection_reader, get_connection_writer
except ImportError:
    from backend.event_bus.app.database import get_connection_reader, get_connection_writer


def _market_stats_db():
    """Use primary when client sends ``cache_mode=bypass`` (read-your-writes after trades)."""
    return (
        get_connection_writer()
        if not cache_enabled_for_request()
        else get_connection_reader()
    )


def _json_ts(value: Any) -> Optional[str]:
    """Serialize MySQL datetimes for JSON so browsers parse ``ts`` reliably."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _cache_success(key: str, value: Any, ttl_seconds: float) -> Any:
    if isinstance(value, dict) and value.get("ok") is False:
        return value
    market_read_cache.set(key, value, ttl_seconds)
    return value


def quote_m(data: dict[str, Any]):
    user_id = data.get("user_id")
    market_id = data.get("market_id")
    token_id = data.get("token_id")
    side = data.get("side")
    qty = data.get("qty")
    transaction_type = data.get("transaction_type")

    if user_id is None:
        result = _fail("validation", "quote_m payload is missing required field 'user_id'.")
        _log_result("quote_m", result)
        return result
    if market_id is None:
        result = _fail("validation", "quote_m payload is missing required field 'market_id'.")
        _log_result("quote_m", result)
        return result
    if token_id is None:
        result = _fail("validation", "quote_m payload is missing required field 'token_id'.")
        _log_result("quote_m", result)
        return result
    if side is None:
        result = _fail("validation", "quote_m payload is missing required field 'side'.")
        _log_result("quote_m", result)
        return result
    if qty is None:
        result = _fail("validation", "quote_m payload is missing required field 'qty'.")
        _log_result("quote_m", result)
        return result
    if transaction_type is None:
        result = _fail("validation", "quote_m payload is missing required field 'transaction_type'.")
        _log_result("quote_m", result)
        return result

    with get_connection_reader() as db:
        cursor = db.cursor()
        result = _quote_m(cursor, db, user_id, market_id, token_id, side, qty, transaction_type)
    _log_result("quote_m", result)
    return result

def stats_m_liquidity(data: dict[str, Any]): 
    user_id = data.get("user_id")
    market_id = data.get("market_id")

    if user_id is None:
        result = _fail("validation", "stats_m_liquidity payload is missing required field 'user_id'.")
        _log_result("stats_m_liquidity", result)
        return result

    if market_id is None:
        result = _fail("validation", "stats_m_liquidity payload is missing required field 'market_id'.")
        _log_result("stats_m_liquidity", result)
        return result

    cache_key = market_stats_key(
        market_id=int(market_id),
        user_id=int(user_id),
        stat_name="liquidity",
    )
    cached = market_read_cache.get(cache_key)
    if cached is not None:
        _log_result("stats_m_liquidity", cached)
        return cached

    with _market_stats_db() as db:
        cursor = db.cursor()
        result = _stats_m_liquidity(cursor, db, user_id, market_id)
    result = _cache_success(cache_key, result, 3.0)
    _log_result("stats_m_liquidity", result)
    return result


def _stats_m_liquidity(cursor, db, user_id, market_id): 

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
            return _fail("permission", "You do not have permission to view market liquidity.")

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

        return _fail("validation", f"Failed to retrieve market liquidity statistics: {e}")


def _quote_m(cursor, db, user_id, market_id, token_id, side, qty, transaction_type):
    try:
        qty = int(qty)
        token_id = int(token_id)
        if qty <= 0:
            return _fail("validation", "Trade quantity must be greater than zero.")
        if transaction_type not in ("BUY", "SELL"):
            return _fail("validation", "Transaction type must be BUY or SELL.")

        normalized_side = coerce_market_side_bool(side)

        cursor.execute(
            """
            SELECT m.is_open
            FROM market m
            WHERE m.id = %s
            """,
            (market_id,),
        )
        market_row = cursor.fetchone()
        if market_row is None:
            return _fail("validation", "That market does not exist.")
        if market_row[0] == 0:
            return _fail("not_open", "That market is not open for trading.")

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

        total_token_value, average_price = _average_fill_from_logs(
            cursor, market_id, normalized_side, qty, transaction_type
        )

        return {
            "market_id": int(market_id),
            "token_id": token_id,
            "transaction_type": transaction_type,
            "side": normalized_side,
            "qty": qty,
            "total_token_value": int(total_token_value),
            "average_price": int(average_price),
        }
    except Exception as e:
        print(f"Failed to quote market transaction: {e}")
        return _fail("validation", f"Failed to quote market transaction: {e}")

def stats_m_time_focus(data: dict[str, Any]): 
    user_id = data.get("user_id")
    market_id = data.get("market_id")

    if user_id is None:
        result = _fail("validation", "stats_m_time_focus payload is missing required field 'user_id'.")
        _log_result("stats_m_time_focus", result)
        return result

    if market_id is None:
        result = _fail("validation", "stats_m_time_focus payload is missing required field 'market_id'.")
        _log_result("stats_m_time_focus", result)
        return result

    with get_connection_reader() as db:
        cursor = db.cursor()
        result = _stats_m_time_focus(cursor, db, user_id, market_id)
    _log_result("stats_m_time_focus", result)
    return result


def _stats_m_time_focus(cursor, db, user_id, market_id): 

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
            return _fail("permission", "You do not have permission to view market time focus statistics.")

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

        return _fail("validation", f"Failed to retrieve market time focus statistics: {e}")

def stats_m_whales(data: dict[str, Any]): 
    user_id = data.get("user_id")
    market_id = data.get("market_id")

    if user_id is None:
        result = _fail("validation", "stats_m_whales payload is missing required field 'user_id'.")
        _log_result("stats_m_whales", result)
        return result

    if market_id is None:
        result = _fail("validation", "stats_m_whales payload is missing required field 'market_id'.")
        _log_result("stats_m_whales", result)
        return result

    with get_connection_reader() as db:
        cursor = db.cursor()
        result = _stats_m_whales(cursor, db, user_id, market_id)
    _log_result("stats_m_whales", result)
    return result


def _stats_m_whales(cursor, db, user_id, market_id): 

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
            return _fail("permission", "You do not have permission to view market whale statistics.")

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

        return _fail("validation", f"Failed to retrieve market whale statistics: {e}")

def points_m(data: dict[str, Any]): 
    user_id = data.get("user_id")
    market_id = data.get("market_id")
    span = data.get("span")
    hours = data.get("hours")

    if user_id is None:
        result = _fail("validation", "points_m payload is missing required field 'user_id'.")
        _log_result("points_m", result)
        return result

    if market_id is None:
        result = _fail("validation", "points_m payload is missing required field 'market_id'.")
        _log_result("points_m", result)
        return result

    if span is None:
        result = _fail("validation", "points_m payload is missing required field 'span'.")
        _log_result("points_m", result)
        return result

    if hours is not None:
        try:
            hours = int(hours)
        except (TypeError, ValueError):
            result = _fail("validation", "points_m payload field 'hours' must be an integer.")
            _log_result("points_m", result)
            return result
        if hours <= 0:
            result = _fail("validation", "points_m payload field 'hours' must be greater than zero.")
            _log_result("points_m", result)
            return result

    with _market_stats_db() as db:
        cursor = db.cursor()
        result = _points_m(cursor, db, user_id, market_id, span, hours)
    _log_result("points_m", result)
    return result


def _points_m(cursor, db, user_id, market_id, span, hours=None): 

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
            return _fail("permission", "You do not have permission to view market points.")

        points = []
        yes_pool = 0
        no_pool = 0

        if hours is not None:
            # Seed pools from all earlier trades so shorter windows still chart the real
            # price level at the beginning of the selected period.
            cursor.execute(
                """
                SELECT side, type, COALESCE(SUM(amt * price), 0) AS total_value
                FROM market_transaction
                WHERE market_id = %s
                  AND ts < (UTC_TIMESTAMP() - INTERVAL %s HOUR)
                GROUP BY side, type
                """,
                (market_id, hours),
            )

            for side_value, transaction_type, total_value in cursor.fetchall():
                signed_value = total_value if transaction_type == "BUY" else -total_value
                if coerce_market_side_bool(side_value):
                    yes_pool += signed_value
                else:
                    no_pool += signed_value

            yes_pool = max(yes_pool, 0)
            no_pool = max(no_pool, 0)

            cursor.execute(
                """
                SELECT transaction_id, ts, side, type, amt, price
                FROM (
                    SELECT transaction_id, ts, side, type, amt, price
                    FROM market_transaction
                    WHERE market_id = %s
                      AND ts >= (UTC_TIMESTAMP() - INTERVAL %s HOUR)
                    ORDER BY ts DESC, transaction_id DESC
                    LIMIT %s
                ) recent_points
                ORDER BY ts ASC, transaction_id ASC
                """,
                (market_id, hours, span),
            )
        else:
            # Build chart points directly from executed trades so graph visualizations follow market history.
            cursor.execute(
                """
                SELECT transaction_id, ts, side, type, amt, price
                FROM (
                    SELECT transaction_id, ts, side, type, amt, price
                    FROM market_transaction
                    WHERE market_id = %s
                    ORDER BY ts DESC, transaction_id DESC
                    LIMIT %s
                ) recent_points
                ORDER BY ts ASC, transaction_id ASC
                """,
                (market_id, span),
            )

        for transaction_id, ts, side_value, transaction_type, amt, price in cursor.fetchall():
            trade_value = amt * price

            if coerce_market_side_bool(side_value):
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
                    "ts": _json_ts(ts),
                    "side": coerce_market_side_bool(side_value),
                    "type": transaction_type,
                    "qty": int(amt) if amt is not None else 0,
                    "price": int(price) if price is not None else 0,
                    "yes_price": int(_current_side_price(yes_pool, no_pool, True)),
                    "no_price": int(_current_side_price(yes_pool, no_pool, False)),
                    "yes_pool": int(yes_pool),
                    "no_pool": int(no_pool),
                }
            )

        # Hour-limited windows can return zero rows even when the market has older
        # trades (everything outside the window). Return one snapshot so the UI is
        # not a blank chart while liquidity still shows trade_count > 0.
        if not points and hours is not None:
            cursor.execute(
                "SELECT COUNT(*) FROM market_transaction WHERE market_id = %s",
                (market_id,),
            )
            n_tx_row = cursor.fetchone()
            n_tx = int(n_tx_row[0]) if n_tx_row and n_tx_row[0] is not None else 0
            if n_tx > 0:
                gy, gn = _market_side_pools(cursor, market_id)
                cursor.execute(
                    """
                    SELECT COALESCE(MAX(ts), UTC_TIMESTAMP())
                    FROM market_transaction
                    WHERE market_id = %s
                    """,
                    (market_id,),
                )
                last_ts = cursor.fetchone()[0]
                points.append(
                    {
                        "transaction_id": 0,
                        "ts": _json_ts(last_ts),
                        "side": True,
                        "type": "SNAPSHOT",
                        "qty": 0,
                        "price": 0,
                        "yes_price": int(_current_side_price(gy, gn, True)),
                        "no_price": int(_current_side_price(gy, gn, False)),
                        "yes_pool": int(gy),
                        "no_pool": int(gn),
                    }
                )

        return points

    except Exception as e:
        print(f"Failed to retrieve market points: {e}")

        return _fail("validation", f"Failed to retrieve market points: {e}")

def stats_m_trade_distribution(data: dict[str, Any]):
    user_id = data.get("user_id")
    market_id = data.get("market_id")

    if user_id is None:
        result = _fail("validation", "stats_m_trade_distribution payload is missing required field 'user_id'.")
        _log_result("stats_m_trade_distribution", result)
        return result

    if market_id is None:
        result = _fail("validation", "stats_m_trade_distribution payload is missing required field 'market_id'.")
        _log_result("stats_m_trade_distribution", result)
        return result

    cache_key = market_stats_key(
        market_id=int(market_id),
        user_id=int(user_id),
        stat_name="trade_distribution",
    )
    cached = market_read_cache.get(cache_key)
    if cached is not None:
        _log_result("stats_m_trade_distribution", cached)
        return cached

    with get_connection_reader() as db:
        cursor = db.cursor()
        result = _stats_m_trade_distribution(cursor, db, user_id, market_id)
    result = _cache_success(cache_key, result, 5.0)
    _log_result("stats_m_trade_distribution", result)
    return result


def _stats_m_trade_distribution(cursor, db, user_id, market_id):

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
            return _fail("permission", "You do not have permission to view market trade distribution statistics.")

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

        return _fail("validation", f"Failed to retrieve market trade distribution statistics: {e}")

def stats_m_window_comparison(data: dict[str, Any]):
    user_id = data.get("user_id")
    market_id = data.get("market_id")
    hours = data.get("hours")

    if user_id is None:
        result = _fail("validation", "stats_m_window_comparison payload is missing required field 'user_id'.")
        _log_result("stats_m_window_comparison", result)
        return result

    if market_id is None:
        result = _fail("validation", "stats_m_window_comparison payload is missing required field 'market_id'.")
        _log_result("stats_m_window_comparison", result)
        return result

    if hours is None:
        result = _fail("validation", "stats_m_window_comparison payload is missing required field 'hours'.")
        _log_result("stats_m_window_comparison", result)
        return result

    cache_key = market_stats_key(
        market_id=int(market_id),
        user_id=int(user_id),
        stat_name="window_comparison",
        extra=str(hours),
    )
    cached = market_read_cache.get(cache_key)
    if cached is not None:
        _log_result("stats_m_window_comparison", cached)
        return cached

    with get_connection_reader() as db:
        cursor = db.cursor()
        result = _stats_m_window_comparison(cursor, db, user_id, market_id, hours)
    result = _cache_success(cache_key, result, 5.0)
    _log_result("stats_m_window_comparison", result)
    return result


def _stats_m_window_comparison(cursor, db, user_id, market_id, hours):

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
            return _fail("permission", "You do not have permission to view market window comparison statistics.")

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

        return _fail("validation", f"Failed to retrieve market window comparison statistics: {e}")

def read_e_markets(data: dict[str, Any]):
    user_id = data.get("user_id")
    event_id = data.get("event_id")

    if user_id is None:
        result = _fail("validation", "read_e_markets payload is missing required field 'user_id'.")
        _log_result("read_e_markets", result)
        return result

    if event_id is None:
        result = _fail("validation", "read_e_markets payload is missing required field 'event_id'.")
        _log_result("read_e_markets", result)
        return result

    cache_key = event_markets_key(
        event_id=int(event_id),
        user_id=int(user_id),
    )
    cached = metadata_read_cache.get(cache_key)
    if cached is not None:
        _log_result("read_e_markets", cached)
        return cached

    with get_connection_writer() as db:
        cursor = db.cursor()
        result = _read_e_markets(cursor, db, user_id, event_id)
    if not (isinstance(result, dict) and result.get("ok") is False):
        metadata_read_cache.set(cache_key, result)
    _log_result("read_e_markets", result)
    return result


def _read_e_markets(cursor, db, user_id, event_id):
    try:
        # Check if the user can read the event and determine organization access
        cursor.execute(
            """
            SELECT
                e.org_id,
                MAX(CASE WHEN ol.user_id = %s THEN 1 ELSE 0 END) AS is_leader,
                MAX(uor.role_id) AS role_id
            FROM events e
            LEFT JOIN organization_leader ol ON e.org_id = ol.org_id
            LEFT JOIN user_org_role uor ON e.org_id = uor.org_id AND uor.user_id = %s
            WHERE e.event_id = %s
            GROUP BY e.event_id, e.org_id
            """,
            (user_id, user_id, event_id),
        )
        access = cursor.fetchone()
        if access is None:
            return _fail("validation", "That event does not exist.")

        if access[1] == 0 and access[2] is None:
            return _fail("permission", "You do not have permission to read markets in that event.")

        cursor.execute(
            """
            SELECT 1
            FROM event_open_to
            WHERE event_id = %s
            LIMIT 1
            """,
            (event_id,),
        )
        event_has_visibility_rules = cursor.fetchone() is not None

        if access[1] == 0 and event_has_visibility_rules:
            cursor.execute(
                """
                SELECT 1
                FROM event_open_to
                WHERE event_id = %s AND org_id = %s AND role_id = %s
                """,
                (event_id, access[0], access[2]),
            )
            if cursor.fetchone() is None:
                return _fail("permission", "You do not have permission to read markets in that event.")

        # Read every market in the event that is visible to the user
        cursor.execute(
            """
            SELECT
                m.id,
                m.question,
                m.is_open,
                m.created_by,
                m.created_at,
                m.close_at
            FROM market m
            WHERE m.event_id = %s
              AND (
                    %s = 1
                    OR m.created_by = %s
                    OR NOT EXISTS (
                        SELECT 1
                        FROM market_open_to_as mota
                        WHERE mota.market_id = m.id
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM market_open_to_as mota
                        WHERE mota.market_id = m.id
                          AND mota.org_id = %s
                          AND mota.role_id = %s
                    )
              )
            ORDER BY m.id ASC
            """,
            (event_id, access[1], user_id, access[0], access[2]),
        )

        return [
            {
                "market_id": row[0],
                "question": row[1],
                "is_open": bool(row[2]),
                "created_by": row[3],
                "created_at": row[4],
                "close_at": row[5],
            }
            for row in cursor.fetchall()
        ]

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to read the event's markets: {e}")

def read_m(data: dict[str, Any]):
    user_id = data.get("user_id")
    market_id = data.get("market_id")

    if user_id is None:
        result = _fail("validation", "read_m payload is missing required field 'user_id'.")
        _log_result("read_m", result)
        return result

    if market_id is None:
        result = _fail("validation", "read_m payload is missing required field 'market_id'.")
        _log_result("read_m", result)
        return result

    cache_key = market_detail_key(
        market_id=int(market_id),
        user_id=int(user_id),
    )
    cached = metadata_read_cache.get(cache_key)
    if cached is not None:
        _log_result("read_m", cached)
        return cached

    with get_connection_writer() as db:
        cursor = db.cursor()
        result = _read_m(cursor, db, user_id, market_id)
    if not (isinstance(result, dict) and result.get("ok") is False):
        metadata_read_cache.set(cache_key, result)
    _log_result("read_m", result)
    return result


def _read_m(cursor, db, user_id, market_id):
    try:
        # Check if the user can read the market
        cursor.execute(
            """
            SELECT
                m.event_id,
                m.question,
                m.is_open,
                m.created_by,
                m.created_at,
                m.close_at,
                e.org_id,
                MAX(CASE WHEN ol.user_id = %s THEN 1 ELSE 0 END) AS is_leader,
                MAX(uor.role_id) AS role_id
            FROM market m
            JOIN events e ON m.event_id = e.event_id
            LEFT JOIN organization_leader ol ON e.org_id = ol.org_id
            LEFT JOIN user_org_role uor ON e.org_id = uor.org_id AND uor.user_id = %s
            WHERE m.id = %s
            GROUP BY m.id, m.event_id, m.question, m.is_open, m.created_by, m.created_at, m.close_at, e.org_id
            """,
            (user_id, user_id, market_id),
        )
        market = cursor.fetchone()
        if market is None:
            return _fail("validation", "That market does not exist.")

        if market[7] == 0 and market[8] is None and market[3] != user_id:
            return _fail("permission", "You do not have permission to read that market.")

        cursor.execute(
            """
            SELECT 1
            FROM event_open_to
            WHERE event_id = %s
            LIMIT 1
            """,
            (market[0],),
        )
        event_has_visibility_rules = cursor.fetchone() is not None

        if market[7] == 0 and market[3] != user_id and event_has_visibility_rules:
            cursor.execute(
                """
                SELECT 1
                FROM event_open_to
                WHERE event_id = %s AND org_id = %s AND role_id = %s
                """,
                (market[0], market[6], market[8]),
            )
            if cursor.fetchone() is None:
                return _fail("permission", "You do not have permission to read that market.")

        cursor.execute(
            """
            SELECT 1
            FROM market_open_to_as
            WHERE market_id = %s
            LIMIT 1
            """,
            (market_id,),
        )
        market_has_visibility_rules = cursor.fetchone() is not None

        if market[7] == 0 and market[3] != user_id and market_has_visibility_rules:
            cursor.execute(
                """
                SELECT 1
                FROM market_open_to_as
                WHERE market_id = %s AND org_id = %s AND role_id = %s
                """,
                (market_id, market[6], market[8]),
            )
            if cursor.fetchone() is None:
                return _fail("permission", "You do not have permission to read that market.")

        # Read market tokens, constraints, access roles, and result
        cursor.execute(
            """
            SELECT token_id
            FROM market_tokens_allowed
            WHERE market_id = %s
            ORDER BY token_id ASC
            """,
            (market_id,),
        )
        tokens_allowed = [row[0] for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT constraint_id, constraint_value
            FROM market_constraint
            WHERE market_id = %s
            ORDER BY constraint_id ASC
            """,
            (market_id,),
        )
        constraints = [
            {
                "constraint_id": row[0],
                "value": row[1],
            }
            for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT role_id, as_id
            FROM market_open_to_as
            WHERE market_id = %s
            ORDER BY role_id ASC
            """,
            (market_id,),
        )
        access_roles = [
            {
                "role_id": row[0],
                "as_id": row[1],
            }
            for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT outcome, resolved_at
            FROM market_result
            WHERE market_id = %s
            """,
            (market_id,),
        )
        result_row = cursor.fetchone()

        return {
            "market_id": market_id,
            "event_id": market[0],
            "question": market[1],
            "is_open": bool(market[2]),
            "created_by": market[3],
            "created_at": market[4],
            "close_at": market[5],
            "organization_id": market[6],
            "is_leader": bool(market[7]),
            "role_id": market[8],
            "tokens_allowed": tokens_allowed,
            "constraints": constraints,
            "access_roles": access_roles,
            "result": None if result_row is None else {"outcome": bool(result_row[0]), "resolved_at": result_row[1]},
        }

    except Exception as e:
        # prevent sql transaction from partially executing and leaving the database in an inconsistent state
        db.rollback()
        print(f"Transaction failed, rolled back: {e}")

        return _fail("validation", f"Unable to read the market: {e}")
