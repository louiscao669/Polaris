"""Manual smoke test: enqueue a market transaction, then poll operation status.

If the operation ends with status failed, the worker message is usually one of:
  - "That token is not allowed in the market."  → wrong TEST_TOKEN_ID for TEST_MARKET_ID
  - "You do not have permission to trade in that market." → wrong user / role / market_open_to
  - "The user does not have enough token stock..." → BUY needs user_token_stock for that token

Set DB_* (or LEADER_DB_HOST) in .env to print DB hints before and after the request.
Use an engineer user_id + market_id 1–15 (and matching token) if your seed mirrors benchmarks.
"""

import json
import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()

TARGET_IP = os.getenv("TARGET_IP", "3.149.243.221")
BASE_URL = os.getenv("TARGET_URL", f"http://{TARGET_IP}").rstrip("/")
JWT_TOKEN = os.getenv("BENCHMARK_JWT")

TRANSACTION_URL = f"{BASE_URL}/v2/markets/transactions"


def _db_connect():
    try:
        import pymysql  # type: ignore
        from pymysql.cursors import Cursor
    except ImportError:
        return None
    host = os.getenv("DB_HOST") or os.getenv("LEADER_DB_HOST", "")
    user = os.getenv("DB_USER", "")
    password = os.getenv("DB_PASSWORD", "")
    database = os.getenv("DB_NAME", "")
    if not (host and user and database):
        return None
    port = int(os.getenv("DB_PORT", "3306"))
    ssl_ca = os.getenv("DB_SSL_CA", "").strip()
    kwargs = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "cursorclass": Cursor,
        "autocommit": True,
        "connect_timeout": 10,
    }
    if ssl_ca:
        kwargs["ssl"] = {"ca": ssl_ca}
    return pymysql.connect(**kwargs)


def print_db_hints(user_id: int, market_id: int, token_id: int) -> None:
    if os.getenv("TEST_SKIP_DB_HINTS", "").strip().lower() in {"1", "true", "yes"}:
        return
    conn = _db_connect()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT token_id FROM market_tokens_allowed WHERE market_id = %s ORDER BY token_id",
                (market_id,),
            )
            allowed = [row[0] for row in cur.fetchall()]
            cur.execute(
                "SELECT token_id, qty FROM user_token_stock WHERE user_id = %s ORDER BY token_id",
                (user_id,),
            )
            stock = [(row[0], row[1]) for row in cur.fetchall()]
        print("\n--- DB hints (same vars as benchmarks: DB_HOST, DB_USER, DB_NAME, ...) ---")
        print(f"market_tokens_allowed for market_id={market_id}: {allowed}")
        print(f"user_token_stock for user_id={user_id}: {stock}")
        if allowed and token_id not in allowed:
            print(
                f"  → TEST_TOKEN_ID={token_id} is NOT allowed on this market. "
                f"Try: export TEST_TOKEN_ID={allowed[0]}"
            )
        elif not allowed:
            print("  → No rows in market_tokens_allowed for this market (seed / designate_m_token).")
    except Exception as e:
        print(f"\n(DB hint query failed: {e})")
    finally:
        conn.close()


def verify_transaction_in_db(
    *,
    transaction_id: int,
    market_id: int,
    user_id: int,
    token_id: int,
) -> bool:
    conn = _db_connect()
    if conn is None:
        print("\n(DB verification skipped: DB_* env not configured or pymysql missing)")
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT transaction_id, market_id, user_id, token_id, amt, type, side
                FROM market_transaction
                WHERE transaction_id = %s AND market_id = %s
                LIMIT 1
                """,
                (transaction_id, market_id),
            )
            row = cur.fetchone()
        if row is None:
            print(
                "\nDB verification FAILED: no market_transaction row found "
                f"for transaction_id={transaction_id}, market_id={market_id}."
            )
            return False
        print("\nDB verification PASSED: market_transaction row found:")
        print(
            {
                "transaction_id": row[0],
                "market_id": row[1],
                "user_id": row[2],
                "token_id": row[3],
                "amt": row[4],
                "type": row[5],
                "side": row[6],
            }
        )
        if int(row[2]) != int(user_id) or int(row[3]) != int(token_id):
            print(
                "DB verification WARNING: row user_id/token_id differs from request "
                f"(expected user_id={user_id}, token_id={token_id})."
            )
        return True
    except Exception as e:
        print(f"\n(DB verification query failed: {e})")
        return False
    finally:
        conn.close()


def print_response(label: str, response: requests.Response, *, verbose: bool = True) -> None:
    print(f"\n{label}")
    print(f"Status Code: {response.status_code}")
    ct = response.headers.get("content-type", "")
    text = response.text or ""
    if not verbose:
        if not text.strip():
            print("Response Body: (empty)")
        elif "json" in ct.lower() or text.strip().startswith("{"):
            try:
                print("Response Body:", json.dumps(response.json(), indent=2)[:4000])
            except json.JSONDecodeError:
                print("Response Body (raw):", text[:500])
        else:
            print("Response Body (non-JSON):", text[:500])
        return
    if not text.strip():
        print("Response Body: (empty)")
        return
    if "json" in ct.lower() or text.strip().startswith("{"):
        try:
            print("Response Body:")
            print(json.dumps(response.json(), indent=2))
        except json.JSONDecodeError:
            print("Response Body (raw):")
            print(text)
    else:
        print("Response Body (non-JSON):")
        print(text[:2000])


def main() -> None:
    if not JWT_TOKEN:
        print("Set BENCHMARK_JWT in the environment (or .env).")
        raise SystemExit(1)

    user_id = int(os.getenv("TEST_USER_ID", "18"))
    market_id = int(os.getenv("TEST_MARKET_ID", "1"))
    token_id = int(os.getenv("TEST_TOKEN_ID", "2"))

    print_db_hints(user_id, market_id, token_id)

    payload = {
        "user_id": user_id,
        "market_id": market_id,
        "action": "MARKET_TRANSACTION",
        "token_id": token_id,
        "side": int(os.getenv("TEST_SIDE", "1")),
        "qty": int(float(os.getenv("TEST_QTY", "1"))),
        "transaction_id": int(time.time() * 1000),
        "transaction_type": os.getenv("TEST_TRANSACTION_TYPE", "BUY"),
    }

    headers = {
        "Authorization": f"Bearer {JWT_TOKEN}",
        "Content-Type": "application/json",
        "X-User-Id": str(user_id),
    }

    print(f"\nPOST {TRANSACTION_URL}")
    print("Payload:", json.dumps(payload, indent=2))

    response = requests.post(
        TRANSACTION_URL,
        headers=headers,
        json=payload,
        timeout=30,
    )
    print_response("Accept response", response)

    if response.status_code != 202:
        print("\nExpected HTTP 202 Accepted with operation_id. Fix JWT, payload, or URL.")
        print_db_hints(user_id, market_id, token_id)
        raise SystemExit(1)

    try:
        body = response.json()
    except json.JSONDecodeError:
        print("Could not parse JSON body.")
        raise SystemExit(1)

    operation_id = body.get("operation_id")
    if not operation_id:
        print("No operation_id in response.")
        raise SystemExit(1)

    poll_url = f"{BASE_URL}/v2/operations/{operation_id}"
    poll_headers = {
        "Authorization": f"Bearer {JWT_TOKEN}",
        "X-Force-Leader": "false",
    }

    deadline = time.monotonic() + float(os.getenv("POLL_TIMEOUT_SEC", "120"))
    poll_interval = float(os.getenv("POLL_INTERVAL_SEC", "0.5"))

    print(f"\nPolling GET {poll_url} until terminal status...")
    poll_n = 0
    while time.monotonic() < deadline:
        pr = requests.get(poll_url, headers=poll_headers, timeout=30)
        poll_n += 1
        # Avoid flooding the terminal: full body only while pending or first/last
        verbose_poll = poll_n <= 2
        print_response("Poll", pr, verbose=verbose_poll)
        if pr.status_code != 200:
            break
        try:
            data = pr.json()
        except json.JSONDecodeError:
            break
        status = str(data.get("status", ""))
        if status in {"succeeded", "failed"}:
            print(f"\n{'=' * 60}")
            print(f"Terminal status: {status}")
            print("Full operation payload:")
            print(json.dumps(data, indent=2, default=str))
            print(f"{'=' * 60}")
            err = data.get("error_message")
            if err:
                print(f"\nerror_message: {err}")
            if status == "failed":
                print_db_hints(user_id, market_id, token_id)
                print(
                    "\nTypical fixes: set TEST_USER_ID to an engineer with access to this market; "
                    "set TEST_TOKEN_ID to a token_id listed in market_tokens_allowed for that market; "
                    "ensure user_token_stock has qty for BUY."
                )
                raise SystemExit(2)
            if os.getenv("VERIFY_DB_COMMIT", "true").strip().lower() in {"1", "true", "yes", "y"}:
                ok = verify_transaction_in_db(
                    transaction_id=int(payload["transaction_id"]),
                    market_id=market_id,
                    user_id=user_id,
                    token_id=token_id,
                )
                if not ok:
                    raise SystemExit(3)
            return
        time.sleep(poll_interval)

    print("\nStopped polling (timeout or non-200).")
    print_db_hints(user_id, market_id, token_id)
    raise SystemExit(1)


if __name__ == "__main__":
    main()
