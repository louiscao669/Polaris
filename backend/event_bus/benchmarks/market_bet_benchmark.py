#!/usr/bin/env python3
"""Throughput and latency benchmark for event-bus market betting."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from statistics import mean
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

HARDCODED_TOTAL_USER_SWEEP = [2,4,8,20,40,60,80,100]


@dataclass
class AcceptedOperation:
    index: int
    operation_id: str
    transaction_id: int
    accepted_latency_ms: float
    request_started_at: float


@dataclass
class CompletedOperation:
    accepted: AcceptedOperation
    final_status: str
    completion_latency_ms: float
    polls: int
    error_message: str | None = None


@dataclass(frozen=True)
class StableAssignment:
    user_id: int
    market_id: int
    token_id: int


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return ordered[lo]
    frac = rank - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def json_request(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    raw = None if payload is None else json.dumps(payload).encode("utf-8")
    req = Request(url, data=raw, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)

    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body) if body else {}
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body) if body else {}
        except json.JSONDecodeError:
            parsed = {"detail": body}
        return e.code, parsed
    except URLError as e:
        raise RuntimeError(f"request to {url} failed: {e}") from e


def build_headers(args: argparse.Namespace) -> dict[str, str]:
    headers: dict[str, str] = {}
    if args.jwt:
        headers["Authorization"] = f"Bearer {args.jwt}"
    if args.x_user_id is not None:
        headers["X-User-Id"] = str(args.x_user_id)
    return headers


def parse_int_list(raw: str) -> list[int]:
    out: list[int] = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        out.append(int(p))
    return out


def get_user_market_for_index(index: int, args: argparse.Namespace) -> tuple[int, int]:
    stable_slots: list[StableAssignment] = getattr(args, "stable_slots", [])
    if stable_slots:
        s = stable_slots[index % len(stable_slots)]
        return s.user_id, s.market_id

    if not args.engineer_user_ids and not args.marketing_user_ids:
        market_count = max(1, int(getattr(args, "market_count", 1)))
        market_id = args.market_id + (index % market_count)
        return args.user_id, market_id

    engineers = args.engineer_user_ids
    marketings = args.marketing_user_ids
    total_users = len(engineers) + len(marketings)
    if total_users <= 0:
        raise ValueError("multi-user mode requires at least one user id")

    slot = index % total_users
    if slot < len(engineers):
        user_id = engineers[slot]
        market_id = args.engineer_market_start + (slot % args.engineer_market_count)
        return user_id, market_id

    m_idx = slot - len(engineers)
    user_id = marketings[m_idx]
    market_id = args.marketing_market_start + (m_idx % args.marketing_market_count)
    return user_id, market_id


def is_engineer_slot(index: int, args: argparse.Namespace) -> bool:
    engineers = args.engineer_user_ids
    marketings = args.marketing_user_ids
    if not engineers and not marketings:
        return True
    total_users = len(engineers) + len(marketings)
    if total_users <= 0:
        raise ValueError("multi-user mode requires at least one user id")
    return (index % total_users) < len(engineers)


def get_token_id_for_index(index: int, args: argparse.Namespace) -> int:
    stable_slots: list[StableAssignment] = getattr(args, "stable_slots", [])
    if stable_slots:
        return stable_slots[index % len(stable_slots)].token_id

    using_multi_user_mode = bool(args.engineer_user_ids or args.marketing_user_ids)
    if not using_multi_user_mode:
        if args.token_id is None:
            raise ValueError("--token-id is required in single-user mode.")
        return args.token_id

    if is_engineer_slot(index, args):
        if args.engineer_token_id is not None:
            return args.engineer_token_id
    else:
        if args.marketing_token_id is not None:
            return args.marketing_token_id

    if args.token_id is None:
        raise ValueError(
            "In multi-user mode, provide --engineer-token-id and --marketing-token-id "
            "(or fallback --token-id)."
        )
    return args.token_id


def select_role_users(args: argparse.Namespace) -> tuple[list[int], list[int]]:
    if not args.auto_pick_role_users:
        return args.engineer_user_ids, args.marketing_user_ids

    try:
        import pymysql  # type: ignore
    except ImportError as e:
        raise RuntimeError(
            "auto-pick requires pymysql. Install with: pip install pymysql"
        ) from e

    conn = pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
        ssl={"ca": args.db_ssl_ca} if args.db_ssl_ca else None,
        cursorclass=pymysql.cursors.Cursor,
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id
                FROM user_org_role
                WHERE org_id = %s AND role_id = %s
                ORDER BY user_id
                """,
                (args.org_id, args.engineer_role_id),
            )
            engineer_pool = [int(row[0]) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT user_id
                FROM user_org_role
                WHERE org_id = %s AND role_id = %s
                ORDER BY user_id
                """,
                (args.org_id, args.marketing_role_id),
            )
            marketing_pool = [int(row[0]) for row in cur.fetchall()]
    finally:
        conn.close()

    if len(engineer_pool) < args.engineer_sample_size:
        raise ValueError(
            f"Not enough engineer users in org {args.org_id}: "
            f"need {args.engineer_sample_size}, found {len(engineer_pool)}"
        )
    if len(marketing_pool) < args.marketing_sample_size:
        raise ValueError(
            f"Not enough marketing users in org {args.org_id}: "
            f"need {args.marketing_sample_size}, found {len(marketing_pool)}"
        )

    rng = random.Random(args.role_pick_seed)
    engineer_ids = rng.sample(engineer_pool, args.engineer_sample_size)
    marketing_ids = rng.sample(marketing_pool, args.marketing_sample_size)
    engineer_ids.sort()
    marketing_ids.sort()
    return engineer_ids, marketing_ids


def _build_stable_assignments(args: argparse.Namespace) -> list[StableAssignment]:
    """Pre-validate user/market/token triplets so benchmark issues valid BUY requests.

    Checks:
    - user has role access to the market (market_open_to_as + user_org_role)
    - market allows the token (market_tokens_allowed)
    - for BUY transactions: user has positive token stock (user_token_stock.qty > 0)
    """
    try:
        import pymysql  # type: ignore
    except ImportError as e:
        raise RuntimeError("stable assignment mode requires pymysql. Install with: pip install pymysql") from e

    assignments: list[StableAssignment] = []
    market_access: set[tuple[int, int]] = set()
    token_allowed: set[tuple[int, int]] = set()
    stock_positive: set[tuple[int, int]] = set()

    conn = pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
        ssl={"ca": args.db_ssl_ca} if args.db_ssl_ca else None,
        cursorclass=pymysql.cursors.Cursor,
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT uor.user_id, mota.market_id
                FROM user_org_role uor
                JOIN market_open_to_as mota
                  ON mota.org_id = uor.org_id AND mota.role_id = uor.role_id
                WHERE uor.org_id = %s
                """,
                (args.org_id,),
            )
            market_access = {(int(r[0]), int(r[1])) for r in cur.fetchall()}

            cur.execute(
                """
                SELECT market_id, token_id
                FROM market_tokens_allowed
                """
            )
            token_allowed = {(int(r[0]), int(r[1])) for r in cur.fetchall()}

            if str(args.transaction_type).upper() == "BUY":
                cur.execute(
                    """
                    SELECT user_id, token_id
                    FROM user_token_stock
                    WHERE qty > 0
                    """
                )
                stock_positive = {(int(r[0]), int(r[1])) for r in cur.fetchall()}

    finally:
        conn.close()

    def _markets(start: int, count: int) -> list[int]:
        return [start + i for i in range(max(0, count))]

    engineer_token = (
        args.engineer_token_id if args.engineer_token_id is not None else args.token_id
    )
    marketing_token = (
        args.marketing_token_id if args.marketing_token_id is not None else args.token_id
    )

    if engineer_token is None or marketing_token is None:
        raise ValueError(
            "Stable assignment mode requires engineer/marketing token ids "
            "(or fallback --token-id)."
        )

    for uid in args.engineer_user_ids:
        for mid in _markets(args.engineer_market_start, args.engineer_market_count):
            if (uid, mid) not in market_access:
                continue
            if (mid, int(engineer_token)) not in token_allowed:
                continue
            if stock_positive and (uid, int(engineer_token)) not in stock_positive:
                continue
            assignments.append(
                StableAssignment(user_id=int(uid), market_id=int(mid), token_id=int(engineer_token))
            )

    for uid in args.marketing_user_ids:
        for mid in _markets(args.marketing_market_start, args.marketing_market_count):
            if (uid, mid) not in market_access:
                continue
            if (mid, int(marketing_token)) not in token_allowed:
                continue
            if stock_positive and (uid, int(marketing_token)) not in stock_positive:
                continue
            assignments.append(
                StableAssignment(user_id=int(uid), market_id=int(mid), token_id=int(marketing_token))
            )

    if not assignments:
        raise ValueError(
            "No stable user/market/token assignments found. "
            "Check market_open_to_as, market_tokens_allowed, and user_token_stock."
        )
    return assignments


def submit_transaction(
    index: int,
    args: argparse.Namespace,
    headers: dict[str, str],
    transaction_id: int,
) -> AcceptedOperation:
    user_id, market_id = get_user_market_for_index(index, args)
    token_id = get_token_id_for_index(index, args)
    payload = {
        "user_id": user_id,
        "market_id": market_id,
        "action": "MARKET_TRANSACTION",
        "token_id": token_id,
        "side": args.side,
        "qty": args.qty,
        "transaction_id": transaction_id,
        "transaction_type": args.transaction_type,
    }
    request_headers = dict(headers)
    if args.per_request_x_user_id:
        request_headers["X-User-Id"] = str(user_id)
    started = time.perf_counter()
    status, body = json_request(
        "POST",
        f"{args.base_url.rstrip('/')}/v2/markets/transactions",
        payload=payload,
        headers=request_headers,
        timeout=args.request_timeout,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if status != 202:
        raise RuntimeError(
            f"request {index} rejected with HTTP {status}: {json.dumps(body)}"
        )
    operation_id = body.get("operation_id")
    if not operation_id:
        raise RuntimeError(f"request {index} missing operation_id: {json.dumps(body)}")
    return AcceptedOperation(
        index=index,
        operation_id=str(operation_id),
        transaction_id=transaction_id,
        accepted_latency_ms=elapsed_ms,
        request_started_at=started,
    )


def poll_operation(
    accepted: AcceptedOperation,
    args: argparse.Namespace,
    headers: dict[str, str],
) -> CompletedOperation:
    deadline = time.perf_counter() + args.poll_timeout
    polls = 0
    poll_headers = dict(headers)
    if args.poll_from_replica:
        # Replica polling can be useful for consistency tests, but may lag.
        poll_headers["X-Force-Leader"] = "false"
    while True:
        polls += 1
        status, body = json_request(
            "GET",
            f"{args.base_url.rstrip('/')}/v2/operations/{accepted.operation_id}",
            headers=poll_headers,
            timeout=args.request_timeout,
        )
        if status != 200:
            raise RuntimeError(
                f"poll for {accepted.operation_id} returned HTTP {status}: {json.dumps(body)}"
            )

        op_status = str(body.get("status", "unknown"))
        if op_status in {"succeeded", "failed"}:
            return CompletedOperation(
                accepted=accepted,
                final_status=op_status,
                completion_latency_ms=(
                    time.perf_counter() - accepted.request_started_at
                )
                * 1000.0,
                polls=polls,
                error_message=body.get("error_message"),
            )

        if time.perf_counter() >= deadline:
            return CompletedOperation(
                accepted=accepted,
                final_status="failed",
                completion_latency_ms=(
                    time.perf_counter() - accepted.request_started_at
                )
                * 1000.0,
                polls=polls,
                error_message=(
                    f"operation {accepted.operation_id} did not finish before "
                    f"timeout ({args.poll_timeout}s)"
                ),
            )
        time.sleep(args.poll_interval)


def print_summary(
    accepted_ops: list[AcceptedOperation],
    completed_ops: list[CompletedOperation],
    submit_seconds: float,
    total_seconds: float,
) -> None:
    accepted_latencies = [op.accepted_latency_ms for op in accepted_ops]
    completion_latencies = [op.completion_latency_ms for op in completed_ops]
    succeeded = [op for op in completed_ops if op.final_status == "succeeded"]
    failed = [op for op in completed_ops if op.final_status == "failed"]

    print("\nBenchmark Summary")
    print("=================")
    print(f"requests_sent: {len(accepted_ops)}")
    print(f"operations_completed: {len(completed_ops)}")
    print(f"operations_succeeded: {len(succeeded)}")
    print(f"operations_failed: {len(failed)}")
    print(f"submit_window_seconds: {submit_seconds:.3f}")
    print(f"end_to_end_window_seconds: {total_seconds:.3f}")
    if submit_seconds > 0:
        print(f"submit_throughput_rps: {len(accepted_ops) / submit_seconds:.2f}")
    if total_seconds > 0:
        print(f"completion_throughput_ops_s: {len(completed_ops) / total_seconds:.2f}")

    if accepted_latencies:
        print("\nAccepted Latency (HTTP POST -> 202)")
        print("----------------------------------")
        print(f"avg_ms: {mean(accepted_latencies):.2f}")
        print(f"p50_ms: {percentile(accepted_latencies, 0.50):.2f}")
        print(f"p95_ms: {percentile(accepted_latencies, 0.95):.2f}")
        print(f"p99_ms: {percentile(accepted_latencies, 0.99):.2f}")
        print(f"max_ms: {max(accepted_latencies):.2f}")

    if completion_latencies:
        print("\nCompletion Latency (HTTP POST -> final operation status)")
        print("--------------------------------------------------------")
        print(f"avg_ms: {mean(completion_latencies):.2f}")
        print(f"p50_ms: {percentile(completion_latencies, 0.50):.2f}")
        print(f"p95_ms: {percentile(completion_latencies, 0.95):.2f}")
        print(f"p99_ms: {percentile(completion_latencies, 0.99):.2f}")
        print(f"max_ms: {max(completion_latencies):.2f}")

    if failed:
        print("\nFailed Operations")
        print("-----------------")
        for op in failed[:10]:
            print(
                f"transaction_id={op.accepted.transaction_id} "
                f"operation_id={op.accepted.operation_id} "
                f"error={op.error_message or 'unknown'}"
            )
        if len(failed) > 10:
            print(f"... and {len(failed) - 10} more")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run throughput + latency tests for market betting via the event bus."
    )
    parser.add_argument("--base-url", default="polaris-balancer-1818197353.us-east-2.elb.amazonaws.com")
    parser.add_argument("--user-id", type=int, default=None)
    parser.add_argument("--market-id", type=int, default=None)
    parser.add_argument("--token-id", type=int, default=None)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=30)
    parser.add_argument(
        "--market-count",
        type=int,
        default=1,
        help="How many consecutive market ids to round-robin across, starting at --market-id.",
    )
    parser.add_argument("--qty", type=int, default=1)
    parser.add_argument("--side", type=int, choices=(0, 1), default=1)
    parser.add_argument(
        "--transaction-type",
        choices=("BUY", "SELL"),
        default="BUY",
    )
    parser.add_argument(
        "--transaction-id-start",
        type=int,
        default=int(time.time() * 1000),
    )
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--poll-timeout", type=float, default=30.0)
    parser.add_argument(
        "--poll-from-replica",
        action="store_true",
        help=(
            "Poll operation status from read replicas (X-Force-Leader:false). "
            "Default polls leader/writer for read-your-writes behavior."
        ),
    )
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--jwt", default=os.getenv("BENCHMARK_JWT"))
    parser.add_argument("--x-user-id", type=int, default=None)
    parser.add_argument(
        "--per-request-x-user-id",
        action="store_true",
        help="Set X-User-Id header from the request user_id for each request.",
    )
    parser.add_argument(
        "--engineer-user-ids",
        default="",
        help='Comma-separated engineer user IDs, e.g. "4,5,6,7,8,9,10,11,12,13".',
    )
    parser.add_argument(
        "--marketing-user-ids",
        default="",
        help='Comma-separated marketing user IDs, e.g. "14,15,16,17,18,19,20,21,22,23".',
    )
    parser.add_argument(
        "--auto-pick-role-users",
        action="store_true",
        help="Randomly sample engineer/marketing users from user_org_role instead of passing explicit user-id lists.",
    )
    parser.add_argument("--org-id", type=int, default=3)
    parser.add_argument("--engineer-role-id", default="engineer")
    parser.add_argument("--marketing-role-id", default="marketing")
    parser.add_argument("--engineer-sample-size", type=int, default=10)
    parser.add_argument("--marketing-sample-size", type=int, default=10)
    parser.add_argument("--role-pick-seed", type=int, default=42)
    parser.add_argument(
        "--db-host",
        default=os.getenv("DB_HOST", os.getenv("LEADER_DB_HOST", "")),
    )
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "3306")))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", ""))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", ""))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", ""))
    parser.add_argument("--db-ssl-ca", default=os.getenv("DB_SSL_CA", ""))
    parser.add_argument("--engineer-market-start", type=int, default=1)
    parser.add_argument("--engineer-market-count", type=int, default=5)
    parser.add_argument("--marketing-market-start", type=int, default=6)
    parser.add_argument("--marketing-market-count", type=int, default=5)
    parser.add_argument(
        "--engineer-token-id",
        type=int,
        default=None,
        help="Token id for engineer requests in multi-user mode.",
    )
    parser.add_argument(
        "--marketing-token-id",
        type=int,
        default=None,
        help="Token id for marketing requests in multi-user mode.",
    )
    parser.add_argument(
        "--concurrency-sweep",
        default="",
        help=(
            "Comma-separated concurrency values to run back-to-back, "
            'e.g. "10,20,30,40,50,60,70,80,90,100".'
        ),
    )
    parser.add_argument(
        "--out-csv",
        default="",
        help="Optional output CSV path for sweep results.",
    )
    parser.add_argument(
        "--market-count-sweep",
        default="",
        help=(
            "Comma-separated market-count values to run back-to-back, "
            'e.g. "1,10,20,30,40,50,60,70,80,90,100".'
        ),
    )
    parser.add_argument(
        "--hardcoded-user-sweep",
        action="store_true",
        help=(
            "Run fixed total-user sweep: 2,4,8,10,20,30,...,100 "
            "(split evenly engineer/marketing)."
        ),
    )
    parser.add_argument(
        "--users-equals-concurrency",
        action="store_true",
        help=(
            "When used with --hardcoded-user-sweep, set concurrency equal "
            "to the swept total-user value for each run."
        ),
    )
    parser.add_argument(
        "--stable-auto-pick",
        action="store_true",
        help=(
            "When using multi-user mode, precompute only valid user/market/token "
            "triplets from DB and issue requests from that stable pool."
        ),
    )
    return parser.parse_args()


def run_once(args: argparse.Namespace, headers: dict[str, str], *, verbose: bool) -> tuple[int, int, float, float]:
    if verbose:
        print("Starting market betting benchmark")
        print(
            json.dumps(
                {
                    "base_url": args.base_url,
                    "user_id": args.user_id,
                    "market_id": args.market_id,
                    "token_id": args.token_id,
                    "requests": args.requests,
                    "concurrency": args.concurrency,
                    "market_count": args.market_count,
                    "qty": args.qty,
                    "side": args.side,
                    "transaction_type": args.transaction_type,
                },
                indent=2,
            )
        )

    accepted_ops: list[AcceptedOperation] = []
    submit_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        submit_futures = [
            pool.submit(
                submit_transaction,
                idx,
                args,
                headers,
                args.transaction_id_start + idx,
            )
            for idx in range(args.requests)
        ]
        for future in as_completed(submit_futures):
            accepted_ops.append(future.result())
    submit_seconds = time.perf_counter() - submit_started

    completed_ops: list[CompletedOperation] = []
    poll_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        poll_futures = [
            pool.submit(poll_operation, accepted, args, headers)
            for accepted in accepted_ops
        ]
        for future in as_completed(poll_futures):
            completed_ops.append(future.result())
    total_seconds = time.perf_counter() - submit_started

    if verbose:
        print_summary(accepted_ops, completed_ops, submit_seconds, total_seconds)
        print(f"\npoll_phase_seconds: {time.perf_counter() - poll_started:.3f}")

    succeeded = sum(1 for op in completed_ops if op.final_status == "succeeded")
    failed = len(completed_ops) - succeeded
    return succeeded, failed, submit_seconds, total_seconds


def parse_positive_int_sweep(raw: str, *, label: str) -> list[int]:
    if not raw.strip():
        return []
    values: list[int] = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        v = int(p)
        if v <= 0:
            raise ValueError(f"{label} sweep values must be > 0")
        values.append(v)
    return values


def write_sweep_csv(path: str, rows: list[dict[str, float | int]]) -> None:
    fieldnames = list(rows[0].keys()) if rows else []
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    args = parse_args()
    args.engineer_user_ids = parse_int_list(args.engineer_user_ids)
    args.marketing_user_ids = parse_int_list(args.marketing_user_ids)
    args.stable_slots = []
    if args.auto_pick_role_users:
        if not args.db_host or not args.db_user or not args.db_name:
            raise ValueError(
                "auto-pick mode requires db connection values (--db-host, --db-user, --db-name)."
            )

    if args.engineer_market_count <= 0 or args.marketing_market_count <= 0:
        raise ValueError("engineer/marketing market counts must be > 0")

    using_multi_user_mode = bool(
        args.engineer_user_ids or args.marketing_user_ids or args.auto_pick_role_users
    )
    if (
        using_multi_user_mode
        and not args.auto_pick_role_users
        and not (args.engineer_user_ids and args.marketing_user_ids)
    ):
        raise ValueError(
            "Provide both --engineer-user-ids and --marketing-user-ids for multi-user mode."
        )
    if not using_multi_user_mode:
        if args.user_id is None or args.market_id is None:
            raise ValueError(
                "--user-id and --market-id are required unless multi-user mode is enabled."
            )
        if args.token_id is None:
            raise ValueError(
                "--token-id is required unless multi-user mode uses role-specific token ids."
            )

    headers = build_headers(args)
    concurrency_sweep = parse_positive_int_sweep(
        args.concurrency_sweep, label="concurrency"
    )
    market_count_sweep = parse_positive_int_sweep(
        args.market_count_sweep, label="market-count"
    )
    user_count_sweep = HARDCODED_TOTAL_USER_SWEEP if args.hardcoded_user_sweep else []

    active_sweeps = sum(
        1 for s in (concurrency_sweep, market_count_sweep, user_count_sweep) if s
    )
    if active_sweeps > 1:
        raise ValueError(
            "Use only one of --concurrency-sweep, --market-count-sweep, "
            "or --hardcoded-user-sweep."
        )
    if user_count_sweep and not args.auto_pick_role_users:
        raise ValueError("--hardcoded-user-sweep requires --auto-pick-role-users.")

    if not concurrency_sweep and not market_count_sweep and not user_count_sweep:
        if args.auto_pick_role_users:
            args.engineer_user_ids, args.marketing_user_ids = select_role_users(args)
            print(f"selected_engineer_user_ids: {args.engineer_user_ids}")
            print(f"selected_marketing_user_ids: {args.marketing_user_ids}")
            if args.stable_auto_pick:
                args.stable_slots = _build_stable_assignments(args)
                print(f"stable_assignments: {len(args.stable_slots)}")
        succeeded, failed, _submit_seconds, _total_seconds = run_once(
            args, headers, verbose=True
        )
        return 1 if failed else 0

    if (concurrency_sweep or market_count_sweep) and args.auto_pick_role_users:
        args.engineer_user_ids, args.marketing_user_ids = select_role_users(args)
        print(f"selected_engineer_user_ids: {args.engineer_user_ids}")
        print(f"selected_marketing_user_ids: {args.marketing_user_ids}")
        if args.stable_auto_pick:
            args.stable_slots = _build_stable_assignments(args)
            print(f"stable_assignments: {len(args.stable_slots)}")

    if user_count_sweep:
        sweep_values = user_count_sweep
        sweep_label = "users"
    else:
        sweep_values = concurrency_sweep or market_count_sweep
        sweep_label = "users" if concurrency_sweep else "markets"

    print("Starting market betting benchmark sweep")
    print(f"{sweep_label}_values: {sweep_values}")
    print(f"requests_per_run: {args.requests}")
    print()
    print(
        f"{sweep_label:<7}  succeeded  failed  submit_rps  completion_ops_s  "
        "submit_s  total_s"
    )
    print("-" * 78)
    any_failed = False
    csv_rows: list[dict[str, float | int]] = []
    for i, sweep_value in enumerate(sweep_values):
        run_args = argparse.Namespace(**vars(args))
        if user_count_sweep:
            if sweep_value % 2 != 0:
                raise ValueError("hardcoded user sweep values must be even")
            run_args.engineer_sample_size = sweep_value // 2
            run_args.marketing_sample_size = sweep_value // 2
            run_args.role_pick_seed = args.role_pick_seed + i
            run_args.engineer_user_ids, run_args.marketing_user_ids = select_role_users(
                run_args
            )
            run_args.stable_slots = []
            if args.stable_auto_pick:
                run_args.stable_slots = _build_stable_assignments(run_args)
            if args.users_equals_concurrency:
                run_args.concurrency = sweep_value
            print(
                f"selected({sweep_value}) engineers={run_args.engineer_user_ids} "
                f"marketing={run_args.marketing_user_ids} "
                f"concurrency={run_args.concurrency} "
                f"stable_assignments={len(getattr(run_args, 'stable_slots', []))}"
            )
        elif concurrency_sweep:
            run_args.concurrency = sweep_value
        else:
            run_args.market_count = sweep_value
        print(
            f"running_step={i + 1}/{len(sweep_values)} "
            f"sweep_value={sweep_value} "
            f"concurrency={run_args.concurrency} "
            f"requests={run_args.requests}",
            flush=True,
        )
        # keep transaction_id unique across sweep runs
        run_args.transaction_id_start = args.transaction_id_start + (i * args.requests)
        succeeded, failed, submit_seconds, total_seconds = run_once(
            run_args, headers, verbose=False
        )
        submit_rps = (args.requests / submit_seconds) if submit_seconds > 0 else 0.0
        completion_ops_s = (
            (args.requests / total_seconds) if total_seconds > 0 else 0.0
        )
        print(
            f"{sweep_value:>7}  {succeeded:>9}  {failed:>6}  "
            f"{submit_rps:>10.2f}  {completion_ops_s:>16.2f}  "
            f"{submit_seconds:>8.3f}  {total_seconds:>7.3f}"
        )
        csv_rows.append(
            {
                sweep_label: sweep_value,
                "succeeded": succeeded,
                "failed": failed,
                "submit_rps": round(submit_rps, 4),
                "completion_ops_s": round(completion_ops_s, 4),
                "submit_s": round(submit_seconds, 4),
                "total_s": round(total_seconds, 4),
            }
        )
        any_failed = any_failed or (failed > 0)
    if args.out_csv.strip():
        write_sweep_csv(args.out_csv, csv_rows)
        print(f"\nWrote CSV: {args.out_csv}")
    return 1 if any_failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        raise SystemExit(130)
