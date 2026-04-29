#!/usr/bin/env python3
"""Throughput and latency benchmark for event-bus market betting."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from statistics import mean
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


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


def _is_transient_transport_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    transient_markers = (
        "connection reset by peer",
        "the handshake operation timed out",
        "timed out",
        "ssl",
        "eof occurred in violation of protocol",
        "temporarily unavailable",
    )
    if any(marker in msg for marker in transient_markers):
        return True
    root = exc
    seen: set[int] = set()
    while root is not None and id(root) not in seen:
        seen.add(id(root))
        if isinstance(root, (TimeoutError, socket.timeout, ConnectionResetError)):
            return True
        root = getattr(root, "__cause__", None) or getattr(root, "__context__", None)
    return False


def json_request_with_retry(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 30.0,
    max_attempts: int = 3,
    retry_backoff_seconds: float = 0.2,
) -> tuple[int, dict[str, Any]]:
    attempts = max(1, int(max_attempts))
    for attempt in range(1, attempts + 1):
        try:
            return json_request(
                method,
                url,
                payload=payload,
                headers=headers,
                timeout=timeout,
            )
        except RuntimeError as e:
            if attempt >= attempts or not _is_transient_transport_error(e):
                raise
            sleep_s = retry_backoff_seconds * (2 ** (attempt - 1))
            time.sleep(max(0.0, sleep_s))
    raise RuntimeError(f"request to {url} failed after {attempts} attempts")


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


def get_user_for_index(index: int, args: argparse.Namespace) -> int:
    if args.user_ids:
        return args.user_ids[index % len(args.user_ids)]
    if args.user_id is None:
        raise ValueError("--user-id is required unless --user-ids is provided.")
    return args.user_id


def get_market_for_index(index: int, args: argparse.Namespace) -> int:
    market_count = max(1, int(args.market_count))
    return int(args.market_start) + (index % market_count)


def select_engineer_users(args: argparse.Namespace) -> list[int]:
    try:
        import pymysql  # type: ignore
    except ImportError as e:
        raise RuntimeError(
            "auto-pick engineers requires pymysql. Install with: pip install pymysql"
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
            role_pool = [int(row[0]) for row in cur.fetchall()]

            required_token_id = (
                args.user_token_filter_id
                if args.user_token_filter_id is not None
                else args.token_id
            )
            cur.execute(
                """
                SELECT user_id
                FROM user_token_stock
                WHERE token_id = %s AND qty > 0
                """,
                (required_token_id,),
            )
            token_pool = {int(row[0]) for row in cur.fetchall()}

            pool = sorted([uid for uid in role_pool if uid in token_pool])
    finally:
        conn.close()

    if len(pool) < args.engineer_sample_size:
        raise ValueError(
            f"Not enough engineer users in org {args.org_id}: "
            f"need {args.engineer_sample_size}, found {len(pool)}"
        )
    return pool[: args.engineer_sample_size]


def submit_transaction(
    index: int,
    args: argparse.Namespace,
    headers: dict[str, str],
    transaction_id: int,
) -> AcceptedOperation:
    user_id = get_user_for_index(index, args)
    market_id = get_market_for_index(index, args)
    payload = {
        "user_id": user_id,
        "market_id": market_id,
        "action": "MARKET_TRANSACTION",
        "token_id": args.token_id,
        "side": args.side,
        "qty": args.qty,
        "transaction_id": transaction_id,
        "transaction_type": args.transaction_type,
    }
    request_headers = dict(headers)
    if args.per_request_x_user_id:
        request_headers["X-User-Id"] = str(user_id)
    started = time.perf_counter()
    status, body = json_request_with_retry(
        "POST",
        f"{args.base_url.rstrip('/')}/v2/markets/transactions",
        payload=payload,
        headers=request_headers,
        timeout=args.request_timeout,
        max_attempts=args.retry_attempts,
        retry_backoff_seconds=args.retry_backoff_seconds,
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
    while True:
        polls += 1
        status, body = json_request_with_retry(
            "GET",
            f"{args.base_url.rstrip('/')}/v2/operations/{accepted.operation_id}",
            headers=headers,
            timeout=args.request_timeout,
            max_attempts=args.retry_attempts,
            retry_backoff_seconds=args.retry_backoff_seconds,
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
            raise TimeoutError(
                f"operation {accepted.operation_id} did not finish before timeout"
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
    parser.add_argument(
        "--user-ids",
        default="",
        help='Comma-separated user IDs for round-robin request fanout, e.g. "18,19,20,21".',
    )
    parser.add_argument("--market-id", type=int, required=True)
    parser.add_argument("--market-start", type=int, default=None)
    parser.add_argument("--market-count", type=int, default=1)
    parser.add_argument("--token-id", type=int, required=True)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=10)
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
    parser.add_argument("--poll-interval", type=float, default=0.1)
    parser.add_argument("--poll-timeout", type=float, default=30.0)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=4,
        help="Max retry attempts for transient network/TLS errors.",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=0.25,
        help="Base backoff seconds for retries (exponential).",
    )
    parser.add_argument("--jwt", default=os.getenv("BENCHMARK_JWT"))
    parser.add_argument("--x-user-id", type=int, default=None)
    parser.add_argument(
        "--per-request-x-user-id",
        action="store_true",
        help="Set X-User-Id header from the selected request user_id on each request.",
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
        "--auto-pick-engineers",
        action="store_true",
        help="Automatically sample engineer users from user_org_role.",
    )
    parser.add_argument("--org-id", type=int, default=3)
    parser.add_argument("--engineer-role-id", default="engineer")
    parser.add_argument("--engineer-sample-size", type=int, default=10)
    parser.add_argument(
        "--user-token-filter-id",
        type=int,
        default=None,
        help=(
            "When auto-picking engineers, only include users with positive balance "
            "for this token_id. Defaults to --token-id."
        ),
    )
    parser.add_argument("--db-host", default=os.getenv("DB_HOST", os.getenv("LEADER_DB_HOST", "")))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "3306")))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", ""))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", ""))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", ""))
    parser.add_argument("--db-ssl-ca", default=os.getenv("DB_SSL_CA", ""))
    return parser.parse_args()


def run_once(
    args: argparse.Namespace,
    headers: dict[str, str],
    *,
    verbose: bool,
    worker_concurrency: int | None = None,
) -> tuple[int, int, float, float]:
    effective_concurrency = int(worker_concurrency or args.concurrency)
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
                    "concurrency": effective_concurrency,
                    "qty": args.qty,
                    "side": args.side,
                    "transaction_type": args.transaction_type,
                },
                indent=2,
            )
        )

    accepted_ops: list[AcceptedOperation] = []
    submit_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=effective_concurrency) as pool:
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
    with ThreadPoolExecutor(max_workers=effective_concurrency) as pool:
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


def parse_concurrency_sweep(raw: str) -> list[int]:
    if not raw.strip():
        return []
    values: list[int] = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        v = int(p)
        if v <= 0:
            raise ValueError("concurrency sweep values must be > 0")
        values.append(v)
    return values


def write_sweep_csv(path: str, rows: list[dict[str, float | int]]) -> None:
    fieldnames = [
        "users",
        "succeeded",
        "failed",
        "submit_rps",
        "completion_ops_s",
        "submit_s",
        "total_s",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    args = parse_args()
    if args.market_start is None:
        args.market_start = args.market_id
    if args.market_count <= 0:
        raise ValueError("--market-count must be > 0")
    args.user_ids = parse_int_list(args.user_ids)
    if args.auto_pick_engineers:
        args.user_ids = select_engineer_users(args)
        print(f"selected_engineer_user_ids: {args.user_ids}")
    if not args.user_ids and args.user_id is None:
        raise ValueError("Provide --user-id or --user-ids.")
    headers = build_headers(args)
    sweep_values = parse_concurrency_sweep(args.concurrency_sweep)
    if not sweep_values:
        succeeded, failed, _submit_seconds, _total_seconds = run_once(
            args, headers, verbose=True
        )
        return 1 if failed else 0

    print("Starting market betting benchmark sweep")
    print(f"concurrency_values: {sweep_values}")
    print(f"requests_per_run: {args.requests}")
    print()
    print(
        "users  succeeded  failed  submit_rps  completion_ops_s  "
        "submit_s  total_s"
    )
    print("-" * 78)
    any_failed = False
    csv_rows: list[dict[str, float | int]] = []
    for i, users in enumerate(sweep_values):
        run_args = argparse.Namespace(**vars(args))
        run_args.concurrency = users
        # keep transaction_id unique across sweep runs
        run_args.transaction_id_start = args.transaction_id_start + (i * args.requests)
        succeeded, failed, submit_seconds, total_seconds = run_once(
            run_args,
            headers,
            verbose=False,
            worker_concurrency=users,
        )
        submit_rps = (args.requests / submit_seconds) if submit_seconds > 0 else 0.0
        completion_ops_s = (
            (args.requests / total_seconds) if total_seconds > 0 else 0.0
        )
        print(
            f"{users:>5}  {succeeded:>9}  {failed:>6}  "
            f"{submit_rps:>10.2f}  {completion_ops_s:>16.2f}  "
            f"{submit_seconds:>8.3f}  {total_seconds:>7.3f}"
        )
        csv_rows.append(
            {
                "users": users,
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