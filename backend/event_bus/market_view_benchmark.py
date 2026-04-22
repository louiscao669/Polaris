#!/usr/bin/env python3
"""Throughput and latency benchmark for event-bus market reads."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from statistics import mean
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

HARDCODED_TOTAL_USER_SWEEP = [2, 4, 8, 10] + list(range(20, 101, 10))


@dataclass
class RequestResult:
    index: int
    user_id: int
    market_id: int
    latency_ms: float
    status_code: int
    ok: bool
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
    headers: dict[str, str] | None = None,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    req = Request(url, method=method)
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


def read_market_request(
    index: int,
    args: argparse.Namespace,
    headers: dict[str, str],
) -> RequestResult:
    user_id, market_id = get_user_market_for_index(index, args)
    query = urlencode({"user_id": user_id, "cache_mode": args.cache_mode})
    url = f"{args.base_url.rstrip('/')}/markets/{market_id}?{query}"
    request_headers = dict(headers)
    if args.per_request_x_user_id:
        request_headers["X-User-Id"] = str(user_id)

    started = time.perf_counter()
    status, body = json_request(
        "GET",
        url,
        headers=request_headers,
        timeout=args.request_timeout,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    ok = status == 200
    err = None
    if not ok:
        err = body.get("detail") if isinstance(body, dict) else str(body)
    return RequestResult(
        index=index,
        user_id=user_id,
        market_id=market_id,
        latency_ms=elapsed_ms,
        status_code=status,
        ok=ok,
        error_message=err,
    )


def print_summary(results: list[RequestResult], submit_seconds: float, total_seconds: float) -> None:
    latencies = [r.latency_ms for r in results]
    succeeded = [r for r in results if r.ok]
    failed = [r for r in results if not r.ok]

    print("\nBenchmark Summary")
    print("=================")
    print(f"requests_sent: {len(results)}")
    print(f"requests_succeeded: {len(succeeded)}")
    print(f"requests_failed: {len(failed)}")
    print(f"submit_window_seconds: {submit_seconds:.3f}")
    print(f"end_to_end_window_seconds: {total_seconds:.3f}")
    if submit_seconds > 0:
        print(f"submit_throughput_rps: {len(results) / submit_seconds:.2f}")
    if total_seconds > 0:
        print(f"completion_throughput_ops_s: {len(results) / total_seconds:.2f}")

    if latencies:
        print("\nRead Latency (HTTP GET -> response)")
        print("----------------------------------")
        print(f"avg_ms: {mean(latencies):.2f}")
        print(f"p50_ms: {percentile(latencies, 0.50):.2f}")
        print(f"p95_ms: {percentile(latencies, 0.95):.2f}")
        print(f"p99_ms: {percentile(latencies, 0.99):.2f}")
        print(f"max_ms: {max(latencies):.2f}")

    if failed:
        print("\nFailed Requests")
        print("---------------")
        for r in failed[:10]:
            print(
                f"user_id={r.user_id} market_id={r.market_id} "
                f"http={r.status_code} error={r.error_message or 'unknown'}"
            )
        if len(failed) > 10:
            print(f"... and {len(failed) - 10} more")


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run throughput + latency tests for market reads."
    )
    parser.add_argument("--base-url", default="polaris-balancer-1818197353.us-east-2.elb.amazonaws.com")
    parser.add_argument("--user-id", type=int, default=None)
    parser.add_argument("--market-id", type=int, default=None)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=30)
    parser.add_argument("--market-count", type=int, default=1)
    parser.add_argument("--cache-mode", default="default")
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--jwt", default=os.getenv("BENCHMARK_JWT"))
    parser.add_argument("--x-user-id", type=int, default=None)
    parser.add_argument(
        "--per-request-x-user-id",
        action="store_true",
        help="Set X-User-Id header from the request user_id for each request.",
    )
    parser.add_argument("--engineer-user-ids", default="")
    parser.add_argument("--marketing-user-ids", default="")
    parser.add_argument(
        "--auto-pick-role-users",
        action="store_true",
        help="Randomly sample engineer/marketing users from user_org_role.",
    )
    parser.add_argument("--org-id", type=int, default=3)
    parser.add_argument("--engineer-role-id", default="engineer")
    parser.add_argument("--marketing-role-id", default="marketing")
    parser.add_argument("--engineer-sample-size", type=int, default=10)
    parser.add_argument("--marketing-sample-size", type=int, default=10)
    parser.add_argument("--role-pick-seed", type=int, default=42)
    parser.add_argument("--db-host", default=os.getenv("DB_HOST", os.getenv("LEADER_DB_HOST", "")))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "3306")))
    parser.add_argument("--db-user", default=os.getenv("DB_USER", ""))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD", ""))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", ""))
    parser.add_argument("--db-ssl-ca", default=os.getenv("DB_SSL_CA", ""))
    parser.add_argument("--engineer-market-start", type=int, default=1)
    parser.add_argument("--engineer-market-count", type=int, default=5)
    parser.add_argument("--marketing-market-start", type=int, default=6)
    parser.add_argument("--marketing-market-count", type=int, default=5)
    parser.add_argument("--concurrency-sweep", default="")
    parser.add_argument("--market-count-sweep", default="")
    parser.add_argument(
        "--hardcoded-user-sweep",
        action="store_true",
        help="Run fixed total-user sweep: 2,4,8,10,20,30,...,100 (50/50 split).",
    )
    parser.add_argument("--out-csv", default="")
    return parser.parse_args()


def run_once(args: argparse.Namespace, headers: dict[str, str], *, verbose: bool) -> tuple[int, int, float, float]:
    if verbose:
        print("Starting market view benchmark")
        print(
            json.dumps(
                {
                    "base_url": args.base_url,
                    "user_id": args.user_id,
                    "market_id": args.market_id,
                    "requests": args.requests,
                    "concurrency": args.concurrency,
                    "market_count": args.market_count,
                    "cache_mode": args.cache_mode,
                },
                indent=2,
            )
        )

    results: list[RequestResult] = []
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(read_market_request, idx, args, headers) for idx in range(args.requests)]
        for future in as_completed(futures):
            results.append(future.result())
    total_seconds = time.perf_counter() - started
    submit_seconds = total_seconds

    if verbose:
        print_summary(results, submit_seconds, total_seconds)
    succeeded = sum(1 for r in results if r.ok)
    failed = len(results) - succeeded
    return succeeded, failed, submit_seconds, total_seconds


def main() -> int:
    args = parse_args()
    args.engineer_user_ids = parse_int_list(args.engineer_user_ids)
    args.marketing_user_ids = parse_int_list(args.marketing_user_ids)

    if args.auto_pick_role_users and (not args.db_host or not args.db_user or not args.db_name):
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
    if not using_multi_user_mode and (args.user_id is None or args.market_id is None):
        raise ValueError(
            "--user-id and --market-id are required unless multi-user mode is enabled."
        )

    headers = build_headers(args)
    concurrency_sweep = parse_positive_int_sweep(args.concurrency_sweep, label="concurrency")
    market_count_sweep = parse_positive_int_sweep(args.market_count_sweep, label="market-count")
    user_count_sweep = HARDCODED_TOTAL_USER_SWEEP if args.hardcoded_user_sweep else []
    active_sweeps = sum(1 for s in (concurrency_sweep, market_count_sweep, user_count_sweep) if s)
    if active_sweeps > 1:
        raise ValueError(
            "Use only one of --concurrency-sweep, --market-count-sweep, or --hardcoded-user-sweep."
        )
    if user_count_sweep and not args.auto_pick_role_users:
        raise ValueError("--hardcoded-user-sweep requires --auto-pick-role-users.")

    if not concurrency_sweep and not market_count_sweep and not user_count_sweep:
        if args.auto_pick_role_users:
            args.engineer_user_ids, args.marketing_user_ids = select_role_users(args)
            print(f"selected_engineer_user_ids: {args.engineer_user_ids}")
            print(f"selected_marketing_user_ids: {args.marketing_user_ids}")
        succeeded, failed, _submit_seconds, _total_seconds = run_once(args, headers, verbose=True)
        return 1 if failed else 0

    if user_count_sweep:
        sweep_values = user_count_sweep
        sweep_label = "users"
    else:
        sweep_values = concurrency_sweep or market_count_sweep
        sweep_label = "users" if concurrency_sweep else "markets"

    print("Starting market view benchmark sweep")
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
            run_args.engineer_sample_size = sweep_value // 2
            run_args.marketing_sample_size = sweep_value // 2
            run_args.role_pick_seed = args.role_pick_seed + i
            run_args.engineer_user_ids, run_args.marketing_user_ids = select_role_users(run_args)
            print(
                f"selected({sweep_value}) engineers={run_args.engineer_user_ids} "
                f"marketing={run_args.marketing_user_ids}"
            )
        elif concurrency_sweep:
            run_args.concurrency = sweep_value
        else:
            run_args.market_count = sweep_value

        succeeded, failed, submit_seconds, total_seconds = run_once(
            run_args, headers, verbose=False
        )
        submit_rps = (args.requests / submit_seconds) if submit_seconds > 0 else 0.0
        completion_ops_s = (args.requests / total_seconds) if total_seconds > 0 else 0.0
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
