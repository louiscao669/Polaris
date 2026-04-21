#!/usr/bin/env python3
"""Throughput/latency benchmark for single_server market transactions."""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from statistics import mean
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass
class RequestResult:
    index: int
    transaction_id: int
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
    payload: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    raw = None if payload is None else json.dumps(payload).encode("utf-8")
    req = Request(url, data=raw, method=method)
    req.add_header("Content-Type", "application/json")
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


def submit_transaction(
    index: int,
    args: argparse.Namespace,
    transaction_id: int,
) -> RequestResult:
    payload = {
        "user_id": args.user_id,
        "market_id": args.market_id,
        "token_id": args.token_id,
        "side": bool(args.side),
        "qty": int(args.qty),
        "transaction_id": transaction_id,
        "transaction_type": args.transaction_type,
    }
    started = time.perf_counter()
    status, body = json_request(
        "POST",
        f"{args.base_url.rstrip('/')}/markets/transactions",
        payload=payload,
        timeout=args.request_timeout,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    ok = 200 <= status < 300
    err = None
    if not ok:
        err = body.get("detail") if isinstance(body, dict) else str(body)
    return RequestResult(
        index=index,
        transaction_id=transaction_id,
        latency_ms=elapsed_ms,
        status_code=status,
        ok=ok,
        error_message=err,
    )


def print_summary(results: list[RequestResult], total_seconds: float) -> None:
    latencies = [r.latency_ms for r in results]
    succeeded = [r for r in results if r.ok]
    failed = [r for r in results if not r.ok]

    print("\nBenchmark Summary")
    print("=================")
    print(f"requests_sent: {len(results)}")
    print(f"requests_succeeded: {len(succeeded)}")
    print(f"requests_failed: {len(failed)}")
    print(f"total_seconds: {total_seconds:.3f}")
    if total_seconds > 0:
        print(f"throughput_rps: {len(results) / total_seconds:.2f}")

    if latencies:
        print("\nRequest Latency (HTTP POST -> response)")
        print("--------------------------------------")
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
                f"transaction_id={r.transaction_id} "
                f"http={r.status_code} error={r.error_message or 'unknown'}"
            )
        if len(failed) > 10:
            print(f"... and {len(failed) - 10} more")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run throughput/latency tests for single_server market transactions."
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument("--market-id", type=int, required=True)
    parser.add_argument("--token-id", type=int, required=True)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--qty", type=int, default=1)
    parser.add_argument("--side", type=int, choices=(0, 1), default=1)
    parser.add_argument("--transaction-type", choices=("BUY", "SELL"), default="BUY")
    parser.add_argument(
        "--transaction-id-start",
        type=int,
        default=int(time.time() * 1000),
    )
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument(
        "--concurrency-sweep",
        default="",
        help='Comma-separated values, e.g. "10,20,30,40,50,60,70,80,90,100".',
    )
    parser.add_argument("--out-csv", default="", help="Optional CSV output path for sweep.")
    return parser.parse_args()


def parse_concurrency_sweep(raw: str) -> list[int]:
    if not raw.strip():
        return []
    out: list[int] = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        v = int(p)
        if v <= 0:
            raise ValueError("concurrency values must be > 0")
        out.append(v)
    return out


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


def run_once(args: argparse.Namespace, *, verbose: bool) -> tuple[int, int, float]:
    if verbose:
        print("Starting single_server market benchmark")
        print(
            json.dumps(
                {
                    "base_url": args.base_url,
                    "user_id": args.user_id,
                    "market_id": args.market_id,
                    "token_id": args.token_id,
                    "requests": args.requests,
                    "concurrency": args.concurrency,
                    "qty": args.qty,
                    "side": args.side,
                    "transaction_type": args.transaction_type,
                },
                indent=2,
            )
        )

    results: list[RequestResult] = []
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [
            pool.submit(submit_transaction, idx, args, args.transaction_id_start + idx)
            for idx in range(args.requests)
        ]
        for future in as_completed(futures):
            results.append(future.result())
    total_seconds = time.perf_counter() - started

    if verbose:
        print_summary(results, total_seconds)
    succeeded = sum(1 for r in results if r.ok)
    failed = len(results) - succeeded
    return succeeded, failed, total_seconds


def main() -> int:
    args = parse_args()
    sweep_values = parse_concurrency_sweep(args.concurrency_sweep)
    if not sweep_values:
        succeeded, failed, _total_seconds = run_once(args, verbose=True)
        return 1 if failed else 0

    print("Starting single_server benchmark sweep")
    print(f"concurrency_values: {sweep_values}")
    print(f"requests_per_run: {args.requests}")
    print()
    print(
        "users  succeeded  failed  submit_rps  completion_ops_s  "
        "submit_s  total_s"
    )
    print("-" * 78)
    rows: list[dict[str, float | int]] = []
    any_failed = False
    for i, users in enumerate(sweep_values):
        run_args = argparse.Namespace(**vars(args))
        run_args.concurrency = users
        run_args.transaction_id_start = args.transaction_id_start + (i * args.requests)
        succeeded, failed, total_s = run_once(run_args, verbose=False)
        rps = (args.requests / total_s) if total_s > 0 else 0.0
        print(
            f"{users:>5}  {succeeded:>9}  {failed:>6}  "
            f"{rps:>10.2f}  {rps:>16.2f}  {total_s:>8.3f}  {total_s:>7.3f}"
        )
        rows.append(
            {
                "users": users,
                "succeeded": succeeded,
                "failed": failed,
                "submit_rps": round(rps, 4),
                "completion_ops_s": round(rps, 4),
                "submit_s": round(total_s, 4),
                "total_s": round(total_s, 4),
            }
        )
        any_failed = any_failed or (failed > 0)
    if args.out_csv.strip():
        write_sweep_csv(args.out_csv, rows)
        print(f"\nWrote CSV: {args.out_csv}")
    return 1 if any_failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        raise SystemExit(130)
