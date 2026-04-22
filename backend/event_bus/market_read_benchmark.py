#!/usr/bin/env python3
"""Throughput/latency benchmark for read-heavy market endpoints."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from statistics import mean
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass
class ReadResult:
    index: int
    market_id: int
    latency_ms: float
    status_code: int
    ok: bool
    endpoint: str
    error_message: str | None = None


@dataclass
class ScenarioSpec:
    name: str
    warm_cache: bool
    cache_mode: str


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
) -> tuple[int, dict[str, Any] | list[Any]]:
    req = Request(url, method=method)
    req.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        req.add_header(key, value)

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


def parse_positive_int_sweep(raw: str, *, label: str) -> list[int]:
    if not raw.strip():
        return []
    values: list[int] = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        value = int(p)
        if value <= 0:
            raise ValueError(f"{label} sweep values must be > 0")
        values.append(value)
    return values


def build_endpoint(args: argparse.Namespace, market_id: int) -> str:
    endpoint = args.endpoint_template.format(
        market_id=market_id,
        event_id=args.event_id,
        organization_id=args.organization_id,
    )
    query: dict[str, Any] = {"user_id": args.user_id}
    if "{market_id}" not in args.endpoint_template:
        query["market_id"] = market_id
    if "{event_id}" not in args.endpoint_template and args.event_id is not None:
        query["event_id"] = args.event_id
    if (
        "{organization_id}" not in args.endpoint_template
        and args.organization_id is not None
    ):
        query["organization_id"] = args.organization_id
    if args.hours is not None:
        query["hours"] = args.hours
    if args.span is not None:
        query["span"] = args.span
    if args.cache_mode != "default":
        query["cache_mode"] = args.cache_mode
    return f"{args.base_url.rstrip('/')}{endpoint}?{urlencode(query)}"


def submit_read(
    index: int,
    args: argparse.Namespace,
    headers: dict[str, str],
) -> ReadResult:
    market_count = max(1, int(getattr(args, "market_count", 1)))
    market_id = args.market_id + (index % market_count)
    url = build_endpoint(args, market_id)
    started = time.perf_counter()
    status, body = json_request(
        "GET",
        url,
        headers=headers,
        timeout=args.request_timeout,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    ok = 200 <= status < 300
    error_message = None
    if not ok:
        if isinstance(body, dict):
            error_message = str(body.get("detail") or body)
        else:
            error_message = str(body)
    return ReadResult(
        index=index,
        market_id=market_id,
        latency_ms=elapsed_ms,
        status_code=status,
        ok=ok,
        endpoint=url,
        error_message=error_message,
    )


def warm_cache(args: argparse.Namespace, headers: dict[str, str]) -> None:
    market_count = max(1, int(getattr(args, "market_count", 1)))
    print(f"Warming cache with {market_count} market(s)...")
    for offset in range(market_count):
        result = submit_read(offset, args, headers)
        if not result.ok:
            raise RuntimeError(
                f"cache warmup failed for market_id={result.market_id} "
                f"with HTTP {result.status_code}: {result.error_message or 'unknown'}"
            )


def print_summary(results: list[ReadResult], total_seconds: float) -> None:
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
        print("\nRead Latency (HTTP GET -> response)")
        print("----------------------------------")
        print(f"avg_ms: {mean(latencies):.2f}")
        print(f"p50_ms: {percentile(latencies, 0.50):.2f}")
        print(f"p95_ms: {percentile(latencies, 0.95):.2f}")
        print(f"p99_ms: {percentile(latencies, 0.99):.2f}")
        print(f"max_ms: {max(latencies):.2f}")

    if failed:
        print("\nFailed Reads")
        print("------------")
        for result in failed[:10]:
            print(
                f"market_id={result.market_id} "
                f"http={result.status_code} error={result.error_message or 'unknown'}"
            )
        if len(failed) > 10:
            print(f"... and {len(failed) - 10} more")


def write_sweep_csv(path: str, rows: list[dict[str, float | int | str]]) -> None:
    fieldnames = list(rows[0].keys()) if rows else []
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run throughput/latency tests for read-heavy endpoints."
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument("--market-id", type=int, required=True)
    parser.add_argument("--event-id", type=int, default=None)
    parser.add_argument("--organization-id", type=int, default=None)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--market-count", type=int, default=1)
    parser.add_argument(
        "--endpoint-template",
        default="/markets/stats/liquidity",
        help=(
            "Path to benchmark. Supports {market_id}, {event_id}, and "
            "{organization_id} placeholders."
        ),
    )
    parser.add_argument("--hours", type=int, default=None)
    parser.add_argument("--span", type=int, default=None)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--warm-cache", action="store_true")
    parser.add_argument(
        "--cache-mode",
        choices=("default", "bypass"),
        default="default",
        help="Use 'bypass' to disable cache reads and cache writes for the request.",
    )
    parser.add_argument(
        "--scenario-sweep",
        action="store_true",
        help="Run cold-cache, warm-cache, and no-cache scenarios back-to-back.",
    )
    parser.add_argument("--jwt", default=os.getenv("BENCHMARK_JWT"))
    parser.add_argument("--x-user-id", type=int, default=None)
    parser.add_argument(
        "--concurrency-sweep",
        default="",
        help='Comma-separated values, e.g. "10,20,30,40,50".',
    )
    parser.add_argument(
        "--market-count-sweep",
        default="",
        help='Comma-separated values, e.g. "1,5,10,20".',
    )
    parser.add_argument("--out-csv", default="", help="Optional CSV output path for sweep.")
    return parser.parse_args()


def run_once(args: argparse.Namespace, headers: dict[str, str], *, verbose: bool) -> tuple[int, int, float]:
    if args.warm_cache:
        warm_cache(args, headers)

    if verbose:
        print("Starting market read benchmark")
        print(
            json.dumps(
                {
                    "base_url": args.base_url,
                    "user_id": args.user_id,
                    "market_id": args.market_id,
                    "event_id": args.event_id,
                    "organization_id": args.organization_id,
                    "requests": args.requests,
                    "concurrency": args.concurrency,
                    "market_count": args.market_count,
                    "endpoint_template": args.endpoint_template,
                    "warm_cache": args.warm_cache,
                    "cache_mode": args.cache_mode,
                    "hours": args.hours,
                    "span": args.span,
                },
                indent=2,
            )
        )

    started = time.perf_counter()
    results: list[ReadResult] = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [
            pool.submit(submit_read, idx, args, headers)
            for idx in range(args.requests)
        ]
        for future in as_completed(futures):
            results.append(future.result())
    total_seconds = time.perf_counter() - started

    if verbose:
        print_summary(results, total_seconds)
    succeeded = sum(1 for result in results if result.ok)
    failed = len(results) - succeeded
    return succeeded, failed, total_seconds


def run_scenarios(args: argparse.Namespace, headers: dict[str, str]) -> int:
    scenarios = [
        ScenarioSpec(name="cold-cache", warm_cache=False, cache_mode="default"),
        ScenarioSpec(name="warm-cache", warm_cache=True, cache_mode="default"),
        ScenarioSpec(name="no-cache", warm_cache=False, cache_mode="bypass"),
    ]

    print("Starting market read benchmark scenario sweep")
    print(f"requests_per_run: {args.requests}")
    print(f"market_count: {args.market_count}")
    print()
    print("scenario     succeeded  failed  throughput_rps  total_s")
    print("-" * 61)

    any_failed = False
    csv_rows: list[dict[str, float | int | str]] = []
    for scenario in scenarios:
        run_args = argparse.Namespace(**vars(args))
        run_args.warm_cache = scenario.warm_cache
        run_args.cache_mode = scenario.cache_mode
        succeeded, failed, total_seconds = run_once(run_args, headers, verbose=False)
        throughput_rps = (args.requests / total_seconds) if total_seconds > 0 else 0.0
        print(
            f"{scenario.name:<12} {succeeded:>9}  {failed:>6}  "
            f"{throughput_rps:>14.2f}  {total_seconds:>7.3f}"
        )
        csv_rows.append(
            {
                "scenario": scenario.name,
                "succeeded": succeeded,
                "failed": failed,
                "throughput_rps": round(throughput_rps, 4),
                "total_s": round(total_seconds, 4),
                "endpoint_template": args.endpoint_template,
                "market_count": args.market_count,
                "cache_mode": scenario.cache_mode,
                "warm_cache": str(scenario.warm_cache).lower(),
            }
        )
        any_failed = any_failed or (failed > 0)

    if args.out_csv.strip():
        write_sweep_csv(args.out_csv, csv_rows)
        print(f"\nWrote CSV: {args.out_csv}")
    return 1 if any_failed else 0


def main() -> int:
    args = parse_args()
    headers = build_headers(args)
    concurrency_sweep = parse_positive_int_sweep(
        args.concurrency_sweep,
        label="concurrency",
    )
    market_count_sweep = parse_positive_int_sweep(
        args.market_count_sweep,
        label="market-count",
    )
    if concurrency_sweep and market_count_sweep:
        raise ValueError(
            "Use either --concurrency-sweep or --market-count-sweep, not both together."
        )
    if args.scenario_sweep and (concurrency_sweep or market_count_sweep):
        raise ValueError(
            "Use --scenario-sweep by itself, without concurrency or market-count sweeps."
        )

    if args.scenario_sweep:
        return run_scenarios(args, headers)

    if not concurrency_sweep and not market_count_sweep:
        _succeeded, failed, _total_seconds = run_once(args, headers, verbose=True)
        return 1 if failed else 0

    sweep_values = concurrency_sweep or market_count_sweep
    sweep_label = "users" if concurrency_sweep else "markets"

    print("Starting market read benchmark sweep")
    print(f"{sweep_label}_values: {sweep_values}")
    print(f"requests_per_run: {args.requests}")
    print()
    print(f"{sweep_label:<7}  succeeded  failed  throughput_rps  total_s")
    print("-" * 58)
    any_failed = False
    csv_rows: list[dict[str, float | int | str]] = []
    for sweep_value in sweep_values:
        run_args = argparse.Namespace(**vars(args))
        if concurrency_sweep:
            run_args.concurrency = sweep_value
        else:
            run_args.market_count = sweep_value
        succeeded, failed, total_seconds = run_once(run_args, headers, verbose=False)
        throughput_rps = (args.requests / total_seconds) if total_seconds > 0 else 0.0
        print(
            f"{sweep_value:>7}  {succeeded:>9}  {failed:>6}  "
            f"{throughput_rps:>14.2f}  {total_seconds:>7.3f}"
        )
        csv_rows.append(
            {
                sweep_label: sweep_value,
                "succeeded": succeeded,
                "failed": failed,
                "throughput_rps": round(throughput_rps, 4),
                "total_s": round(total_seconds, 4),
                "endpoint_template": args.endpoint_template,
                "cache_mode": args.cache_mode,
                "warm_cache": str(bool(args.warm_cache)).lower(),
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
