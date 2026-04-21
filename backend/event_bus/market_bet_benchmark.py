#!/usr/bin/env python3
"""Throughput and latency benchmark for event-bus market betting."""

from __future__ import annotations

import argparse
import json
import math
import os
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


def build_headers(args: argparse.Namespace) -> dict[str, str]:
    headers: dict[str, str] = {}
    if args.jwt:
        headers["Authorization"] = f"Bearer {args.jwt}"
    if args.x_user_id is not None:
        headers["X-User-Id"] = str(args.x_user_id)
    return headers


def submit_transaction(
    index: int,
    args: argparse.Namespace,
    headers: dict[str, str],
    transaction_id: int,
) -> AcceptedOperation:
    payload = {
        "user_id": args.user_id,
        "market_id": args.market_id,
        "token_id": args.token_id,
        "side": args.side,
        "qty": args.qty,
        "transaction_id": transaction_id,
        "transaction_type": args.transaction_type,
    }
    started = time.perf_counter()
    status, body = json_request(
        "POST",
        f"{args.base_url.rstrip('/')}/v2/markets/transactions",
        payload=payload,
        headers=headers,
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
    while True:
        polls += 1
        status, body = json_request(
            "GET",
            f"{args.base_url.rstrip('/')}/v2/operations/{accepted.operation_id}",
            headers=headers,
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
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument("--market-id", type=int, required=True)
    parser.add_argument("--token-id", type=int, required=True)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--qty", type=float, default=1.0)
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
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--poll-timeout", type=float, default=30.0)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--jwt", default=os.getenv("BENCHMARK_JWT"))
    parser.add_argument("--x-user-id", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    headers = build_headers(args)

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

    print_summary(accepted_ops, completed_ops, submit_seconds, total_seconds)
    print(f"\npoll_phase_seconds: {time.perf_counter() - poll_started:.3f}")

    failed = [op for op in completed_ops if op.final_status != "succeeded"]
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        raise SystemExit(130)
