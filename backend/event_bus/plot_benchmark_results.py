#!/usr/bin/env python3
"""Plot event-bus benchmark sweep results from CSV.

Expected CSV headers:
users,succeeded,failed,submit_rps,completion_ops_s,submit_s,total_s
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot benchmark sweep CSV metrics into PNG charts."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to CSV file with benchmark sweep results.",
    )
    parser.add_argument(
        "--out-prefix",
        default="benchmark_plot",
        help="Output file prefix (default: benchmark_plot).",
    )
    return parser.parse_args()


def load_rows(csv_path: Path) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "users",
            "succeeded",
            "failed",
            "submit_rps",
            "completion_ops_s",
            "submit_s",
            "total_s",
        }
        if not required.issubset(set(reader.fieldnames or [])):
            missing = sorted(required - set(reader.fieldnames or []))
            raise ValueError(f"CSV is missing required columns: {missing}")
        for row in reader:
            rows.append(
                {
                    "users": float(row["users"]),
                    "succeeded": float(row["succeeded"]),
                    "failed": float(row["failed"]),
                    "submit_rps": float(row["submit_rps"]),
                    "completion_ops_s": float(row["completion_ops_s"]),
                    "submit_s": float(row["submit_s"]),
                    "total_s": float(row["total_s"]),
                }
            )
    if not rows:
        raise ValueError("CSV has no rows.")
    rows.sort(key=lambda r: r["users"])
    return rows


def plot_throughput(rows: list[dict[str, float]], out_path: Path) -> None:
    users = [r["users"] for r in rows]
    submit_rps = [r["submit_rps"] for r in rows]
    completion_ops_s = [r["completion_ops_s"] for r in rows]

    plt.figure(figsize=(9, 5))
    plt.plot(users, submit_rps, marker="o", label="Submit throughput (req/s)")
    plt.plot(users, completion_ops_s, marker="o", label="Completion throughput (ops/s)")
    plt.xlabel("Concurrent users")
    plt.ylabel("Ops per second")
    plt.title("Benchmark Throughput vs Concurrency")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def plot_duration(rows: list[dict[str, float]], out_path: Path) -> None:
    users = [r["users"] for r in rows]
    submit_s = [r["submit_s"] for r in rows]
    total_s = [r["total_s"] for r in rows]

    plt.figure(figsize=(9, 5))
    plt.plot(users, submit_s, marker="o", label="Submit phase (s)")
    plt.plot(users, total_s, marker="o", label="End-to-end (s)")
    plt.xlabel("Concurrent users")
    plt.ylabel("Seconds")
    plt.title("Benchmark Runtime vs Concurrency")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def plot_success_rate(rows: list[dict[str, float]], out_path: Path) -> None:
    users = [r["users"] for r in rows]
    success_rates: list[float] = []
    for r in rows:
        total = r["succeeded"] + r["failed"]
        success_rates.append((r["succeeded"] / total * 100.0) if total else 0.0)

    plt.figure(figsize=(9, 5))
    plt.plot(users, success_rates, marker="o")
    plt.xlabel("Concurrent users")
    plt.ylabel("Success rate (%)")
    plt.title("Benchmark Success Rate vs Concurrency")
    plt.ylim(0, 100)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    rows = load_rows(input_path)

    out_prefix = Path(args.out_prefix).expanduser()
    plot_throughput(rows, out_prefix.with_name(out_prefix.name + "_throughput.png"))
    plot_duration(rows, out_prefix.with_name(out_prefix.name + "_duration.png"))
    plot_success_rate(rows, out_prefix.with_name(out_prefix.name + "_success_rate.png"))

    print("Wrote:")
    print(f"- {out_prefix.name}_throughput.png")
    print(f"- {out_prefix.name}_duration.png")
    print(f"- {out_prefix.name}_success_rate.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
