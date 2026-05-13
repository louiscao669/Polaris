#!/usr/bin/env python3
"""Aggregate repeated read-sweep CSVs and plot avg/min/max across cache modes.

Input CSV format (from market_read_benchmark.py):
users,succeeded,failed,throughput_rps,total_s,endpoint_template,cache_mode,warm_cache

Example:
  python3 benchmarks/aggregate_cache_sweeps.py \
    --bypass-glob "results/sweep_read_bypass*.csv" \
    --inmemory-glob "results/sweep_read_inmemory*.csv" \
    --redis-glob "results/sweep_read_redis*.csv" \
    --out-csv "results/sweep_read_aggregate.csv" \
    --out-prefix "results/sweep_read_aggregate"
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from statistics import mean

import matplotlib.pyplot as plt


@dataclass
class RunPoint:
    users: int
    succeeded: int
    failed: int
    throughput_rps: float
    total_s: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Aggregate repeated sweep CSVs for bypass/in-memory/redis."
    )
    p.add_argument("--bypass-glob", default="", help="Glob for bypass CSV runs.")
    p.add_argument(
        "--inmemory-glob", default="", help="Glob for in-memory cache CSV runs."
    )
    p.add_argument("--redis-glob", default="", help="Glob for redis cache CSV runs.")
    p.add_argument(
        "--single-mode",
        choices=("bypass", "inmemory", "redis"),
        default="",
        help=(
            "Optional: plot only one mode. Provide only that mode's glob (or all; "
            "others will be ignored)."
        ),
    )
    p.add_argument(
        "--out-csv",
        default="results/sweep_read_aggregate.csv",
        help="Output aggregated CSV path.",
    )
    p.add_argument(
        "--out-prefix",
        default="results/sweep_read_aggregate",
        help="Output plot prefix.",
    )
    return p.parse_args()


def load_run(path: Path) -> dict[int, RunPoint]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        req = {"users", "succeeded", "failed"}
        got = set(reader.fieldnames or [])
        if not req.issubset(got):
            missing = sorted(req - got)
            raise ValueError(f"{path} missing required columns: {missing}")
        # Support both read-benchmark and write-benchmark style CSVs.
        if "throughput_rps" in got:
            throughput_key = "throughput_rps"
        elif "completion_ops_s" in got:
            throughput_key = "completion_ops_s"
        elif "submit_rps" in got:
            throughput_key = "submit_rps"
        else:
            raise ValueError(
                f"{path} missing throughput column; expected one of "
                "throughput_rps/completion_ops_s/submit_rps."
            )
        if "total_s" in got:
            total_key = "total_s"
        elif "submit_s" in got:
            total_key = "submit_s"
        else:
            raise ValueError(
                f"{path} missing duration column; expected total_s or submit_s."
            )
        out: dict[int, RunPoint] = {}
        for row in reader:
            u = int(float(row["users"]))
            out[u] = RunPoint(
                users=u,
                succeeded=int(float(row["succeeded"])),
                failed=int(float(row["failed"])),
                throughput_rps=float(row[throughput_key]),
                total_s=float(row[total_key]),
            )
    return out


def collect_runs(glob_pattern: str) -> list[dict[int, RunPoint]]:
    paths = sorted(Path(p) for p in glob(glob_pattern))
    if not paths:
        raise ValueError(f"No CSV files matched: {glob_pattern}")
    runs = [load_run(p) for p in paths]
    print(f"Loaded {len(runs)} runs from {glob_pattern}")
    return runs


def aggregate_mode(mode: str, runs: list[dict[int, RunPoint]]) -> list[dict[str, float | int | str]]:
    common_users = sorted(set.intersection(*(set(r.keys()) for r in runs)))
    if not common_users:
        raise ValueError(f"No common user values across {mode} runs.")

    rows: list[dict[str, float | int | str]] = []
    for u in common_users:
        pts = [r[u] for r in runs]
        throughput_vals = [p.throughput_rps for p in pts]
        total_vals = [p.total_s for p in pts]
        success_rates = [
            (p.succeeded / (p.succeeded + p.failed) * 100.0)
            if (p.succeeded + p.failed) > 0
            else 0.0
            for p in pts
        ]
        rows.append(
            {
                "mode": mode,
                "users": u,
                "runs": len(pts),
                "throughput_avg": mean(throughput_vals),
                "throughput_min": min(throughput_vals),
                "throughput_max": max(throughput_vals),
                "total_s_avg": mean(total_vals),
                "total_s_min": min(total_vals),
                "total_s_max": max(total_vals),
                "success_rate_avg": mean(success_rates),
                "success_rate_min": min(success_rates),
                "success_rate_max": max(success_rates),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, float | int | str]]) -> None:
    if not rows:
        raise ValueError("No rows to write.")
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _rows_for_mode(rows: list[dict[str, float | int | str]], mode: str) -> list[dict[str, float | int | str]]:
    out = [r for r in rows if str(r["mode"]) == mode]
    out.sort(key=lambda r: int(r["users"]))
    return out


def plot_metric_with_band(
    rows: list[dict[str, float | int | str]],
    *,
    metric_avg: str,
    metric_min: str,
    metric_max: str,
    ylabel: str,
    title: str,
    out_path: Path,
) -> None:
    plt.style.use("default")
    colors = {
        "bypass": "#F97316",
        "inmemory": "#06B6D4",
        "redis": "#A78BFA",
    }
    plt.figure(figsize=(11, 7), dpi=150)
    for mode in ("bypass", "inmemory", "redis"):
        mode_rows = _rows_for_mode(rows, mode)
        if not mode_rows:
            continue
        xs = [float(r["users"]) for r in mode_rows]
        av = [float(r[metric_avg]) for r in mode_rows]
        mn = [float(r[metric_min]) for r in mode_rows]
        mx = [float(r[metric_max]) for r in mode_rows]
        c = colors[mode]
        plt.plot(xs, av, marker="o", linewidth=2.2, markersize=5, color=c, label=f"{mode} avg")
        plt.fill_between(xs, mn, mx, color=c, alpha=0.2, label=f"{mode} min-max")

    ax = plt.gca()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, alpha=0.25, color="#9CA3AF")
    plt.xlabel("Users")
    plt.ylabel(ylabel)
    plt.title(title, pad=10)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def main() -> int:
    args = parse_args()
    if not (args.bypass_glob or args.inmemory_glob or args.redis_glob):
        raise ValueError("Provide at least one of --bypass-glob/--inmemory-glob/--redis-glob.")

    rows: list[dict[str, float | int | str]] = []
    if args.single_mode:
        mode_to_glob = {
            "bypass": args.bypass_glob,
            "inmemory": args.inmemory_glob,
            "redis": args.redis_glob,
        }
        target_glob = mode_to_glob[args.single_mode]
        if not target_glob:
            raise ValueError(
                f"--single-mode {args.single_mode} requires its matching --{args.single_mode}-glob."
            )
        rows += aggregate_mode(args.single_mode, collect_runs(target_glob))
    else:
        if args.bypass_glob:
            rows += aggregate_mode("bypass", collect_runs(args.bypass_glob))
        if args.inmemory_glob:
            rows += aggregate_mode("inmemory", collect_runs(args.inmemory_glob))
        if args.redis_glob:
            rows += aggregate_mode("redis", collect_runs(args.redis_glob))

    if not rows:
        raise ValueError("No rows aggregated. Check input globs.")
    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    write_csv(out_csv, rows)

    out_prefix = Path(args.out_prefix)
    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    plot_metric_with_band(
        rows,
        metric_avg="throughput_avg",
        metric_min="throughput_min",
        metric_max="throughput_max",
        ylabel="Throughput (req/s)",
        title="Read Benchmark Throughput: avg with min/max band",
        out_path=out_prefix.with_name(out_prefix.name + "_throughput_avg_minmax.png"),
    )
    plot_metric_with_band(
        rows,
        metric_avg="total_s_avg",
        metric_min="total_s_min",
        metric_max="total_s_max",
        ylabel="Total execution time (s)",
        title="Read Benchmark Duration: avg with min/max band",
        out_path=out_prefix.with_name(out_prefix.name + "_duration_avg_minmax.png"),
    )
    plot_metric_with_band(
        rows,
        metric_avg="success_rate_avg",
        metric_min="success_rate_min",
        metric_max="success_rate_max",
        ylabel="Success rate (%)",
        title="Read Benchmark Success Rate: avg with min/max band",
        out_path=out_prefix.with_name(out_prefix.name + "_success_avg_minmax.png"),
    )

    print(f"Wrote CSV: {out_csv}")
    print(
        "Wrote PNGs:\n"
        f"- {out_prefix.name}_throughput_avg_minmax.png\n"
        f"- {out_prefix.name}_duration_avg_minmax.png\n"
        f"- {out_prefix.name}_success_avg_minmax.png"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
