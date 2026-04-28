#!/usr/bin/env python3
"""Plot event-bus benchmark sweep results from CSV.

Expected CSV headers:
users,succeeded,failed,submit_rps,completion_ops_s,submit_s,total_s

Comparison mode (--compare-input): pass baseline CSV as --input and optimized run
(e.g. Redis read cache) as --compare-input. Extra PNGs report % improvement.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt

plt.style.use("dark_background")

COLORS = {
    "throughput": "#4F46E5",
    "submit": "#059669",
    "total": "#DC2626",
    "success": "#0EA5E9",
}


def _beautify_axes() -> None:
    ax = plt.gca()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, alpha=0.18, linewidth=0.8, color="#9CA3AF")


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
    parser.add_argument(
        "--compare-input",
        default="",
        help="Optional second CSV path to overlay for comparison.",
    )
    parser.add_argument(
        "--label-a",
        default="Run A",
        help="Legend label for --input.",
    )
    parser.add_argument(
        "--label-b",
        default="Run B",
        help="Legend label for --compare-input.",
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
    completion_ops_s = [r["completion_ops_s"] for r in rows]

    plt.figure(figsize=(10, 6), dpi=160)
    plt.plot(
        users,
        completion_ops_s,
        marker="o",
        linewidth=2.5,
        markersize=6,
        color=COLORS["throughput"],
        label="Completion throughput (ops/s)",
    )
    plt.xlabel("Concurrent users")
    plt.ylabel("Ops per second")
    plt.title("Benchmark Throughput vs Concurrency", pad=12)
    _beautify_axes()
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def plot_throughput_compare(
    rows_a: list[dict[str, float]],
    rows_b: list[dict[str, float]],
    out_path: Path,
    *,
    label_a: str,
    label_b: str,
) -> None:
    users_a = [r["users"] for r in rows_a]
    users_b = [r["users"] for r in rows_b]
    completion_a = [r["completion_ops_s"] for r in rows_a]
    completion_b = [r["completion_ops_s"] for r in rows_b]

    plt.figure(figsize=(10, 6), dpi=160)
    plt.plot(
        users_a,
        completion_a,
        marker="o",
        linewidth=2.5,
        markersize=6,
        color=COLORS["throughput"],
        label=f"{label_a} completion_ops_s",
    )
    plt.plot(
        users_b,
        completion_b,
        marker="s",
        linewidth=2.5,
        markersize=6,
        color="#F59E0B",
        label=f"{label_b} completion_ops_s",
    )
    plt.xlabel("Concurrent users")
    plt.ylabel("Ops per second")
    plt.title("Benchmark Throughput Comparison", pad=12)
    _beautify_axes()
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def plot_duration(rows: list[dict[str, float]], out_path: Path) -> None:
    users = [r["users"] for r in rows]
    submit_s = [r["submit_s"] for r in rows]
    total_s = [r["total_s"] for r in rows]

    plt.figure(figsize=(10, 6), dpi=160)
    plt.plot(
        users,
        submit_s,
        marker="o",
        linewidth=2.5,
        markersize=6,
        color=COLORS["submit"],
        label="Submit phase (s)",
    )
    plt.plot(
        users,
        total_s,
        marker="o",
        linewidth=2.5,
        markersize=6,
        color=COLORS["total"],
        label="End-to-end (s)",
    )
    plt.xlabel("Concurrent users")
    plt.ylabel("Seconds")
    plt.title("Benchmark Runtime vs Concurrency", pad=12)
    _beautify_axes()
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def plot_duration_compare(
    rows_a: list[dict[str, float]],
    rows_b: list[dict[str, float]],
    out_path: Path,
    *,
    label_a: str,
    label_b: str,
) -> None:
    users_a = [r["users"] for r in rows_a]
    users_b = [r["users"] for r in rows_b]
    total_a = [r["total_s"] for r in rows_a]
    total_b = [r["total_s"] for r in rows_b]

    plt.figure(figsize=(10, 6), dpi=160)
    plt.plot(
        users_a,
        total_a,
        marker="o",
        linewidth=2.5,
        markersize=6,
        color=COLORS["total"],
        label=f"{label_a} total_s",
    )
    plt.plot(
        users_b,
        total_b,
        marker="s",
        linewidth=2.5,
        markersize=6,
        color="#22D3EE",
        label=f"{label_b} total_s",
    )
    plt.xlabel("Concurrent users")
    plt.ylabel("Seconds")
    plt.title("Benchmark Duration Comparison", pad=12)
    _beautify_axes()
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

    plt.figure(figsize=(10, 6), dpi=160)
    plt.plot(
        users,
        success_rates,
        marker="o",
        linewidth=2.5,
        markersize=6,
        color=COLORS["success"],
    )
    plt.xlabel("Concurrent users")
    plt.ylabel("Success rate (%)")
    plt.title("Benchmark Success Rate vs Concurrency", pad=12)
    plt.ylim(0, 100)
    _beautify_axes()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def _success_rates(rows: list[dict[str, float]]) -> tuple[list[float], list[float]]:
    users = [r["users"] for r in rows]
    rates: list[float] = []
    for r in rows:
        total = r["succeeded"] + r["failed"]
        rates.append((r["succeeded"] / total * 100.0) if total else 0.0)
    return users, rates


def _pair_by_users(
    rows_a: list[dict[str, float]],
    rows_b: list[dict[str, float]],
) -> tuple[list[float], list[dict[str, float]], list[dict[str, float]]]:
    da = {int(r["users"]): r for r in rows_a}
    db = {int(r["users"]): r for r in rows_b}
    users = sorted(set(da.keys()) & set(db.keys()))
    return [float(u) for u in users], [da[u] for u in users], [db[u] for u in users]


def plot_total_time_improvement_pct(
    baseline_rows: list[dict[str, float]],
    optimized_rows: list[dict[str, float]],
    out_path: Path,
    *,
    baseline_label: str,
    optimized_label: str,
) -> None:
    """Percent reduction in total_s vs baseline (positive = optimized faster)."""
    _, base, opt = _pair_by_users(baseline_rows, optimized_rows)
    xs: list[float] = []
    ys: list[float] = []
    for r0, r1 in zip(base, opt):
        u = r0["users"]
        t0, t1 = r0["total_s"], r1["total_s"]
        if t0 <= 0:
            continue
        xs.append(u)
        ys.append((t0 - t1) / t0 * 100.0)

    plt.figure(figsize=(10, 6), dpi=160)
    plt.plot(
        xs,
        ys,
        marker="o",
        linewidth=2.5,
        markersize=6,
        color="#34D399",
        label=f"{optimized_label} vs {baseline_label}",
    )
    plt.axhline(0.0, color="#6B7280", linewidth=1.0, linestyle="--", alpha=0.7)
    plt.xlabel("Concurrent users")
    plt.ylabel("Total time improvement over baseline (%)")
    plt.title(
        "End-to-end time improvement (positive = faster than baseline)",
        pad=12,
    )
    _beautify_axes()
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def plot_completion_throughput_improvement_pct(
    baseline_rows: list[dict[str, float]],
    optimized_rows: list[dict[str, float]],
    out_path: Path,
    *,
    baseline_label: str,
    optimized_label: str,
) -> None:
    """Percent gain in completion_ops_s vs baseline (positive = higher throughput)."""
    _, base, opt = _pair_by_users(baseline_rows, optimized_rows)
    xs: list[float] = []
    ys: list[float] = []
    for r0, r1 in zip(base, opt):
        u = r0["users"]
        c0, c1 = r0["completion_ops_s"], r1["completion_ops_s"]
        if c0 <= 0:
            continue
        xs.append(u)
        ys.append((c1 - c0) / c0 * 100.0)

    plt.figure(figsize=(10, 6), dpi=160)
    plt.plot(
        xs,
        ys,
        marker="s",
        linewidth=2.5,
        markersize=6,
        color="#F472B6",
        label=f"{optimized_label} vs {baseline_label}",
    )
    plt.axhline(0.0, color="#6B7280", linewidth=1.0, linestyle="--", alpha=0.7)
    plt.xlabel("Concurrent users")
    plt.ylabel("Completion throughput improvement over baseline (%)")
    plt.title(
        "Throughput improvement (positive = higher ops/s than baseline)",
        pad=12,
    )
    _beautify_axes()
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def plot_success_rate_compare(
    rows_a: list[dict[str, float]],
    rows_b: list[dict[str, float]],
    out_path: Path,
    *,
    label_a: str,
    label_b: str,
) -> None:
    users_a, rates_a = _success_rates(rows_a)
    users_b, rates_b = _success_rates(rows_b)

    plt.figure(figsize=(10, 6), dpi=160)
    plt.plot(
        users_a,
        rates_a,
        marker="o",
        linewidth=2.5,
        markersize=6,
        color=COLORS["success"],
        label=f"{label_a} success %",
    )
    plt.plot(
        users_b,
        rates_b,
        marker="s",
        linewidth=2.5,
        markersize=6,
        color="#A78BFA",
        label=f"{label_b} success %",
    )
    plt.xlabel("Concurrent users")
    plt.ylabel("Success rate (%)")
    plt.title("Benchmark Success Rate Comparison", pad=12)
    plt.ylim(0, 100)
    _beautify_axes()
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    rows = load_rows(input_path)
    compare_path = Path(args.compare_input).expanduser().resolve() if args.compare_input else None

    out_prefix = Path(args.out_prefix).expanduser()
    if compare_path is None:
        plot_throughput(rows, out_prefix.with_name(out_prefix.name + "_throughput.png"))
        plot_duration(rows, out_prefix.with_name(out_prefix.name + "_duration.png"))
        plot_success_rate(rows, out_prefix.with_name(out_prefix.name + "_success_rate.png"))
    else:
        rows_b = load_rows(compare_path)
        plot_throughput_compare(
            rows,
            rows_b,
            out_prefix.with_name(out_prefix.name + "_throughput.png"),
            label_a=args.label_a,
            label_b=args.label_b,
        )
        plot_duration_compare(
            rows,
            rows_b,
            out_prefix.with_name(out_prefix.name + "_duration.png"),
            label_a=args.label_a,
            label_b=args.label_b,
        )
        plot_success_rate_compare(
            rows,
            rows_b,
            out_prefix.with_name(out_prefix.name + "_success_rate.png"),
            label_a=args.label_a,
            label_b=args.label_b,
        )
        plot_total_time_improvement_pct(
            rows,
            rows_b,
            out_prefix.with_name(out_prefix.name + "_time_improvement_pct.png"),
            baseline_label=args.label_a,
            optimized_label=args.label_b,
        )
        plot_completion_throughput_improvement_pct(
            rows,
            rows_b,
            out_prefix.with_name(out_prefix.name + "_throughput_improvement_pct.png"),
            baseline_label=args.label_a,
            optimized_label=args.label_b,
        )

    print("Wrote:")
    print(f"- {out_prefix.name}_throughput.png")
    print(f"- {out_prefix.name}_duration.png")
    print(f"- {out_prefix.name}_success_rate.png")
    if compare_path is not None:
        print(f"- {out_prefix.name}_time_improvement_pct.png")
        print(f"- {out_prefix.name}_throughput_improvement_pct.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
