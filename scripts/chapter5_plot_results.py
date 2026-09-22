#!/usr/bin/env python3
"""
Create Chapter 5 plots from benchmark CSV files.

Usage from project root after running mvn test:
    python3 scripts/chapter5_plot_results.py target/chapter5-benchmarks

Outputs PNG charts in target/chapter5-benchmarks/plots/.
Requires: pandas, matplotlib.
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 70.36 s -- in com.database.tttdb.AppTest
[INFO] Running com.database.tttdb.DBMSIndexTypeIntegrationTest
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.015 s -- in com.database.tttdb.DBMSIndexTypeIntegrationTest
[INFO] Running com.database.tttdb.benchmark.Chapter5DbmsBenchmarkTest
f[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 13088 s -- in com.database.tttdb.benchmark.Chapter5DbmsBenchmarkTest
[INFO] Running com.database.tttdb.benchmark.Chapter5EnvironmentReportTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.008 s -- in com.database.tttdb.benchmark.Chapter5EnvironmentReportTest
[INFO] Running com.database.tttdb.benchmark.Chapter5IndexOnlyBenchmarkTest
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 30.80 s -- in com.database.tttdb.benchmark.Chapter5IndexOnlyBenchmarkTest
[INFO]
[INFO] Results:
[INFO]
[INFO] Tests run: 225, Failures: 0, Errors: 0, Skipped: 0
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  03:39 h
[INFO] Finished at: 2026-07-16T03:25:14+03:00
[INFO] ------------------------------------------------------------------------
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


WORKLOAD_TITLES = {
    "sequential_unique_insert": "Index-only Sequential Insert Time",
    "random_unique_insert": "Index-only Random Insert Time",
    "equality_search_hit": "Index-only Equality Search Latency",
    "duplicate_key_search": "Index-only Duplicate-Key Search Latency",
    "range_search": "Index-only Range Search Time",
    "delete_existing_keys": "Index-only Delete Time",
    "memory_unique_index_after_insert": "Index-only Memory Usage",
    "dbms_safe_batch_insert_sequential": "DBMS Sequential Insert Time",
    "dbms_safe_batch_insert_random": "DBMS Random Insert Time",
    "dbms_equality_search_primary_key": "DBMS Equality Search Latency",
    "dbms_duplicate_equality_search_secondary_index": "DBMS Duplicate-Key Search Latency",
    "dbms_range_search_secondary_index": "DBMS Range Search Time",
    "dbms_delete_by_primary_key": "DBMS Delete Time",
    "dbms_memory_after_build": "DBMS Memory Usage",
}


def load_csvs(root: Path) -> pd.DataFrame:
    frames = []
    for name in ["index-only-results.csv", "dbms-level-results.csv"]:
        file = root / name
        if file.exists():
            frames.append(pd.read_csv(file))
    if not frames:
        raise SystemExit(f"No benchmark result CSV files found in {root}")
    return pd.concat(frames, ignore_index=True)


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["benchmark_level", "index_type", "workload", "dataset_size"]
    return (
        df.groupby(group_cols, as_index=False)
        .agg(
            elapsed_ms_mean=("elapsed_ms", "mean"),
            elapsed_ms_std=("elapsed_ms", "std"),
            avg_ns_per_op_mean=("avg_ns_per_op", "mean"),
            ops_per_sec_mean=("ops_per_sec", "mean"),
            memory_mb_mean=("memory_mb", "mean"),
        )
    )


def plot_workload(summary: pd.DataFrame, workload: str, out_dir: Path) -> None:
    sub = summary[summary["workload"] == workload].copy()
    if sub.empty:
        return
    y_col = "memory_mb_mean" if "memory" in workload else "elapsed_ms_mean"
    y_label = "Memory usage (MB)" if "memory" in workload else "Elapsed time (ms)"

    plt.figure()
    for index_type, group in sub.groupby("index_type"):
        group = group.sort_values("dataset_size")
        plt.plot(group["dataset_size"], group[y_col], marker="o", label=index_type)
    plt.xlabel("Number of records")
    plt.ylabel(y_label)
    plt.title(WORKLOAD_TITLES.get(workload, workload))
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    out_file = out_dir / f"{workload}.png"
    plt.savefig(out_file, dpi=180)
    plt.close()


def main() -> None:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("target/chapter5-benchmarks")
    out_dir = root / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)
    df = load_csvs(root)
    summary = summarize(df)
    summary.to_csv(root / "summary.csv", index=False)
    for workload in sorted(summary["workload"].unique()):
        plot_workload(summary, workload, out_dir)
    print(f"Wrote summary to {root / 'summary.csv'}")
    print(f"Wrote plots to {out_dir}")


if __name__ == "__main__":
    main()
