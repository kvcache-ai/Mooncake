"""Plot CachePilot experiment results from results/csv into results/figures."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from benchmark.metrics import ensure_dir, setup_logger

PROJECT_ROOT = _ROOT


def _load_csv(path: Path, logger: logging.Logger):
    import pandas as pd

    if not path.exists():
        logger.warning("CSV not found, skip: %s", path)
        return None
    try:
        df = pd.read_csv(path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to read %s: %s", path, exc)
        return None
    if df.empty:
        logger.warning("CSV is empty, skip: %s", path)
        return None
    return df


def plot_store_bandwidth(df, out: Path, logger: logging.Logger) -> None:
    import matplotlib.pyplot as plt

    if "mib_s" not in df.columns or df["mib_s"].isna().all():
        logger.warning("No mib_s data for store bandwidth plot; skip")
        return
    fig, ax = plt.subplots(figsize=(8, 5))
    for bs, g in df.groupby("batch_size"):
        g2 = g.dropna(subset=["mib_s"]).sort_values("value_size")
        if g2.empty:
            continue
        ax.plot(
            g2["value_size"],
            g2["mib_s"],
            marker="o",
            label=f"batch={bs}",
        )
    ax.set_xscale("log", base=2)
    ax.set_xlabel("Value size (bytes)")
    ax.set_ylabel("Bandwidth (MiB/s)")
    ax.set_title("Store Benchmark: Bandwidth by Value Size")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)


def plot_store_p99(df, out: Path, logger: logging.Logger) -> None:
    import matplotlib.pyplot as plt

    if "lat_p99" not in df.columns or df["lat_p99"].isna().all():
        logger.warning("No lat_p99 data for store p99 plot; skip")
        return
    fig, ax = plt.subplots(figsize=(8, 5))
    for vs, g in df.groupby("value_size"):
        g2 = g.dropna(subset=["lat_p99"]).sort_values("batch_size")
        if g2.empty:
            continue
        ax.plot(
            g2["batch_size"],
            g2["lat_p99"],
            marker="o",
            label=f"value={vs}",
        )
    ax.set_xlabel("Batch size")
    ax.set_ylabel("Latency p99")
    ax.set_title("Store Benchmark: p99 Latency by Batch Size")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)


def plot_prefix_length_vs_reduction(df, out: Path, logger: logging.Logger) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    for hr, g in df.groupby("cache_hit_ratio"):
        g2 = g.sort_values("prefix_tokens")
        ax.plot(
            g2["prefix_tokens"],
            g2["ttft_reduction_pct"],
            marker="o",
            label=f"hit={hr}",
        )
    ax.set_xlabel("Prefix tokens")
    ax.set_ylabel("TTFT reduction (%)")
    ax.set_title("Prefix Reuse: Length vs TTFT Reduction")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)


def plot_hit_ratio_vs_ttft(df, out: Path, logger: logging.Logger) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    for pt, g in df.groupby("prefix_tokens"):
        g2 = g.sort_values("cache_hit_ratio")
        ax.plot(
            g2["cache_hit_ratio"],
            g2["prefix_cache_ttft_ms"],
            marker="o",
            label=f"prefix={pt}",
        )
    ax.set_xlabel("Cache hit ratio")
    ax.set_ylabel("Prefix-cache TTFT (ms)")
    ax.set_title("Prefix Reuse: Hit Ratio vs TTFT")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)


def plot_scheduler_latency(df, out: Path, logger: logging.Logger) -> None:
    import matplotlib.pyplot as plt
    import numpy as np

    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(df))
    width = 0.2
    metrics = [
        ("avg_retrieval_latency_ms", "avg"),
        ("p50_latency_ms", "p50"),
        ("p95_latency_ms", "p95"),
        ("p99_latency_ms", "p99"),
    ]
    for i, (col, label) in enumerate(metrics):
        if col not in df.columns:
            continue
        ax.bar(x + i * width, df[col], width, label=label)
    ax.set_xticks(x + width * 1.5)
    ax.set_xticklabels(df["scheduler"])
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Scheduler: Retrieval Latency")
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)


def plot_scheduler_remote(df, out: Path, logger: logging.Logger) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(df["scheduler"], df["remote_traffic_mb"], color="#2a6f97")
    ax.set_ylabel("Remote traffic (MiB)")
    ax.set_title("Scheduler: Remote Traffic")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)


def plot_scheduler_hotspot(df, out: Path, logger: logging.Logger) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(df["scheduler"], df["hotspot_ratio"], color="#bc4749")
    ax.set_ylabel("Hotspot ratio (max/mean load)")
    ax.set_title("Scheduler: Hotspot Ratio")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot CachePilot CSV results.")
    parser.add_argument(
        "--csv-dir",
        default="results/csv",
        help="Directory containing result CSVs",
    )
    parser.add_argument(
        "--fig-dir",
        default="results/figures",
        help="Directory to write PNG figures",
    )
    parser.add_argument(
        "--log-file",
        default="results/logs/plot_results.log",
    )
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    fig_dir = Path(args.fig_dir)
    log_file = Path(args.log_file)
    if not csv_dir.is_absolute():
        csv_dir = PROJECT_ROOT / csv_dir
    if not fig_dir.is_absolute():
        fig_dir = PROJECT_ROOT / fig_dir
    if not log_file.is_absolute():
        log_file = PROJECT_ROOT / log_file

    ensure_dir(fig_dir)
    ensure_dir(log_file.parent)
    logger = setup_logger(log_file, name="plot_results")

    # Headless-friendly backend
    import matplotlib

    matplotlib.use("Agg")

    store_df = _load_csv(csv_dir / "store_benchmark.csv", logger)
    if store_df is not None:
        # Only plot successful-looking rows if return_code present
        if "return_code" in store_df.columns:
            ok = store_df[store_df["return_code"] == 0]
            if ok.empty:
                logger.warning(
                    "store_benchmark.csv has no successful rows; "
                    "store plots may be empty/skipped"
                )
            else:
                store_df = ok
        plot_store_bandwidth(
            store_df, fig_dir / "store_bandwidth_by_value_size.png", logger
        )
        plot_store_p99(store_df, fig_dir / "store_p99_by_batch_size.png", logger)
    else:
        logger.warning("Skipping store plots (store_benchmark.csv missing)")

    prefix_df = _load_csv(csv_dir / "prefix_reuse.csv", logger)
    if prefix_df is not None:
        plot_prefix_length_vs_reduction(
            prefix_df,
            fig_dir / "prefix_length_vs_ttft_reduction.png",
            logger,
        )
        plot_hit_ratio_vs_ttft(
            prefix_df,
            fig_dir / "cache_hit_ratio_vs_ttft.png",
            logger,
        )
    else:
        logger.warning("Skipping prefix plots")

    sched_df = _load_csv(csv_dir / "retrieval_scheduler.csv", logger)
    if sched_df is not None:
        plot_scheduler_latency(sched_df, fig_dir / "scheduler_latency.png", logger)
        plot_scheduler_remote(
            sched_df, fig_dir / "scheduler_remote_traffic.png", logger
        )
        plot_scheduler_hotspot(sched_df, fig_dir / "scheduler_hotspot.png", logger)
    else:
        logger.warning("Skipping scheduler plots")

    logger.info("Plotting complete. Figures in %s", fig_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
