"""Wrap Mooncake official store_kv_bench.py via subprocess."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Sequence

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from benchmark.metrics import (
    ensure_dir,
    parse_int_list,
    setup_logger,
    write_csv,
)
from benchmark.parse_store_output import parse_store_output

PROJECT_ROOT = _ROOT

# Default --nr-objects for most scenarios. verify_write uses 16 when the user
# leaves this default unchanged (scripts may still pass --nr-objects explicitly).
DEFAULT_NR_OBJECTS = 64
VERIFY_WRITE_DEFAULT_NR_OBJECTS = 16

DEFAULT_CANDIDATES = [
    # When CachePilot lives at Mooncake/benchmarks/cachepilot
    Path(".."),
    Path("../.."),
    Path("../Mooncake"),
    Path("../../Mooncake"),
    Path("/root/autodl-tmp/mooncake_competition/Mooncake"),
]

CSV_FIELDS = [
    "timestamp",
    "scenario",
    "io_api",
    "value_size",
    "value_size_mb",
    "batch_size",
    "runtime",
    "nr_objects",
    "return_code",
    "req_s",
    "kv_s",
    "mib_s",
    "lat_mean",
    "lat_p50",
    "lat_p95",
    "lat_p99",
    "misses",
    "verify_failures",
    "errors",
    "log_file",
]


def resolve_mooncake_root(explicit: Optional[str] = None) -> Path:
    """Resolve Mooncake repository root.

    Order: --mooncake-root, MOONCAKE_ROOT, then default candidates
    (including parent dirs when embedded under benchmarks/cachepilot).
    """
    candidates: List[Path] = []
    if explicit:
        candidates.append(Path(explicit).expanduser())
    env = os.environ.get("MOONCAKE_ROOT")
    if env:
        candidates.append(Path(env).expanduser())
    # In-tree layout: benchmarks/cachepilot -> Mooncake root is ../..
    candidates.append(PROJECT_ROOT.parent.parent)
    candidates.append(PROJECT_ROOT.parent)
    for c in DEFAULT_CANDIDATES:
        # Resolve relative to project root / cwd
        if not c.is_absolute():
            candidates.append((PROJECT_ROOT / c).resolve())
            candidates.append((Path.cwd() / c).resolve())
        else:
            candidates.append(c)

    seen = set()
    for cand in candidates:
        try:
            resolved = cand.resolve()
        except OSError:
            continue
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        bench = resolved / "mooncake-store" / "benchmarks" / "store_kv_bench.py"
        if bench.is_file():
            return resolved

    raise FileNotFoundError(
        "Mooncake root not found. Please set --mooncake-root or MOONCAKE_ROOT."
    )


def _relpath(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


def build_bench_command(
    bench_script: Path,
    *,
    scenario: str,
    io_api: str,
    value_size: int,
    batch_size: int,
    runtime: int,
    nr_objects: int,
    local_hostname: str,
    metadata_server: str,
    master_server: str,
    protocol: str,
    global_segment_size: int,
    local_buffer_size: int,
    memory_replica_num: int,
    nof_replica_num: int,
) -> List[str]:
    """Build argv for official store_kv_bench.py for one (value_size, batch_size)."""
    cmd: List[str] = [
        sys.executable,
        str(bench_script),
        "--scenario",
        scenario,
        "--io-api",
        io_api,
        "--value-size",
        str(value_size),
        "--batch-size",
        str(batch_size),
        "--nr-objects",
        str(nr_objects),
        "--local-hostname",
        local_hostname,
        "--metadata-server",
        metadata_server,
        "--master-server",
        master_server,
        "--protocol",
        protocol,
        "--global-segment-size",
        str(global_segment_size),
        "--local-buffer-size",
        str(local_buffer_size),
        "--memory-replica-num",
        str(memory_replica_num),
        "--nof-replica-num",
        str(nof_replica_num),
    ]

    if scenario == "verify_write":
        cmd.extend(["--verify", "--pattern", "0xab"])
    elif scenario == "write_perf":
        cmd.extend(["--runtime", str(runtime)])
    elif scenario == "read_perf":
        cmd.extend(
            [
                "--prepare-mode",
                "auto",
                "--phase-gap-mode",
                "sleep",
                "--phase-gap-sec",
                "1",
                "--runtime",
                str(runtime),
                "--verify",
                "--pattern",
                "0xee",
            ]
        )
    elif scenario == "mixed_rw":
        cmd.extend(
            [
                "--prepare-mode",
                "auto",
                "--runtime",
                str(runtime),
                "--rwmixread",
                "70",
            ]
        )
    else:
        cmd.extend(["--runtime", str(runtime)])

    return cmd


def run_one(
    cmd: Sequence[str],
    log_file: Path,
    logger,
    timeout: Optional[int] = None,
) -> int:
    """Run one benchmark command; capture stdout/stderr to log_file."""
    ensure_dir(log_file.parent)
    logger.info("Running: %s", " ".join(cmd))
    try:
        proc = subprocess.run(
            list(cmd),
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(PROJECT_ROOT),
        )
        combined = ""
        if proc.stdout:
            combined += proc.stdout
            if not proc.stdout.endswith("\n"):
                combined += "\n"
        if proc.stderr:
            combined += "\n===== STDERR =====\n"
            combined += proc.stderr
        log_file.write_text(combined, encoding="utf-8")
        return int(proc.returncode)
    except FileNotFoundError as exc:
        msg = f"Failed to execute benchmark: {exc}\n"
        log_file.write_text(msg, encoding="utf-8")
        logger.error(msg.strip())
        return 127
    except subprocess.TimeoutExpired as exc:
        parts = []
        if exc.stdout:
            parts.append(exc.stdout if isinstance(exc.stdout, str) else exc.stdout.decode())
        if exc.stderr:
            parts.append("===== STDERR =====")
            parts.append(
                exc.stderr if isinstance(exc.stderr, str) else exc.stderr.decode()
            )
        parts.append("\nERROR: benchmark timed out\n")
        log_file.write_text("\n".join(parts), encoding="utf-8")
        logger.error("Benchmark timed out: %s", log_file)
        return 124


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "CachePilot wrapper around Mooncake official store_kv_bench.py. "
            "Does not simulate Store API; requires a real Mooncake installation."
        )
    )
    parser.add_argument("--mooncake-root", default=None, help="Path to Mooncake repo root")
    parser.add_argument(
        "--scenario",
        default="read_perf",
        choices=["verify_write", "fill", "write_perf", "read_perf", "mixed_rw"],
        help="Benchmark scenario (default: read_perf)",
    )
    parser.add_argument(
        "--io-api",
        default="plain",
        choices=["plain", "zcopy"],
        help="IO API (default: plain)",
    )
    parser.add_argument(
        "--value-sizes",
        default="4096,65536,1048576,4194304",
        help="Comma-separated value sizes in bytes",
    )
    parser.add_argument(
        "--batch-sizes",
        default="1,4,8,16",
        help="Comma-separated batch sizes",
    )
    parser.add_argument("--runtime", type=int, default=5, help="Runtime seconds")
    parser.add_argument(
        "--nr-objects",
        type=int,
        default=DEFAULT_NR_OBJECTS,
        help=(
            f"Number of objects (default: {DEFAULT_NR_OBJECTS}; "
            "verify_write uses 16 when left at the default)"
        ),
    )
    parser.add_argument(
        "--local-hostname",
        default="127.0.0.1:50071",
        help="Local hostname for client",
    )
    parser.add_argument(
        "--metadata-server",
        default="http://127.0.0.1:8080/metadata",
        help="Metadata server URL",
    )
    parser.add_argument(
        "--master-server",
        default="127.0.0.1:50051",
        help="Master server address",
    )
    parser.add_argument("--protocol", default="tcp", help="Transport protocol")
    parser.add_argument(
        "--global-segment-size",
        type=int,
        default=67108864,
        help="Global segment size",
    )
    parser.add_argument(
        "--local-buffer-size",
        type=int,
        default=33554432,
        help="Local buffer size",
    )
    parser.add_argument(
        "--memory-replica-num",
        type=int,
        default=1,
        help="Memory replica number",
    )
    parser.add_argument(
        "--nof-replica-num",
        type=int,
        default=0,
        help="NOF replica number",
    )
    parser.add_argument(
        "--output-csv",
        default="results/csv/store_benchmark.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--log-dir",
        default="results/logs",
        help="Directory for per-run logs",
    )
    parser.add_argument(
        "--skip-on-fail",
        action="store_true",
        help="Continue remaining runs when one fails",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="Per-run timeout in seconds (default: 600)",
    )
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    if not log_dir.is_absolute():
        log_dir = PROJECT_ROOT / log_dir
    ensure_dir(log_dir)

    runner_log = log_dir / f"store_runner_{args.scenario}.log"
    logger = setup_logger(runner_log, name="mooncake_store_runner")

    try:
        mooncake_root = resolve_mooncake_root(args.mooncake_root)
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        print(str(exc), file=sys.stderr)
        # Soft-fail so run_all.sh can continue
        return 0 if args.skip_on_fail else 1

    bench_script = (
        mooncake_root / "mooncake-store" / "benchmarks" / "store_kv_bench.py"
    )
    logger.info("Using Mooncake root: %s", mooncake_root)
    logger.info("Benchmark script: %s", bench_script)

    # verify_write defaults to fewer objects unless the user overrides --nr-objects.
    if (
        args.scenario == "verify_write"
        and args.nr_objects == DEFAULT_NR_OBJECTS
    ):
        effective_nr_objects = VERIFY_WRITE_DEFAULT_NR_OBJECTS
    else:
        effective_nr_objects = args.nr_objects
    logger.info(
        "Using nr_objects=%s (scenario=%s)",
        effective_nr_objects,
        args.scenario,
    )

    value_sizes = parse_int_list(args.value_sizes)
    batch_sizes = parse_int_list(args.batch_sizes)
    if not value_sizes or not batch_sizes:
        logger.error("value-sizes and batch-sizes must be non-empty")
        return 1

    output_csv = Path(args.output_csv)
    if not output_csv.is_absolute():
        output_csv = PROJECT_ROOT / output_csv
    ensure_dir(output_csv.parent)

    rows = []
    any_fail = False
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for vs in value_sizes:
        for bs in batch_sizes:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            log_name = f"store_{args.scenario}_{args.io_api}_vs{vs}_bs{bs}_{stamp}.log"
            log_file = log_dir / log_name

            cmd = build_bench_command(
                bench_script,
                scenario=args.scenario,
                io_api=args.io_api,
                value_size=vs,
                batch_size=bs,
                runtime=args.runtime,
                nr_objects=effective_nr_objects,
                local_hostname=args.local_hostname,
                metadata_server=args.metadata_server,
                master_server=args.master_server,
                protocol=args.protocol,
                global_segment_size=args.global_segment_size,
                local_buffer_size=args.local_buffer_size,
                memory_replica_num=args.memory_replica_num,
                nof_replica_num=args.nof_replica_num,
            )

            rc = run_one(cmd, log_file, logger, timeout=args.timeout)
            text = log_file.read_text(encoding="utf-8", errors="replace")
            metrics = parse_store_output(text)

            if rc != 0:
                any_fail = True
                logger.warning(
                    "store_kv_bench failed (rc=%s) for value_size=%s batch_size=%s. "
                    "Ensure mooncake_master is running, e.g.:\n"
                    "  mooncake_master \\\n"
                    "    --enable_http_metadata_server=true \\\n"
                    "    --http_metadata_server_host=0.0.0.0 \\\n"
                    "    --http_metadata_server_port=8080 \\\n"
                    "    --eviction_high_watermark_ratio=0.95",
                    rc,
                    vs,
                    bs,
                )

            row = {
                "timestamp": ts,
                "scenario": args.scenario,
                "io_api": args.io_api,
                "value_size": vs,
                "value_size_mb": round(vs / (1024 * 1024), 6),
                "batch_size": bs,
                "runtime": args.runtime,
                "nr_objects": effective_nr_objects,
                "return_code": rc,
                "req_s": metrics.get("req_s"),
                "kv_s": metrics.get("kv_s"),
                "mib_s": metrics.get("mib_s"),
                "lat_mean": metrics.get("lat_mean"),
                "lat_p50": metrics.get("lat_p50"),
                "lat_p95": metrics.get("lat_p95"),
                "lat_p99": metrics.get("lat_p99"),
                "misses": metrics.get("misses"),
                "verify_failures": metrics.get("verify_failures"),
                "errors": metrics.get("errors"),
                "log_file": _relpath(log_file),
            }
            rows.append(row)

            if rc != 0 and not args.skip_on_fail:
                write_csv(output_csv, rows, CSV_FIELDS, append=False)
                logger.error("Aborting due to failure (omit --skip-on-fail to stop).")
                return rc

    write_csv(output_csv, rows, CSV_FIELDS, append=False)
    logger.info("Wrote %d rows to %s", len(rows), output_csv)

    if any_fail:
        logger.warning(
            "Some store benchmark runs failed. CSV still written. "
            "Start mooncake_master and re-run for real metrics."
        )
        return 0 if args.skip_on_fail else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
