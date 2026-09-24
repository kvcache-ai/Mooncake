#!/usr/bin/env python3
"""Run a small, repeatable matrix against oplog_batch_bench."""

import argparse
import fcntl
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path


PROFILES = {
    "smoke": {
        "cases": [
            {
                "name": "writer-b1",
                "mode": "writer",
                "max_entries": 1,
                "duration_sec": 5,
                "warmup_sec": 1,
            },
            {
                "name": "writer-b64",
                "mode": "writer",
                "max_entries": 64,
                "duration_sec": 5,
                "warmup_sec": 1,
            },
            {
                "name": "writer-b1024",
                "mode": "writer",
                "max_entries": 1024,
                "duration_sec": 5,
                "warmup_sec": 1,
            },
        ]
    },
    "normal": {
        "cases": [
            {
                "name": f"writer-b{size}",
                "mode": "writer",
                "max_entries": size,
                "duration_sec": 30,
                "warmup_sec": 10,
            }
            for size in (1, 8, 64, 256, 1024)
        ]
    },
    "full": {
        "cases": [
            {
                "name": f"{mode}-b{size}",
                "mode": mode,
                "max_entries": size,
                "duration_sec": 60,
                "warmup_sec": 10,
            }
            for mode in ("backend", "writer")
            for size in (1, 8, 64, 256, 1024, 4096)
        ]
    },
}


def _load_matrix(matrix_path, profile):
    if matrix_path:
        return json.loads(Path(matrix_path).read_text())
    return PROFILES[profile]


def run_sweep(
    benchmark,
    output_dir,
    repeat,
    endpoints,
    matrix_path=None,
    profile="smoke",
    resume=False,
):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / ".sweep.lock").open("w") as lock_stream:
        try:
            fcntl.flock(lock_stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f"sweep already running in {output_dir}") from error
        return _run_sweep_unlocked(
            benchmark,
            output_dir,
            repeat,
            endpoints,
            matrix_path,
            profile,
            resume,
        )


def _run_sweep_unlocked(
    benchmark, output_dir, repeat, endpoints, matrix_path, profile, resume
):
    benchmark = Path(benchmark)
    matrix = _load_matrix(matrix_path, profile)
    manifest = {
        "schema_version": 1,
        "benchmark": str(benchmark),
        "endpoints": endpoints,
        "repeat": repeat,
        "profile": profile if not matrix_path else None,
        "matrix": matrix,
        "created_unix_ns": time.time_ns(),
        "host": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
            "python": sys.version.split()[0],
        },
    }
    samples_path = output_dir / "samples.jsonl"
    manifest_path = output_dir / "manifest.json"
    completed_keys = set()
    if resume:
        previous = json.loads(manifest_path.read_text())
        for key in ("benchmark", "endpoints", "repeat", "matrix"):
            if previous.get(key) != manifest.get(key):
                raise ValueError(f"resume manifest mismatch: {key}")
        if samples_path.is_file():
            for line in samples_path.read_text().splitlines():
                row = json.loads(line)
                if row.get("exit_code") == 0:
                    completed_keys.add((row["case"], row["repeat"]))
    else:
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    failed = False
    with samples_path.open("a" if resume else "w") as samples:
        for case in matrix.get("cases", []):
            name = case["name"]
            for repeat_index in range(repeat):
                if (name, repeat_index) in completed_keys:
                    continue
                run_id = f"{name}-r{repeat_index}-{time.time_ns()}"
                result_path = output_dir / f"{run_id}.json"
                command = [
                    str(benchmark),
                    f"--endpoints={endpoints}",
                    f"--cluster_id=oplog-bench-{run_id}",
                    f"--output_json={result_path}",
                ]
                command.extend(
                    f"--{key}={str(value).lower() if isinstance(value, bool) else value}"
                    for key, value in case.items()
                    if key != "name"
                )
                started_ns = time.time_ns()
                completed = subprocess.run(command, capture_output=True, text=True)
                row = {}
                if result_path.is_file():
                    try:
                        row = json.loads(result_path.read_text())
                    except (json.JSONDecodeError, OSError):
                        row = {}
                row.update(
                    {
                        "schema_version": 1,
                        "case": name,
                        "repeat": repeat_index,
                        "cluster_id": f"oplog-bench-{run_id}",
                        "exit_code": completed.returncode,
                        "wall_time_ns": time.time_ns() - started_ns,
                        "stdout_file": f"{run_id}.stdout.log",
                        "stderr_file": f"{run_id}.stderr.log",
                    }
                )
                (output_dir / row["stdout_file"]).write_text(completed.stdout)
                (output_dir / row["stderr_file"]).write_text(completed.stderr)
                samples.write(json.dumps(row, sort_keys=True) + "\n")
                samples.flush()
                failed |= completed.returncode != 0
    return int(failed)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--endpoints", default="127.0.0.1:2379")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--profile", choices=PROFILES, default="smoke")
    parser.add_argument("--matrix", type=Path)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error("--repeat must be at least 1")
    raise SystemExit(
        run_sweep(
            benchmark=args.benchmark,
            output_dir=args.output_dir,
            repeat=args.repeat,
            endpoints=args.endpoints,
            matrix_path=args.matrix,
            profile=args.profile,
            resume=args.resume,
        )
    )


if __name__ == "__main__":
    main()
