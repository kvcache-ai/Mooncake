#!/usr/bin/env python3
"""Run the RFC #5169 native TENT matrix; preserve raw results and timing traces."""

import argparse
import json
import os
import shlex
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tebench", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--label", required=True, help="before or after + commit SHA")
    parser.add_argument("--target-2", default="<target-segment-for-2-lanes>")
    parser.add_argument("--target-8", default="<target-segment-for-8-lanes>")
    parser.add_argument("--lanes", type=int, nargs="+", choices=(2, 8), default=(2, 8))
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument(
        "--trace", action="store_true", help="separate instrumented run"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    base = json.loads(args.config.read_text())
    args.out.mkdir(parents=True, exist_ok=True)
    binary = args.tebench.resolve()
    manifest = {"label": args.label, "binary": str(binary), "runs": []}
    for lanes in args.lanes:
        config = json.loads(json.dumps(base))
        # tebench uses contiguous addresses. Disable merging only in this
        # benchmark to retain the requested descriptor count at the TENT API.
        config["merge_requests"] = False
        rdma = config.setdefault("transports", {}).setdefault("rdma", {})
        rdma["num_lanes"] = lanes
        config_path = (args.out / f"tent-{lanes}.json").resolve()
        config_path.write_text(json.dumps(config, indent=2) + "\n")
        target = getattr(args, f"target_{lanes}")
        if not args.dry_run and target.startswith("<"):
            parser.error(f"--target-{lanes} is required for execution")
        for batch in (128, 16384, 65536, 131072):
            prefix = args.out / f"lanes-{lanes}-batch-{batch}"
            command = [
                str(binary),
                "--backend=tent",
                "--xport_type=rdma",
                "--seg_type=DRAM",
                "--local_gpu_id=-1",
                "--op_type=write",
                "--total_buffer_size=2147483648",
                "--start_block_size=16384",
                "--max_block_size=16384",
                f"--start_batch_size={batch}",
                f"--max_batch_size={batch}",
                "--start_num_threads=1",
                "--max_num_threads=1",
                f"--duration={args.duration}",
                f"--target_seg_name={target}",
                f"--result_output_jsonl={prefix.resolve()}.results.jsonl",
            ]
            env = os.environ.copy()
            env.update(
                MC_USE_TENT="1",
                MC_TENT_CONF=str(config_path),
                MC_TENT_TRACE_TRANSFER="1" if args.trace else "0",
            )
            manifest["runs"].append(
                {
                    "lanes": lanes,
                    "batch": batch,
                    "command": command,
                    "config": config,
                    "trace": args.trace,
                }
            )
            print(
                f"MC_USE_TENT=1 MC_TENT_CONF={shlex.quote(str(config_path))} "
                f"MC_TENT_TRACE_TRANSFER={env['MC_TENT_TRACE_TRANSFER']} "
                + shlex.join(command),
                flush=True,
            )
            if not args.dry_run:
                with prefix.with_suffix(".log").open("w") as log:
                    subprocess.run(
                        command,
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=True,
                    )
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
