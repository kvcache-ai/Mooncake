#!/usr/bin/env python3
"""Check tebench's effective operation labels using two TENT TCP processes."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time


def run(tebench, output, config=None):
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError("output directory must be empty")
    if config is None:
        config = output / "tcp.json"
        config.write_text(
            json.dumps(
                {
                    "rpc_server_hostname": "127.0.0.1",
                    "rpc_server_threads": 1,
                    "transports": {
                        name: {"enable": name == "tcp"}
                        for name in (
                            "tcp",
                            "hp_tcp",
                            "rdma",
                            "shm",
                            "nvlink",
                            "mnnvl",
                            "gds",
                            "io_uring",
                            "bufio",
                            "ub",
                            "ascend",
                            "ascend_direct",
                            "sunrise_link",
                            "mpcomm",
                            "hylink",
                            "tpu",
                            "xpu",
                            "nccl",
                        )
                    },
                }
            )
        )
    env = {
        key: value for key, value in os.environ.items() if not key.startswith("MC_TENT")
    }
    env["MC_TENT_CONF"] = str(config.resolve())
    common = [
        str(tebench.resolve()),
        "--backend=tent",
        "--xport_type=tcp",
        "--tent_transport_hint=tcp",
        "--metadata_type=p2p",
        "--seg_type=DRAM",
        "--total_buffer_size=1048576",
        "--start_num_threads=1",
        "--max_num_threads=1",
        "--duration=1",
        "--start_block_size=4096",
        "--max_block_size=4096",
        "--start_batch_size=1",
        "--max_batch_size=1",
        "--rpc_server_port=0",
    ]
    cases = [
        (op, check, "mix" if check else op, [])
        for op in ("read", "write", "mix")
        for check in (True, False)
    ]
    workload = json.dumps(
        [
            {
                "name": "foreground",
                "threads": 1,
                "block_size": 4096,
                "batch_size": 1,
                "intent_type": "foreground_get",
                "slo_us": 1000,
                "weight": 1,
            }
        ]
    )
    cases.append(("read", True, "mix", ["--workload_classes_json=" + workload]))
    cases.extend((op, True, None, []) for op in ("write_seed", "read_verify"))
    failures, results = [], []
    commands = {"target": common, "clients": [], "MC_TENT_CONF": env["MC_TENT_CONF"]}
    target_log = output / "target.log"
    with target_log.open("w") as log:
        target = subprocess.Popen(common, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                text = target_log.read_text()
                match = re.search(r"--target_seg_name=(127\.0\.0\.1:\d+)", text)
                if match:
                    if "TCP transport installed:" not in text:
                        raise AssertionError(text)
                    break
                if target.poll() is not None:
                    raise RuntimeError("target exited before readiness: " + text)
                time.sleep(0.05)
            else:
                raise TimeoutError("target readiness: " + target_log.read_text())
            for index, (op, check, expected, extra) in enumerate(cases):
                name = f"{index}-{op}-{check}"
                result_path = output / (name + ".jsonl")
                command = (
                    common
                    + [
                        "--target_seg_name=" + match[1],
                        "--op_type=" + op,
                        "--check_consistency=" + str(check).lower(),
                        "--result_output_jsonl=" + str(result_path),
                    ]
                    + extra
                )
                commands["clients"].append(command)
                with (output / (name + ".log")).open("w") as client_log:
                    result = subprocess.run(
                        command,
                        env=env,
                        stdout=client_log,
                        stderr=subprocess.STDOUT,
                        timeout=15,
                    )
                text = (output / (name + ".log")).read_text()
                rows = (
                    [json.loads(line) for line in result_path.read_text().splitlines()]
                    if result_path.exists()
                    else []
                )
                results.append({"case": name, "exit": result.returncode, "rows": rows})
                if expected is None:
                    valid = (
                        result.returncode == 1
                        and not rows
                        and "--check_consistency cannot be combined with" in text
                    )
                else:
                    valid = (
                        result.returncode == 0
                        and "TCP transport installed:" in text
                        and len(rows) == 1
                        and rows[0]["op_type"] == expected
                        and rows[0]["backend"] == "tent"
                        and rows[0]["aggregate_operations"] > 0
                        and rows[0]["aggregate_transferred_bytes"] > 0
                    )
                if not valid:
                    failures.append(name)
                    print(
                        f"FAIL {name}: expected {expected}, exit {result.returncode}, {rows}"
                    )
                    print(text)
        finally:
            if target.poll() is None:
                target.terminate()
            try:
                target_exit = target.wait(timeout=5)
            except subprocess.TimeoutExpired:
                target.kill()
                target.wait(timeout=5)
                raise RuntimeError("target failed to stop") from None
            (output / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
            (output / "results.json").write_text(
                json.dumps(
                    {
                        "target_exit": target_exit,
                        "cases": results,
                        "failures": failures,
                    },
                    indent=2,
                )
                + "\n"
            )
    if target_exit != 0:
        raise AssertionError(target_log.read_text())
    if failures:
        raise AssertionError("incorrect mode reports: " + ", ".join(failures))
    print(f"PASS: {len(cases)} operation-mode cases over TENT TCP")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tebench", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--config", type=Path)
    args = parser.parse_args()
    if args.output_dir:
        run(args.tebench, args.output_dir.resolve(), args.config)
    else:
        with tempfile.TemporaryDirectory(prefix="tebench-op-mode-") as temporary:
            run(args.tebench, Path(temporary), args.config)
