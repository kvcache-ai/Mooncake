"""Exercise the native workload over local TENT/TCP; never claim RDMA evidence.
Run in Linux/WSL after building tent_rdma_workload and its unit test.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import time

root = Path(__file__).resolve().parents[3]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--binary",
    type=Path,
    default=Path("build/rdma-batch/mooncake-transfer-engine/tent/tent_rdma_workload"),
)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
args.binary = args.binary.resolve()
args.output = args.output.resolve()
args.output.mkdir(parents=True, exist_ok=False)
# Reproduce the wrapper's inherited-config hazard. The native explicit spec
# must still install its policies/transport, as checked by real TCP transfers.
env = dict(
    os.environ, MC_TENT_CONF='{"policy":[],"transports":{"tcp":{"enable":false}}}'
)
results = []
for mode in ("burst", "biased_backlog"):
    configs = {}
    for role, port in (("server", 19871), ("client", 19872)):
        spec = dict(
            role=role,
            mode=mode,
            count=3,
            burst_size=3,
            interval_us=5000,
            background_mib=32,
            control_tcp=True,
            warmup=1,
            output=str(args.output / f"{mode}-{role}"),
            peer="127.0.0.1:19871",
            timeout_ms=10000,
            engine=dict(
                local_segment_name=f"127.0.0.1:{port}",
                metadata_type="p2p",
                rpc_server_hostname="127.0.0.1",
                rpc_server_port=port,
            ),
        )
        config = args.output / f"{mode}-{role}.json"
        config.write_text(json.dumps(spec, indent=2) + "\n", encoding="utf-8")
        configs[role] = config
        with (args.output / f"{mode}-{role}-dry.json").open("wb") as output:
            subprocess.run(
                [str(args.binary), "--config", str(config), "--dry-run"],
                cwd=root,
                stdout=output,
                check=True,
            )
    with (args.output / f"{mode}-server.log").open("wb") as server_log:
        server = subprocess.Popen(
            [str(args.binary), "--config", str(configs["server"])],
            cwd=root,
            stdout=server_log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        try:
            deadline = time.monotonic() + 30
            ready = args.output / f"{mode}-server/ready.json"
            while not ready.exists():
                if server.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"{mode} server did not become ready; inspect log"
                    )
                time.sleep(0.05)
            with (args.output / f"{mode}-client.log").open("wb") as client_log:
                completed = subprocess.run(
                    [str(args.binary), "--config", str(configs["client"])],
                    cwd=root,
                    stdout=client_log,
                    stderr=subprocess.STDOUT,
                    timeout=60,
                    check=False,
                    env=env,
                )
            assert completed.returncode == 0, (mode, completed.returncode)
            folder = args.output / f"{mode}-client"
            summary = json.loads((folder / "summary.json").read_text())
            effective = json.loads((folder / "engine-effective.json").read_text())
            assert [p["name"] for p in effective["policy"]] == ["background", "target"]
            assert effective["transports"]["tcp"]["enable"]
            records = [
                json.loads(line)
                for line in (folder / "requests.jsonl").read_text().splitlines()
            ]
            assert summary["evidence"] == "tcp_control_only"
            assert summary["target"]["success"] == 3
            assert all(r["detail"]["data_verified"] for r in records)
            assert all(
                a["offset"] + a["bytes"] <= b["offset"]
                for a, b in zip(records, records[1:])
            )
            if mode == "burst":
                assert max(r["submit_ns"] for r in records) < min(
                    r["terminal_visible_ns"] for r in records
                )
            else:
                assert summary["background"]["success"] == 3
                assert all(
                    r["detail"]["backlog_validity"] == "control_only"
                    for r in records
                    if r["group"] == "target"
                )
            results.append(
                dict(
                    mode=mode,
                    evidence="tcp_control_only",
                    verified_requests=len(records),
                    disjoint_slots=True,
                    asynchronous_arrivals=True,
                    exit_code=completed.returncode,
                )
            )
        finally:
            server.terminate()
            try:
                server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
(args.output / "control-results.json").write_text(
    json.dumps(results, indent=2) + "\n", encoding="utf-8"
)
print(json.dumps(results, indent=2))
