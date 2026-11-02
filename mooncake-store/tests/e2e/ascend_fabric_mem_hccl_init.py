#!/usr/bin/env python3
"""Run HCCL -> Mooncake/HIXL initialization in a fresh process, without KV I/O.

Compare the default SDK VA layout with --fabric-start 128 on the affected stack.
The latter is an experimental layout, not a general fix or DRAM quota.
Requires a running isolated Mooncake master and an otherwise unused NPU.
"""

import argparse
import json
import os
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--master", required=True)
p.add_argument("--endpoint", required=True)
p.add_argument("--device", type=int, required=True)
p.add_argument("--dist-port", type=int, required=True)
p.add_argument("--fabric-start", type=int)
p.add_argument("--hixl-first", action="store_true")
a = p.parse_args()
os.environ["ASCEND_ENABLE_USE_FABRIC_MEM"] = "1"
os.environ["ASCEND_AUTO_CONNECT"] = "1"
os.environ["ASCEND_BUFFER_POOL"] = "0:0"
for key in [
    "ASCEND_GLOBAL_RESOURCE_CONFIG",
    "HCCL_INTRA_ROCE_ENABLE",
    "ASCEND_USE_ASYNC_TRANSFER",
]:
    os.environ.pop(key, None)
if a.fabric_start is not None:
    os.environ["ASCEND_GLOBAL_RESOURCE_CONFIG"] = json.dumps(
        {"fabric_memory.start_address": str(a.fabric_start)}
    )

import torch  # noqa: E402
import torch.distributed as dist  # noqa: E402
import torch_npu  # noqa: E402
from mooncake.store import MooncakeDistributedStore  # noqa: E402

torch.npu.set_device(a.device)
c = None
secondary = None
result = {
    "configuration": vars(a),
    "torch": torch.__version__,
    "torch_npu": torch_npu.__version__,
    "resource_config": os.environ.get("ASCEND_GLOBAL_RESOURCE_CONFIG"),
    "scope": "initialization only; no KV transfers submitted",
}


def hccl():
    dist.init_process_group(
        "hccl", init_method=f"tcp://127.0.0.1:{a.dist_port}", world_size=1, rank=0
    )
    dist.barrier()


try:
    if not a.hixl_first:
        hccl()
    c = MooncakeDistributedStore()
    t = time.monotonic()
    result["setup_ret"] = c.setup(
        local_hostname=a.endpoint,
        metadata_server="P2PHANDSHAKE",
        global_segment_size=0,
        local_buffer_size=0,
        protocol="ascend",
        rdma_devices="",
        master_server_addr=a.master,
    )
    result["setup_s"] = time.monotonic() - t
    if a.hixl_first and result["setup_ret"] == 0:
        hccl()
        secondary = MooncakeDistributedStore()
        host, port = a.endpoint.rsplit(":", 1)
        result["secondary_setup_ret"] = secondary.setup(
            local_hostname=f"{host}:{int(port)+1}",
            metadata_server="P2PHANDSHAKE",
            global_segment_size=0,
            local_buffer_size=0,
            protocol="ascend",
            rdma_devices="",
            master_server_addr=a.master,
        )
    result["status"] = (
        "INIT_PASS"
        if result["setup_ret"] == 0 and result.get("secondary_setup_ret", 0) == 0
        else "INIT_FAIL"
    )
except Exception as exc:
    result["status"] = "INIT_FAIL"
    result["error"] = f"{type(exc).__name__}: {exc}"
finally:
    # No KV operation was submitted; this cleanup is not a timeout-quiescence claim.
    for client, key in [(secondary, "secondary_close_ret"), (c, "close_ret")]:
        if client is not None:
            try:
                result[key] = client.close()
                if result[key] != 0:
                    result["status"] = "CLOSE_FAIL"
            except Exception as exc:
                result[key] = f"{type(exc).__name__}: {exc}"
                result["status"] = "CLOSE_FAIL"
    if dist.is_initialized():
        try:
            dist.destroy_process_group()
        except Exception as exc:
            result["status"] = "CLOSE_FAIL"
            result["destroy_error"] = f"{type(exc).__name__}: {exc}"
    print(json.dumps(result, indent=2), flush=True)
raise SystemExit(0 if result.get("status") == "INIT_PASS" else 1)
