#!/usr/bin/env python3
"""
Quick check script: read back one or more tensors from Mooncake and print shapes/dtypes.

Example:
  python -m mooncake.scripts.mooncake_check_load \
    --prefix my-model/weights \
    --keys lm_head.weight model.embed_tokens.weight \
    --local-hostname node1 \
    --metadata-server 127.0.0.1:2379
"""
from __future__ import annotations

import argparse
from typing import List

from mooncake import create_store


def parse_args():
    p = argparse.ArgumentParser("Check load from Mooncake")
    p.add_argument("--prefix", required=True)
    p.add_argument("--keys", nargs="*", default=[], help="param FQNs to check; if empty, will print manifest only")
    p.add_argument("--tp-rank", type=int, default=0)
    # store setup
    p.add_argument("--local-hostname", required=True)
    p.add_argument("--metadata-server", required=True)
    p.add_argument("--protocol", default="tcp")
    p.add_argument("--rdma-devices", default="")
    p.add_argument("--master-server-addr", default="127.0.0.1:50051")
    p.add_argument("--global-segment-size", type=int, default=256 << 20)
    p.add_argument("--local-buffer-size", type=int, default=256 << 20)
    return p.parse_args()


essential = ("manifest.json",)


def main():
    args = parse_args()
    setup = dict(
        local_hostname=args.local_hostname,
        metadata_server=args.metadata_server,
        global_segment_size=args.global_segment_size,
        local_buffer_size=args.local_buffer_size,
        protocol=args.protocol,
        rdma_devices=args.rdma_devices,
        master_server_addr=args.master_server_addr,
    )
    st = create_store(setup=setup)

    manifest_key = f"{args.prefix}/manifest.json"
    m = st.get(manifest_key)
    if not m or len(m) == 0:
        print(f"Manifest missing: {manifest_key}")
        return

    import json

    names: List[str] = json.loads(m.decode("utf-8"))
    print(f"Manifest loaded: {len(names)} params")

    if not args.keys:
        print("First 10 names:")
        for n in names[:10]:
            print("  ", n)
        return

    for k in args.keys:
        key = f"{args.prefix}/rank_{args.tp_rank}/{k}"
        t = st.get_tensor(key)
        if t is None:
            print(f"Missing tensor for {k}")
        else:
            import torch

            assert isinstance(t, torch.Tensor)
            print(f"{k}: shape={tuple(t.shape)}, dtype={t.dtype}, device={t.device}")


if __name__ == "__main__":
    main()
