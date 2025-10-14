#!/usr/bin/env python3
"""
Upload a PyTorch state_dict to Mooncake distributed store with a manifest.

Example:
  python -m mooncake.scripts.mooncake_upload_weights \
    --checkpoint /path/to/model.pt \
    --prefix my-model/weights \
    --local-hostname node1 \
    --metadata-server 127.0.0.1:2379

If your state_dict is sharded by tensor parallel rank, run this script on each
rank with different --tp-rank.
"""
from __future__ import annotations

import argparse
import os
import torch

from mooncake import create_store, save_model_to_mooncake


def parse_args():
    p = argparse.ArgumentParser("Upload weights to Mooncake")
    p.add_argument("--checkpoint", required=True, help="Path to a .pt/.pth state_dict or whole model")
    p.add_argument("--prefix", required=True, help="Mooncake key prefix")
    p.add_argument("--tp-rank", type=int, default=0)
    p.add_argument("--use-gpu-direct", action="store_true", help="Upload directly from GPU tensors")
    # store setup
    p.add_argument("--local-hostname", required=True)
    p.add_argument("--metadata-server", required=True)
    p.add_argument("--protocol", default="tcp")
    p.add_argument("--rdma-devices", default="")
    p.add_argument("--master-server-addr", default="127.0.0.1:50051")
    p.add_argument("--global-segment-size", type=int, default=256 << 20)
    p.add_argument("--local-buffer-size", type=int, default=256 << 20)
    return p.parse_args()


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

    # Load checkpoint
    ckpt = torch.load(args.checkpoint, map_location="cpu")
    if hasattr(ckpt, "state_dict"):
        state = ckpt.state_dict()
    elif isinstance(ckpt, dict) and all(isinstance(v, torch.Tensor) for v in ckpt.values()):
        # raw state_dict
        class _Dummy(torch.nn.Module):
            pass
        m = _Dummy()
        m.load_state_dict(ckpt)
        state = m.state_dict()
        model = m
    else:
        # Try to treat as whole module
        if isinstance(ckpt, torch.nn.Module):
            model = ckpt
        else:
            raise ValueError("Unsupported checkpoint format; expected Module or state_dict")

    # Build a Module from state_dict if needed
    if "model" not in locals():
        class _Wrap(torch.nn.Module):
            def __init__(self, state):
                super().__init__()
                for k, v in state.items():
                    # Register as buffers to avoid requiring gradients
                    self.register_buffer(k.replace(".", "_"), v)
            def state_dict(self, *a, **k):
                # rebuild original names best-effort
                d = super().state_dict(*a, **k)
                return {k.replace("_", ".", 1): v for k, v in d.items()}
        model = _Wrap(ckpt if isinstance(ckpt, dict) else state)

    save_model_to_mooncake(model, st, prefix=args.prefix, tp_rank=args.tp_rank, use_gpu_direct=args.use_gpu_direct)
    print(f"Uploaded weights to Mooncake prefix={args.prefix}, tp_rank={args.tp_rank}")


if __name__ == "__main__":
    main()
