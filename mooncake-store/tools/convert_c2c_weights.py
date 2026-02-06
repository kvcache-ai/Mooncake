#!/usr/bin/env python3
"""
C2C Projector Weight Converter

Convert HuggingFace PyTorch format to Mooncake binary format.
Model dimensions are auto-fetched from HuggingFace config.json.

Usage:
    python convert_c2c_weights.py \
        --checkpoint nics-efc/C2C_Fuser \
        --fuser qwen3_0.6b+qwen3_4b_Fuser \
        --src-hf Qwen/Qwen3-4B \
        --tgt-hf Qwen/Qwen3-0.6B \
        --output projector.bin

C2C projector architecture (per-layer):
    key_in: [hidden, src_dim + tgt_dim]    # input projection
    key_mlp1: RMSNorm + FFN               # shared embedding
    key_scalar_mlp2 + head: -> [num_heads] # scalar path
    key_proj_mlp2 + out: -> [tgt_dim]      # projection path
    key_gate_logit: scalar                 # gating
"""

import argparse
import json
import struct
import numpy as np
from pathlib import Path
from urllib.request import urlopen, Request

try:
    import torch
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False


# ============================================================================
# Auto-fetch model dimensions from HuggingFace
# ============================================================================
def fetch_hf_config(hf_model: str) -> dict:
    """Fetch model params from HuggingFace config.json"""
    url = f"https://huggingface.co/{hf_model}/resolve/main/config.json"
    print(f"  Fetching: {url}")
    req = Request(url, headers={"User-Agent": "mooncake-c2c/1.0"})
    with urlopen(req, timeout=15) as resp:
        cfg = json.loads(resp.read())

    num_layers = cfg["num_hidden_layers"]
    num_kv_heads = cfg["num_key_value_heads"]
    head_dim = cfg.get("head_dim",
                       cfg["hidden_size"] // cfg["num_attention_heads"])

    return {
        "num_layers": num_layers,
        "num_kv_heads": num_kv_heads,
        "head_dim": head_dim,
    }


def convert_layer_projector(state_dict: dict, src_dim: int, tgt_dim: int, hidden_dim: int):
    """
    Convert single-layer projector weights to simplified format.

    Simplification strategy (pure projection mode):
    1. key_in: take only src_dim slice (ignore tgt_dim portion)
    2. Merge mlp1 + proj_mlp2 + proj_out into single projection
    3. Ignore scalar path (use fixed gate)
    """
    def get_weight(name):
        if name in state_dict:
            return state_dict[name].float().numpy()
        return None

    # Input projection: [hidden, src+tgt] -> take [hidden, src] only
    key_in_w = get_weight("key_in.weight")
    key_in_b = get_weight("key_in.bias")
    value_in_w = get_weight("value_in.weight")
    value_in_b = get_weight("value_in.bias")

    # Slice src_dim portion
    key_in_w_src = key_in_w[:, :src_dim].T if key_in_w is not None else None     # [src, hidden]
    value_in_w_src = value_in_w[:, :src_dim].T if value_in_w is not None else None

    # MLP1: RMSNorm + FFN
    key_mlp1_norm = get_weight("key_mlp1.blocks.0.norm.weight")
    key_mlp1_w1 = get_weight("key_mlp1.blocks.0.w1.weight")     # [hidden, hidden]
    key_mlp1_w2 = get_weight("key_mlp1.blocks.0.w2.weight")
    value_mlp1_norm = get_weight("value_mlp1.blocks.0.norm.weight")
    value_mlp1_w1 = get_weight("value_mlp1.blocks.0.w1.weight")
    value_mlp1_w2 = get_weight("value_mlp1.blocks.0.w2.weight")

    # Projection output
    key_proj_out_w = get_weight("key_proj_out.weight")           # [tgt, hidden]
    key_proj_out_b = get_weight("key_proj_out.bias")
    value_proj_out_w = get_weight("value_proj_out.weight")
    value_proj_out_b = get_weight("value_proj_out.bias")

    # Gate scalars
    key_gate = get_weight("key_gate_logit")
    value_gate = get_weight("value_gate_logit")
    key_gate = float(key_gate) if key_gate is not None else 0.0
    value_gate = float(value_gate) if value_gate is not None else 0.0

    return {
        "key_in_weight": key_in_w_src,
        "key_in_bias": key_in_b,
        "key_mlp1_weight": key_mlp1_w1.T if key_mlp1_w1 is not None else None,     # [hidden, hidden]
        "key_mlp1_bias": np.zeros(hidden_dim, dtype=np.float32),                    # FFN has no bias
        "key_proj_weight": key_proj_out_w.T if key_proj_out_w is not None else None, # [hidden, tgt]
        "key_proj_bias": key_proj_out_b,
        "value_in_weight": value_in_w_src,
        "value_in_bias": value_in_b,
        "value_mlp1_weight": value_mlp1_w1.T if value_mlp1_w1 is not None else None,
        "value_mlp1_bias": np.zeros(hidden_dim, dtype=np.float32),
        "value_proj_weight": value_proj_out_w.T if value_proj_out_w is not None else None,
        "value_proj_bias": value_proj_out_b,
        "key_gate_logit": key_gate,
        "value_gate_logit": value_gate,
    }


def write_mooncake_format(weights_list: list, src_dim: int, tgt_dim: int, hidden_dim: int, output_path: str):
    """
    Write Mooncake binary format.

    Layout:
    [num_layers:i32][src_dim:i32][tgt_dim:i32][hidden_dim:i32]
    For each layer:
        [key_gate_logit:f32][value_gate_logit:f32]
        [key_in_weight: src*hidden][key_in_bias: hidden]
        [key_mlp1_weight: hidden*hidden][key_mlp1_bias: hidden]
        [key_proj_weight: hidden*tgt][key_proj_bias: tgt]
        [value_in_weight...][value_in_bias...]
        [value_mlp1_weight...][value_mlp1_bias...]
        [value_proj_weight...][value_proj_bias...]
    """
    num_layers = len(weights_list)

    with open(output_path, "wb") as f:
        # Header
        f.write(struct.pack("iiii", num_layers, src_dim, tgt_dim, hidden_dim))

        for layer_idx, w in enumerate(weights_list):
            # Gate scalars
            f.write(struct.pack("ff", w["key_gate_logit"], w["value_gate_logit"]))

            # Key weights
            f.write(w["key_in_weight"].astype(np.float32).tobytes())
            f.write(w["key_in_bias"].astype(np.float32).tobytes())
            f.write(w["key_mlp1_weight"].astype(np.float32).tobytes())
            f.write(w["key_mlp1_bias"].astype(np.float32).tobytes())
            f.write(w["key_proj_weight"].astype(np.float32).tobytes())
            f.write(w["key_proj_bias"].astype(np.float32).tobytes())

            # Value weights
            f.write(w["value_in_weight"].astype(np.float32).tobytes())
            f.write(w["value_in_bias"].astype(np.float32).tobytes())
            f.write(w["value_mlp1_weight"].astype(np.float32).tobytes())
            f.write(w["value_mlp1_bias"].astype(np.float32).tobytes())
            f.write(w["value_proj_weight"].astype(np.float32).tobytes())
            f.write(w["value_proj_bias"].astype(np.float32).tobytes())

    print(f"\nSaved {num_layers} layer projectors to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Convert C2C weights to Mooncake format")
    parser.add_argument("--checkpoint", type=str, default="nics-efc/C2C_Fuser",
                        help="HuggingFace checkpoint repo")
    parser.add_argument("--fuser", type=str, default="qwen3_0.6b+qwen3_4b_Fuser",
                        help="Fuser subdirectory name")
    parser.add_argument("--src-hf", type=str, required=True,
                        help="Source HuggingFace model (e.g. Qwen/Qwen3-4B)")
    parser.add_argument("--tgt-hf", type=str, required=True,
                        help="Target HuggingFace model (e.g. Qwen/Qwen3-0.6B)")
    parser.add_argument("--hidden-dim", type=int, default=None,
                        help="Projector hidden dim (auto-detect from weights if omitted)")
    parser.add_argument("--output", type=str, default="projector.bin",
                        help="Output file path")
    parser.add_argument("--local", type=str, default=None,
                        help="Local checkpoint directory")
    args = parser.parse_args()

    if not HAS_TORCH:
        print("Error: PyTorch is required")
        print("Install with: pip install torch")
        return 1

    # Fetch model dimensions from HuggingFace
    print(f"\nFetching model configs from HuggingFace...")
    src_cfg = fetch_hf_config(args.src_hf)
    tgt_cfg = fetch_hf_config(args.tgt_hf)

    src_dim = src_cfg["num_kv_heads"] * src_cfg["head_dim"]
    tgt_dim = tgt_cfg["num_kv_heads"] * tgt_cfg["head_dim"]

    print(f"\nSource: {args.src_hf}")
    print(f"  layers={src_cfg['num_layers']}, kv_heads={src_cfg['num_kv_heads']}, "
          f"head_dim={src_cfg['head_dim']}, kv_dim={src_dim}")
    print(f"Target: {args.tgt_hf}")
    print(f"  layers={tgt_cfg['num_layers']}, kv_heads={tgt_cfg['num_kv_heads']}, "
          f"head_dim={tgt_cfg['head_dim']}, kv_dim={tgt_dim}")

    # Locate checkpoint directory
    if args.local:
        checkpoint_dir = Path(args.local)
    else:
        from huggingface_hub import snapshot_download
        print(f"\nDownloading {args.checkpoint}/{args.fuser}...")
        checkpoint_dir = Path(snapshot_download(
            repo_id=args.checkpoint,
            allow_patterns=[f"{args.fuser}/*"]
        )) / args.fuser / "final"

    # Find all projector_*.pt files
    pt_files = sorted(checkpoint_dir.glob("projector_*.pt"),
                      key=lambda p: int(p.stem.split("_")[1]))

    if not pt_files:
        print(f"Error: No projector_*.pt files found in {checkpoint_dir}")
        return 1

    print(f"\nFound {len(pt_files)} projector files")

    # Infer hidden_dim from first weight file
    hidden_dim = args.hidden_dim
    if hidden_dim is None:
        probe = torch.load(pt_files[0], map_location="cpu", weights_only=True)
        key_in_w = probe.get("key_in.weight")
        if key_in_w is not None:
            hidden_dim = key_in_w.shape[0]
            print(f"  Auto-detected hidden_dim={hidden_dim} from weights")
        else:
            hidden_dim = 1024
            print(f"  Using default hidden_dim={hidden_dim}")

    print(f"Projector dims: src={src_dim}, tgt={tgt_dim}, hidden={hidden_dim}")

    # Convert per-layer weights
    weights_list = []
    for pt_file in pt_files:
        layer_idx = int(pt_file.stem.split("_")[1])
        print(f"  Loading layer {layer_idx}: {pt_file.name}")
        state_dict = torch.load(pt_file, map_location="cpu", weights_only=True)
        weights = convert_layer_projector(state_dict, src_dim, tgt_dim, hidden_dim)
        weights_list.append(weights)

    # Write Mooncake format
    write_mooncake_format(weights_list, src_dim, tgt_dim, hidden_dim, args.output)

    return 0


if __name__ == "__main__":
    exit(main())
