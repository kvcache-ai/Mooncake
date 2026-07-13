#!/usr/bin/env python3
"""Run the real Qwen-VL Mooncake EPD vLLM demo.

This is the public real-demo entry point.  It wraps the strict online direct
FeatureHandle E2E runner so the demo cannot accidentally fall back to the old
mock pipeline or asset-bytes multimodal path.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.scripts.check_real_epd_gate import validate_real_epd_summary
from mooncake_epd.scripts.run_vllm_online_direct_e2e import run as run_online_direct


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Run real strict Qwen-VL EPD demo: Encoder hidden-state -> Prefill -> Decode."
    )
    ap.add_argument("--workdir", default="/tmp/mooncake_epd_real_qwenvl_demo")
    ap.add_argument(
        "--model",
        default=os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct"),
    )
    ap.add_argument("--encoder-device", default="cuda:0")
    ap.add_argument("--encoder-dtype", default="bfloat16")
    ap.add_argument("--encoder-family", choices=["auto", "qwen3_vl", "qwen2_5_omni"], default="auto")
    ap.add_argument("--encoder-port", type=int, default=8330)
    ap.add_argument("--encoder-request-timeout-s", type=float, default=30.0)
    ap.add_argument("--prefill-gpu", type=int, default=1)
    ap.add_argument("--prefill-gpus", nargs="*", type=int, default=None)
    ap.add_argument("--prefill-ports", nargs="*", type=int, default=None)
    ap.add_argument("--decode-gpu", type=int, default=2)
    ap.add_argument("--decode-gpus", nargs="*", type=int, default=None)
    ap.add_argument("--decode-ports", nargs="*", type=int, default=None)
    ap.add_argument("--gpu-memory-utilization", type=float, default=0.65)
    ap.add_argument("--max-model-len", type=int, default=4096)
    ap.add_argument("--max-num-batched-tokens", type=int, default=0, help="0 keeps the vLLM default")
    ap.add_argument("--max-num-seqs", type=int, default=0, help="0 keeps the vLLM default")
    ap.add_argument("--local-hostname", default="127.0.0.1")
    ap.add_argument("--timeout", type=float, default=900.0)
    ap.add_argument("--request-timeout", type=float, default=300.0)
    ap.add_argument("--warmup-requests", type=int, default=1)
    ap.add_argument("--min-warmup-per-decode", type=int, default=2)
    ap.add_argument("--warmup-concurrency", type=int, default=0)
    ap.add_argument("--repeat-requests", type=int, default=8)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--between-repeat-sleep-s", type=float, default=0.0)
    ap.add_argument("--prompt", default="Describe the image briefly.")
    ap.add_argument("--workflow-id", default="real-qwenvl-epd-demo")
    ap.add_argument("--max-tokens", type=int, default=32)
    ap.add_argument("--temperature", type=float, default=0.0)
    ap.add_argument("--demo-image", default="room")
    ap.add_argument("--image-url", default=None)
    ap.add_argument("--layers-per-group", type=int, default=32)
    ap.add_argument("--max-group-bytes", type=int, default=64 * 1024 * 1024)
    ap.add_argument("--max-transfer-descriptors", type=int, default=512)
    ap.add_argument("--max-transfer-bytes", type=int, default=64 * 1024 * 1024)
    ap.add_argument(
        "--mooncake-protocol",
        choices=["tcp", "shm", "rdma", "nvlink_intra"],
        default="tcp",
        help="P-D KV transport; nvlink_intra requires USE_INTRA_NVLINK=ON.",
    )
    ap.add_argument("--direct-source-mode", choices=["registered_tensor", "managed_buffer"], default="registered_tensor")
    ap.add_argument("--direct-target-mode", choices=["registered_tensor", "managed_buffer", "auto"], default="registered_tensor")
    ap.add_argument("--direct-register-memory", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--direct-persistent-cache", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--direct-proxy-handle-cache", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--direct-proxy-handle-cache-max-entries", type=int, default=4096)
    ap.add_argument("--direct-proxy-handle-cache-ttl-s", type=float, default=600.0)
    ap.add_argument("--direct-cache-max-entries", type=int, default=64)
    ap.add_argument("--direct-cache-max-bytes", type=int, default=2 * 1024 * 1024 * 1024)
    ap.add_argument("--prefill-dispatch-mode", choices=["render_generate", "openai_prompt_only"], default="render_generate")
    ap.add_argument(
        "--allow-unverified-openai-prompt-only",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Only for a version/workload pair with recorded output-equivalence evidence.",
    )
    ap.add_argument("--owner-shards", type=int, default=1)
    ap.add_argument("--kv-directory-rpc-url", default=None)
    ap.add_argument(
        "--strict-no-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Real demo requires strict no-fallback; --no-strict-no-fallback is rejected.",
    )
    return ap.parse_args()


def run(args: argparse.Namespace) -> Dict[str, Any]:
    if not bool(args.strict_no_fallback):
        raise ValueError("real Qwen-VL EPD demo requires --strict-no-fallback")
    summary = run_online_direct(args)
    summary["demo_kind"] = "real_qwenvl_epd_direct"
    summary["uses_mock"] = False
    summary["strict_no_fallback"] = True
    summary["claim_scope"] = {
        "epd_three_stage": "real vLLM services with online E-stage FeatureHandle direct transfer and P→D MooncakeConnector",
        "agent_state_cloning": "not exercised by this demo",
        "throughput_improvement": "not claimed by this demo; use benchmark matrix",
    }
    summary["real_epd_metric_summary"] = dict(summary.get("online_direct_metric_summary") or {})
    validate_real_epd_summary(summary)

    workdir = Path(args.workdir).expanduser()
    out = workdir / "real_qwenvl_epd_demo_summary.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    legacy = workdir / "online_direct_e2e_summary.json"
    if legacy.exists() and legacy != out:
        # Keep both filenames: old automation reads the legacy name; reviewers get
        # the real-demo name that states the claim scope explicitly.
        shutil.copyfile(out, legacy)
    return summary


def main() -> None:
    args = parse_args()
    summary = run(args)
    print(
        json.dumps(
            {
                "summary": str(Path(args.workdir).expanduser() / "real_qwenvl_epd_demo_summary.json"),
                "response": summary.get("response", {}),
                "real_epd_metric_summary": summary.get("real_epd_metric_summary", {}),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
