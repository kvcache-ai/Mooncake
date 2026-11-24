"""
vLLM-XPU + MooncakeStoreConnector smoke test: prefill -> store -> decode.

Runs a single vLLM instance on an Intel XPU with vLLM's ``MooncakeStoreConnector``
(``kv_role=kv_both``) and *local prefix caching disabled*, so that:

  1. the first request prefills the prompt and the connector saves the KV blocks
     into Mooncake Store (XPU KV cache -> host staging -> store segment);
  2. the second request, with the same prompt, cannot hit vLLM's own prefix
     cache; the connector looks the blocks up in the store, loads them back
     (store -> host staging -> XPU KV cache) and decoding resumes from them.

The test passes when both requests return identical, non-empty greedy tokens
and the connector metrics show ``save_put`` and ``load_get`` keys with no
failures. It exercises, end to end, the XPU support in this repo: the Transfer
Engine's registration of PyTorch XPU tensors (``register_buffer`` on the KV
cache) and the store client's XPU host-staging copies (``batch_put_from`` /
``batch_get_into`` on device pointers).

Two small in-process shims work around vLLM (<= 0.29) connector code that is
CUDA-specific rather than Mooncake-specific (see the comments in ``main()`` and
``_patch_vllm_kv_address_math()``); drop them once vLLM is device-generic there.

Requires:
  - an Intel GPU with the Level Zero runtime (``ONEAPI_DEVICE_SELECTOR=level_zero:gpu``)
  - vLLM with XPU support (e.g. the ``vllm/vllm-openai-xpu`` image, >= 0.29)
  - the Mooncake wheel / ``mooncake.store`` bindings built with ``-DUSE_XPU=ON``
  - a running ``mooncake_master`` (or pass ``--master-bin`` to start one)

Usage:
  ONEAPI_DEVICE_SELECTOR=level_zero:gpu \\
  python scripts/xpu/vllm_store_smoke.py --model /models/Qwen2.5-0.5B-Instruct \\
      --master-bin build/mooncake-store/src/mooncake_master
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time

# The engine core and the worker must run in this process: the connector's
# save path (see the torch.cuda.Event note in main()) is shimmed here, and a
# spawned worker would not inherit the shim.
os.environ.setdefault("VLLM_ENABLE_V1_MULTIPROCESSING", "0")
# Intel XPU is only supported by the TENT engine; the classic Transfer Engine
# has no XPU platform and would hand device pointers to a plain TCP transport.
# The store client reads this when it constructs its Transfer Engine.
os.environ.setdefault("MC_USE_TENT", "1")

import torch  # noqa: E402

PROMPT_PARAGRAPH = (
    "Mooncake is a KVCache-centric disaggregated architecture for LLM serving. "
    "It separates prefill and decode clusters and pools the CPU, DRAM and SSD "
    "resources of the GPU cluster into a distributed KVCache store. "
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--model", required=True, help="HF model id or local path")
    parser.add_argument(
        "--master", default="127.0.0.1:50051", help="mooncake_master address"
    )
    parser.add_argument(
        "--master-bin",
        default=None,
        help="Path to mooncake_master; if given, a master is started on --master's port",
    )
    parser.add_argument("--metadata-server", default="P2PHANDSHAKE")
    parser.add_argument("--protocol", default="tcp", choices=["tcp", "rdma"])
    parser.add_argument(
        "--device-name", default="", help="Transfer Engine device filter"
    )
    parser.add_argument("--global-segment-size", default="2GB")
    parser.add_argument("--local-buffer-size", default="512MB")
    parser.add_argument("--max-model-len", type=int, default=2048)
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.5)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--prompt-repeats", type=int, default=12)
    parser.add_argument("--save-timeout", type=float, default=60.0)
    return parser.parse_args()


def start_master(master_bin: str, address: str) -> subprocess.Popen:
    port = address.rsplit(":", 1)[1]
    proc = subprocess.Popen(
        [master_bin, f"--port={port}", "--enable_http_metadata_server=false"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2.0)
    if proc.poll() is not None:
        raise RuntimeError(f"mooncake_master exited early with code {proc.returncode}")
    return proc


def write_mooncake_config(args: argparse.Namespace) -> str:
    config = {
        "metadata_server": args.metadata_server,
        "master_server_address": args.master,
        "protocol": args.protocol,
        "device_name": args.device_name,
        "global_segment_size": args.global_segment_size,
        "local_buffer_size": args.local_buffer_size,
    }
    fd, path = tempfile.mkstemp(prefix="mooncake-xpu-smoke-", suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(config, f)
    return path


def store_counter(metrics, name: str, operation: str) -> tuple[int, int]:
    """Return (ok keys, failed keys) of a Mooncake store connector counter.

    prometheus_client drops the ``_total`` suffix from counter family names,
    so ``vllm:<name>_keys_total`` is reported as ``vllm:<name>_keys``.
    """
    ok = failed = 0
    for metric in metrics:
        if metric.labels.get("operation") != operation:
            continue
        if metric.name == f"vllm:{name}_keys":
            ok += int(metric.value)
        elif metric.name == f"vllm:{name}_failed_keys":
            failed += int(metric.value)
    return ok, failed


def _patch_vllm_kv_address_math() -> None:
    """Make the connector's KV address arithmetic 64-bit unsigned.

    ``ChunkedTokenDatabase.prepare_values`` in vLLM (<= 0.29) vectorises
    ``base_addr + block_id * block_len`` with ``np.int64``. CUDA device
    pointers fit, but Level Zero maps XPU device USM into the upper canonical
    half (``0xffff8...``), so ``np.asarray(addrs, dtype=np.int64)`` raises
    ``OverflowError``. Mooncake's bindings take ``uintptr_t``, so computing in
    ``np.uint64`` is exact for both. Drop this once vLLM does the same.
    """
    import numpy as np
    from vllm.distributed.kv_transfer.kv_connector.v1.mooncake.store import data

    def prepare_values(self, chunks, block_ids):
        if not chunks:
            return [], [], []
        base = np.asarray(self.kv_caches_base_addr, dtype=np.uint64)
        length = len(self.block_len)
        blen = np.asarray(
            [self.block_len[i % length] for i in range(base.shape[0])],
            dtype=np.uint64,
        )
        n = len(chunks)
        starts = np.fromiter((c[0] for c in chunks), dtype=np.int64, count=n)
        spans = np.fromiter((c[1] for c in chunks), dtype=np.int64, count=n) - starts
        assert not (spans % self.hash_block_size).any()
        bids = np.fromiter(
            (block_ids[i] for i in (starts // self.block_size).tolist()),
            dtype=np.int64,
            count=n,
        )
        addrs = base[None, :] + bids.astype(np.uint64)[:, None] * blen[None, :]
        block_counts = (spans + self.block_size - 1) // self.block_size
        sizes = blen[None, :] * block_counts.astype(np.uint64)[:, None]
        return addrs.tolist(), sizes.tolist(), bids.tolist()

    data.ChunkedTokenDatabase.prepare_values = prepare_values


def main() -> int:
    args = parse_args()

    if not (hasattr(torch, "xpu") and torch.xpu.is_available()):
        print("FAIL: torch.xpu is not available", file=sys.stderr)
        return 2

    # vLLM's MooncakeStoreWorker.wait_for_save() records a torch.cuda.Event to
    # order the async save after the forward pass. On an XPU-only torch build
    # torch.cuda.Event is a dummy that raises on construction; torch.xpu.Event
    # has the same record()/synchronize() surface, so stand it in until vLLM
    # uses a device-generic event.
    if not torch.cuda.is_available():
        torch.cuda.Event = torch.xpu.Event  # type: ignore[assignment,misc]

    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    _patch_vllm_kv_address_math()

    master_proc = None
    if args.master_bin:
        master_proc = start_master(args.master_bin, args.master)

    config_path = write_mooncake_config(args)
    os.environ["MOONCAKE_CONFIG_PATH"] = config_path
    llm = None
    try:
        llm = LLM(
            model=args.model,
            max_model_len=args.max_model_len,
            gpu_memory_utilization=args.gpu_memory_utilization,
            enable_prefix_caching=False,
            enforce_eager=True,
            disable_log_stats=False,
            kv_transfer_config=KVTransferConfig(
                kv_connector="MooncakeStoreConnector",
                kv_role="kv_both",
            ),
        )
        sampling = SamplingParams(temperature=0.0, max_tokens=args.max_tokens)
        prompt = (
            PROMPT_PARAGRAPH * args.prompt_repeats
            + "In one sentence, what is Mooncake?"
        )

        # 1) prefill -> store (the save is asynchronous; wait for it to land).
        first = llm.generate([prompt], sampling)[0].outputs[0]
        deadline = time.monotonic() + args.save_timeout
        while True:
            saved, save_failed = store_counter(
                llm.get_metrics(), "mooncake_store_operation", "save_put"
            )
            if saved > 0 or time.monotonic() > deadline:
                break
            time.sleep(0.5)

        # 2) store -> decode: same prompt, no local prefix cache to hit.
        second = llm.generate([prompt], sampling)[0].outputs[0]
        loaded, load_failed = store_counter(
            llm.get_metrics(), "mooncake_store_operation", "load_get"
        )

        print(f"prompt tokens : {len(llm.get_tokenizer().encode(prompt))}")
        print(f"first  output : {first.token_ids}")
        print(f"second output : {second.token_ids}")
        print(f"save_put keys : {saved} ok, {save_failed} failed")
        print(f"load_get keys : {loaded} ok, {load_failed} failed")

        failures = []
        if not first.token_ids:
            failures.append("first request returned no tokens")
        if first.token_ids != second.token_ids:
            failures.append("decode after store load diverged from the first run")
        if saved == 0:
            failures.append("connector saved no KV blocks to the store")
        if loaded == 0:
            failures.append("connector loaded no KV blocks from the store")
        if save_failed or load_failed:
            failures.append("connector reported failed keys")
        if failures:
            names = sorted({m.name for m in llm.get_metrics() if "mooncake" in m.name})
            print(f"connector metrics seen: {names}", file=sys.stderr)
            print("FAIL: " + "; ".join(failures), file=sys.stderr)
            return 1
        print("PASS: vLLM-XPU prefill -> Mooncake Store -> decode")
        return 0
    finally:
        if llm is not None:
            del llm
        os.unlink(config_path)
        if master_proc is not None:
            master_proc.terminate()
            master_proc.wait(timeout=10)


if __name__ == "__main__":
    sys.exit(main())
