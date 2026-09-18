"""
vLLM-XPU disaggregated prefill/decode smoke over Mooncake Store.

Two vLLM instances on two different Intel XPUs (same node or two nodes) share
KV through Mooncake Store with vLLM's ``MooncakeStoreConnector``:

  prefill  (``kv_role=kv_producer``, embedded mode): prefills the prompt, the
           connector saves the KV blocks from its XPU KV cache into the store
           segment it contributes; it also greedily decodes the reference
           continuation. It then stays alive so its segment remains mounted.
  decode   (``kv_role=kv_consumer``, standalone-store mode, contributes no
           segment): same prompt, local prefix caching disabled; the connector
           finds the blocks in the store, pulls them from the prefill side's
           segment (over the Transfer Engine, TCP or RDMA) into *its* XPU KV
           cache and decodes from them.

PASS requires the decode side to load exactly the keys the prefill side saved,
with no failed keys, and to produce the prefill side's reference tokens.

Roles:
  --role local     (default) run both roles as subprocesses on this host,
                   prefill on ``--prefill-xpu`` and decode on ``--decode-xpu``
                   (Level Zero device indices) - the single-node 2-XPU case.
  --role prefill   run the prefill side only; writes ``--result`` when the KV
                   is in the store and holds until ``--done-file`` appears.
  --role decode    run the decode side only; waits for ``--reference`` (the
                   prefill side's ``--result``), then decodes and reports.
  For the two-node case run ``--role prefill`` on node A (with the master) and
  ``--role decode --master <A>:50051`` on node B, copying the result file over.

Requires the same environment as vllm_store_smoke.py (XPU vLLM, Mooncake
bindings built with -DUSE_XPU=ON) and reuses its vLLM shims.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import vllm_store_smoke as base  # noqa: E402  (sets VLLM_ENABLE_V1_MULTIPROCESSING=0, MC_USE_TENT=1)

torch = base.torch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--role", default="local", choices=["local", "prefill", "decode"]
    )
    parser.add_argument("--model", required=True, help="HF model id or local path")
    parser.add_argument(
        "--master", default="127.0.0.1:50051", help="mooncake_master address"
    )
    parser.add_argument(
        "--master-bin",
        default=None,
        help="Path to mooncake_master; started by the prefill side on --master's port",
    )
    parser.add_argument("--metadata-server", default="P2PHANDSHAKE")
    parser.add_argument("--protocol", default="tcp", choices=["tcp", "rdma"])
    parser.add_argument(
        "--device-name", default="", help="Transfer Engine device filter"
    )
    parser.add_argument(
        "--global-segment-size", default="2GB", help="prefill side segment"
    )
    parser.add_argument("--local-buffer-size", default="512MB")
    parser.add_argument("--max-model-len", type=int, default=2048)
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.5)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--prompt-repeats", type=int, default=12)
    parser.add_argument("--save-timeout", type=float, default=60.0)
    parser.add_argument(
        "--prefill-xpu", type=int, default=0, help="local: Level Zero index"
    )
    parser.add_argument(
        "--decode-xpu", type=int, default=1, help="local: Level Zero index"
    )
    parser.add_argument(
        "--result", default=None, help="prefill: JSON written when KV is saved"
    )
    parser.add_argument(
        "--reference", default=None, help="decode: prefill side's --result"
    )
    parser.add_argument(
        "--done-file", default=None, help="prefill exits once this exists"
    )
    parser.add_argument("--hold-timeout", type=float, default=900.0)
    parser.add_argument(
        "--wait-timeout", type=float, default=900.0, help="decode: --reference"
    )
    return parser.parse_args()


def write_config(args: argparse.Namespace, role: str) -> str:
    # The decode side contributes no segment, so every KV block lives on the
    # prefill side and each load is a real cross-device (or cross-node) pull.
    config = {
        "metadata_server": args.metadata_server,
        "master_server_address": args.master,
        "protocol": args.protocol,
        "device_name": args.device_name,
        "mode": "embedded" if role == "prefill" else "standalone-store",
        "global_segment_size": args.global_segment_size if role == "prefill" else 0,
        "local_buffer_size": args.local_buffer_size,
    }
    fd, path = tempfile.mkstemp(prefix=f"mooncake-xpu-pd-{role}-", suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(config, f)
    return path


def describe_xpu() -> str:
    props = torch.xpu.get_device_properties(0)
    selector = os.environ.get("ONEAPI_DEVICE_SELECTOR", "<unset>")
    return f"{props.name} (ONEAPI_DEVICE_SELECTOR={selector}, {torch.xpu.device_count()} visible)"


def build_llm(args: argparse.Namespace, kv_role: str):
    from vllm import LLM
    from vllm.config import KVTransferConfig

    return LLM(
        model=args.model,
        max_model_len=args.max_model_len,
        gpu_memory_utilization=args.gpu_memory_utilization,
        enable_prefix_caching=False,
        enforce_eager=True,
        disable_log_stats=False,
        kv_transfer_config=KVTransferConfig(
            kv_connector="MooncakeStoreConnector", kv_role=kv_role
        ),
    )


def prompt_for(args: argparse.Namespace) -> str:
    return (
        base.PROMPT_PARAGRAPH * args.prompt_repeats
        + "In one sentence, what is Mooncake?"
    )


def wait_for_file(path: str, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            return False
        time.sleep(0.5)
    return True


def install_shims() -> None:
    if not torch.cuda.is_available():
        torch.cuda.Event = torch.xpu.Event  # type: ignore[assignment,misc]
    base._patch_vllm_kv_address_math()


def run_prefill(args: argparse.Namespace) -> int:
    from vllm import SamplingParams

    install_shims()
    master_proc = None
    if args.master_bin:
        master_proc = base.start_master(args.master_bin, args.master)
    config_path = write_config(args, "prefill")
    os.environ["MOONCAKE_CONFIG_PATH"] = config_path
    llm = None
    try:
        print(f"[prefill] xpu : {describe_xpu()}", flush=True)
        llm = build_llm(args, "kv_producer")
        prompt = prompt_for(args)
        sampling = SamplingParams(temperature=0.0, max_tokens=args.max_tokens)
        out = llm.generate([prompt], sampling)[0].outputs[0]

        deadline = time.monotonic() + args.save_timeout
        while True:
            saved, save_failed = base.store_counter(
                llm.get_metrics(), "mooncake_store_operation", "save_put"
            )
            if saved > 0 or time.monotonic() > deadline:
                break
            time.sleep(0.5)

        result = {
            "prompt_tokens": len(llm.get_tokenizer().encode(prompt)),
            "tokens": list(out.token_ids),
            "saved": saved,
            "save_failed": save_failed,
        }
        print(f"[prefill] prompt tokens : {result['prompt_tokens']}", flush=True)
        print(f"[prefill] reference     : {result['tokens']}", flush=True)
        print(f"[prefill] save_put keys : {saved} ok, {save_failed} failed", flush=True)
        if args.result:
            tmp = args.result + ".tmp"
            with open(tmp, "w") as f:
                json.dump(result, f)
            os.replace(tmp, args.result)
        if saved == 0 or save_failed:
            print("FAIL: prefill side saved no KV blocks", file=sys.stderr)
            return 1

        # Keep the segment mounted for the decode side.
        if args.done_file:
            print(f"[prefill] holding until {args.done_file} exists", flush=True)
            if not wait_for_file(args.done_file, args.hold_timeout):
                print("[prefill] hold timed out", flush=True)
        return 0
    finally:
        if llm is not None:
            del llm
        os.unlink(config_path)
        if master_proc is not None:
            master_proc.terminate()
            master_proc.wait(timeout=10)


def run_decode(args: argparse.Namespace) -> int:
    from vllm import SamplingParams

    install_shims()
    config_path = write_config(args, "decode")
    os.environ["MOONCAKE_CONFIG_PATH"] = config_path
    llm = None
    try:
        print(f"[decode] xpu : {describe_xpu()}", flush=True)
        llm = build_llm(args, "kv_consumer")
        if not args.reference or not wait_for_file(args.reference, args.wait_timeout):
            print("FAIL: prefill reference did not appear", file=sys.stderr)
            return 1
        with open(args.reference) as f:
            reference = json.load(f)

        prompt = prompt_for(args)
        sampling = SamplingParams(temperature=0.0, max_tokens=args.max_tokens)
        out = llm.generate([prompt], sampling)[0].outputs[0]
        loaded, load_failed = base.store_counter(
            llm.get_metrics(), "mooncake_store_operation", "load_get"
        )
        print(f"[decode] prompt tokens : {len(llm.get_tokenizer().encode(prompt))}")
        print(f"[decode] output        : {list(out.token_ids)}")
        print(f"[decode] reference     : {reference['tokens']}")
        print(f"[decode] save_put keys : {reference['saved']} ok (prefill side)")
        print(f"[decode] load_get keys : {loaded} ok, {load_failed} failed")

        failures = []
        if not out.token_ids:
            failures.append("decode returned no tokens")
        if list(out.token_ids) != reference["tokens"]:
            failures.append("decode from loaded KV diverged from the prefill side")
        if loaded == 0:
            failures.append("decode side loaded no KV blocks")
        elif loaded != reference["saved"]:
            failures.append(
                f"loaded {loaded} keys but prefill saved {reference['saved']}"
            )
        if load_failed:
            failures.append("decode side reported failed keys")
        if failures:
            print("FAIL: " + "; ".join(failures), file=sys.stderr)
            return 1
        print("PASS: vLLM-XPU prefill (XPU A) -> Mooncake Store -> decode (XPU B)")
        return 0
    finally:
        if args.done_file:
            open(args.done_file, "w").close()
        if llm is not None:
            del llm
        os.unlink(config_path)


def run_local(args: argparse.Namespace) -> int:
    if torch.xpu.device_count() < 2:
        print(f"FAIL: need 2 XPUs, found {torch.xpu.device_count()}", file=sys.stderr)
        return 2
    workdir = tempfile.mkdtemp(prefix="mooncake-xpu-pd-")
    result = os.path.join(workdir, "prefill.json")
    done = os.path.join(workdir, "done")
    common = [sys.executable, os.path.abspath(__file__)]
    for name in (
        "model",
        "master",
        "master_bin",
        "metadata_server",
        "protocol",
        "device_name",
        "global_segment_size",
        "local_buffer_size",
        "max_model_len",
        "gpu_memory_utilization",
        "max_tokens",
        "prompt_repeats",
        "save_timeout",
        "hold_timeout",
        "wait_timeout",
    ):
        value = getattr(args, name)
        if value is not None:
            common += [f"--{name.replace('_', '-')}", str(value)]
    common += ["--result", result, "--reference", result, "--done-file", done]

    def spawn(role: str, xpu: int):
        # One Level Zero device per role; the master (if any) belongs to prefill.
        env = dict(os.environ, ONEAPI_DEVICE_SELECTOR=f"level_zero:{xpu}")
        log = open(os.path.join(workdir, f"{role}.log"), "w")
        proc = subprocess.Popen(
            [*common, "--role", role], env=env, stdout=log, stderr=subprocess.STDOUT
        )
        return proc, log

    prefill, plog = spawn("prefill", args.prefill_xpu)
    decode, dlog = spawn("decode", args.decode_xpu)
    try:
        decode_rc = decode.wait(timeout=args.wait_timeout + 600)
        open(done, "w").close()
        prefill_rc = prefill.wait(timeout=120)
    finally:
        for proc in (prefill, decode):
            if proc.poll() is None:
                proc.kill()
        plog.close()
        dlog.close()

    for role in ("prefill", "decode"):
        with open(os.path.join(workdir, f"{role}.log")) as f:
            for line in f:
                if line.startswith((f"[{role}]", "PASS", "FAIL", "Traceback")):
                    print(line.rstrip())
    print(f"logs: {workdir}")
    if prefill_rc != 0:
        print(f"FAIL: prefill side exited with {prefill_rc}", file=sys.stderr)
        return 1
    return decode_rc


def main() -> int:
    args = parse_args()
    if not (hasattr(torch, "xpu") and torch.xpu.is_available()):
        print("FAIL: torch.xpu is not available", file=sys.stderr)
        return 2
    if args.role == "local":
        return run_local(args)
    if args.role == "prefill":
        return run_prefill(args)
    return run_decode(args)


if __name__ == "__main__":
    sys.exit(main())
