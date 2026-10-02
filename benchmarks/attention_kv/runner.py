# Command line entry point: --dry-run prints the whole load matrix, a real run
# waits for the server to become healthy before measuring.

import argparse
import json
import os
import signal
import subprocess
import sys
import time

from . import config as config_module
from . import manifest as manifest_module
from . import report as report_module
from .config import build_plan_from_args, load_model_kv_config
from .e2e import (
    MooncakeStoreService,
    SGLangServer,
    find_free_port,
    resolve_mooncake_binary,
    run_e2e_case,
)

SUPPORTED_BACKENDS = ("sglang",)
DEFAULT_RESULT_ROOT = "artifacts/attention-kv"


class CleanupRegistry:
    """Track child process handles so they can all be reaped on exit or signal."""

    def __init__(self):
        self.handles = []
        self.installed = False

    def add(self, handle):
        self.handles.append(handle)
        return handle

    def release(self, handle):
        if handle in self.handles:
            self.handles.remove(handle)

    def cleanup(self):
        for handle in reversed(self.handles):
            try:
                handle.stop()
            except Exception as exc:  # noqa: BLE001 - record and keep cleaning up
                print(f"[cleanup] failed to reap a child: {exc}", file=sys.stderr)
        self.handles.clear()

    def install_signal_handlers(self):
        if self.installed:
            return
        for signal_name in ("SIGINT", "SIGTERM"):
            signal.signal(getattr(signal, signal_name), self._handler)
        self.installed = True

    def _handler(self, signum, frame):
        print(f"\nsignal {signum} received, reaping children", file=sys.stderr)
        self.cleanup()
        sys.exit(130)


def build_parser():
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.attention_kv",
        description=(
            "Attention -> KV cache access benchmark: KV write and paged read "
            "cost in the kernel phase, and the end-to-end benefit of HiCache "
            "and Mooncake cache reuse"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m benchmarks.attention_kv --full --dry-run\n"
            "  python -m benchmarks.attention_kv --quick --model /mnt/afs/models/Qwen3-8B "
            "--tp-size 8 --page-size 64 --result-dir artifacts/attention-kv/quick\n"
        ),
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--quick", action="store_true", help="smallest workable load, for a smoke run"
    )
    mode.add_argument("--full", action="store_true", help="the full load matrix")

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the whole load matrix only; starts nothing and takes no GPU memory",
    )
    parser.add_argument(
        "--model",
        required=False,
        default=None,
        help="path to the model directory, for example /mnt/afs/models/Qwen3-8B",
    )
    parser.add_argument(
        "--backend",
        default="sglang",
        choices=SUPPORTED_BACKENDS,
        help="inference framework used in the end-to-end phase",
    )
    parser.add_argument(
        "--tp-size",
        type=int,
        default=None,
        help="tensor parallel size; empty means take the visible GPU count",
    )
    parser.add_argument(
        "--page-size", type=int, default=64, help="tokens per page in paged attention"
    )
    parser.add_argument(
        "--input-lens", type=int, nargs="+", default=None, help="override input lengths"
    )
    parser.add_argument(
        "--output-len", type=int, default=None, help="override the output length"
    )
    parser.add_argument(
        "--requests", type=int, default=None, help="requests per load point"
    )
    parser.add_argument(
        "--concurrency", type=int, nargs="+", default=None, help="override concurrency"
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=None,
        help="repeats per load point; formal results need at least 3",
    )
    parser.add_argument("--rounds", type=int, default=None, help="multi-turn rounds")
    parser.add_argument("--seed", type=int, default=42, help="random seed")
    parser.add_argument(
        "--tiers",
        nargs="+",
        default=None,
        choices=list(config_module.CACHE_TIERS),
        help="cache tiers to measure",
    )
    parser.add_argument(
        "--patterns",
        nargs="+",
        default=None,
        choices=list(config_module.HIT_PATTERNS),
        help="hit patterns to measure",
    )
    parser.add_argument(
        "--skip-kernel", action="store_true", help="skip the kernel phase"
    )
    parser.add_argument(
        "--skip-e2e", action="store_true", help="skip the end-to-end phase"
    )
    parser.add_argument(
        "--result-dir",
        default=None,
        help=f"result directory, defaults to {DEFAULT_RESULT_ROOT}/<timestamp>",
    )
    parser.add_argument("--port", type=int, default=None, help="SGLang server port")
    parser.add_argument(
        "--mem-fraction-static",
        type=float,
        default=0.60,
        help="SGLang static memory fraction; lower it on a memory-constrained host",
    )
    parser.add_argument(
        "--context-length", type=int, default=None, help="server max context length"
    )
    parser.add_argument(
        "--attention-backend",
        default=None,
        help="SGLang attention backend; empty lets SGLang choose",
    )
    parser.add_argument(
        "--hicache-ratio",
        type=int,
        default=2,
        help="HiCache host pool size relative to the device pool",
    )
    parser.add_argument(
        "--hicache-size",
        type=int,
        default=0,
        help="HiCache host pool size in GB; 0 derives it from the ratio",
    )
    parser.add_argument(
        "--prefetch-policy",
        default="wait_complete",
        choices=("best_effort", "wait_complete", "timeout"),
        help="HiCache L3 prefetch termination policy",
    )
    parser.add_argument(
        "--write-policy",
        default="write_through",
        choices=("write_through", "write_through_selective", "write_back"),
        help="HiCache write-back policy",
    )
    parser.add_argument(
        "--mooncake-master", default=None, help="path to the mooncake_master binary"
    )
    parser.add_argument(
        "--store-port", type=int, default=50051, help="local mooncake master RPC port"
    )
    parser.add_argument(
        "--store-metadata-port",
        type=int,
        default=8080,
        help="mooncake master HTTP metadata port, used by the Transfer Engine",
    )
    parser.add_argument(
        "--store-segment-bytes",
        type=int,
        default=8 * 1024**3,
        help="Mooncake global segment size",
    )
    parser.add_argument(
        "--device", default="cuda:0", help="device used by the kernel phase"
    )
    parser.add_argument(
        "--kernel-worker",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--cuda-home",
        default=None,
        help="CUDA toolkit directory providing nvcc; sgl_kernel JIT needs nvcc >= 12.9",
    )
    parser.add_argument(
        "--mooncake-python-path",
        default=None,
        help="directory holding the mooncake package, added to the server PYTHONPATH",
    )
    parser.add_argument(
        "--mooncake-lib-path",
        default=None,
        help="mooncake shared library directory, added to the server LD_LIBRARY_PATH",
    )
    parser.add_argument(
        "--store-protocol",
        default="tcp",
        choices=("tcp", "rdma"),
        help="Mooncake Transfer Engine protocol; use tcp for a local loopback path",
    )
    parser.add_argument(
        "--chunked-prefill-size",
        type=int,
        default=None,
        help="SGLang chunked prefill size",
    )
    parser.add_argument(
        "--max-prefill-tokens",
        type=int,
        default=None,
        help="SGLang maximum tokens per prefill",
    )
    parser.add_argument(
        "--hicache-mem-layout",
        default=None,
        choices=(
            "layer_first",
            "page_first",
            "page_first_direct",
            "page_first_kv_split",
        ),
        help="HiCache host pool memory layout",
    )
    parser.add_argument(
        "--hicache-io-backend",
        default=None,
        choices=("direct", "kernel"),
        help="HiCache host to device transfer backend",
    )
    parser.add_argument(
        "--kernel-warmup",
        type=int,
        default=10,
        help="kernel phase warmup iterations, at least 10",
    )
    parser.add_argument(
        "--kernel-timed",
        type=int,
        default=100,
        help="kernel phase timed iterations, at least 100",
    )
    parser.add_argument("--repo-dir", default=None, help="Mooncake repository root")
    parser.add_argument(
        "--git-commit",
        default=None,
        help="commit under test, supplied when the tested directory has no full .git",
    )
    parser.add_argument("--git-subject", default=None, help="commit subject under test")
    parser.add_argument("--git-branch", default=None, help="branch under test")
    return parser


def git_override_from_args(args):
    if not args.git_commit:
        return None
    return {
        "commit": args.git_commit,
        "subject": args.git_subject,
        "branch": args.git_branch,
        "dirty": None,
    }


def default_result_dir():
    stamp = time.strftime("%Y%m%d-%H%M%S")
    return os.path.join(DEFAULT_RESULT_ROOT, stamp)


MAX_BATCHED_SEQ_LEN = 8192


def kernel_cases_from_plan(plan, model_config):
    """Build the kernel measurement points.

    Prefill attention work grows with the square of the sequence length, so long
    sequences are only measured at batch 1; anything else would make a single
    point too slow and too large. Decode computes one token, so every length is
    measured at batch 1 and 4.
    """
    from .kernel_bench import build_kernel_cases

    cases = []
    for mode in ("prefill", "decode"):
        for batch_size in (1,) if plan.quick else (1, 4):
            if batch_size == 1:
                lengths = list(plan.input_lens)
            elif mode == "prefill":
                lengths = [v for v in plan.input_lens if v <= MAX_BATCHED_SEQ_LEN]
            else:
                lengths = list(plan.input_lens)
            if not lengths:
                continue
            cases.extend(
                build_kernel_cases(
                    model_config,
                    tp_size=plan.tp_size,
                    page_size=plan.page_size,
                    input_lens=lengths,
                    batch_sizes=(batch_size,),
                    modes=(mode,),
                )
            )
    return cases


def run_kernel_phase(plan, model_config, jsonl_path, device, warmup, timed, errors):
    from .kernel_bench import run_case

    cases = kernel_cases_from_plan(plan, model_config)
    print(f"[kernel] {len(cases)} measurement points")
    results = []
    with open(jsonl_path, "a", encoding="utf-8") as handle:
        for case in cases:
            print(
                f"[kernel] {case.label} kv_cache={case.kv_cache_bytes / 2**20:.1f} MiB"
            )
            record = run_case(case, device, warmup=warmup, timed=timed, verify=True)
            handle.write(json.dumps(record, default=str) + "\n")
            handle.flush()
            results.append(record)

            scatter = record["correctness"]["scatter"]
            attention = record["correctness"]["attention"]
            if isinstance(scatter, dict) and not scatter.get("passed"):
                errors.append(f"kernel {case.label} scatter check failed")
            if isinstance(attention, dict) and not attention.get("passed"):
                errors.append(f"kernel {case.label} attention reference check failed")
    return results


def build_runtime_env(args, config, store):
    """Build the server process environment. cuda-home provides nvcc for
    sgl_kernel's JIT; the mooncake package and library directories are only
    needed when L3 is enabled."""
    env = {}
    path_parts = []
    library_parts = []
    python_parts = []

    if args.cuda_home:
        path_parts.append(os.path.join(args.cuda_home, "bin"))
        env["CUDA_HOME"] = args.cuda_home
        library_parts.append(os.path.join(args.cuda_home, "lib"))
    if args.mooncake_lib_path:
        library_parts.append(args.mooncake_lib_path)
    if args.mooncake_python_path:
        python_parts.append(args.mooncake_python_path)

    if path_parts:
        env["PATH"] = ":".join(path_parts + [os.environ.get("PATH", "")])
    if library_parts:
        existing = os.environ.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = ":".join(
            library_parts + ([existing] if existing else [])
        )
    if python_parts:
        existing = os.environ.get("PYTHONPATH", "")
        env["PYTHONPATH"] = ":".join(python_parts + ([existing] if existing else []))
    env["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

    if config.cache_tier == "mooncake":
        env.update(
            {
                "MOONCAKE_TE_META_DATA_SERVER": store.metadata_server,
                "MOONCAKE_GLOBAL_SEGMENT_SIZE": str(args.store_segment_bytes),
                "MOONCAKE_PROTOCOL": args.store_protocol,
                "MOONCAKE_MASTER": f"127.0.0.1:{args.store_port}",
                "MOONCAKE_LOCAL_HOSTNAME": "127.0.0.1",
            }
        )
    return env


def build_server_spec(plan, config, args, port, log_name, store):
    spec = {
        "python": sys.executable,
        "model_path": plan.model_path,
        "tp_size": plan.tp_size,
        "page_size": plan.page_size,
        "port": port,
        "cache_tier": config.cache_tier,
        "mem_fraction_static": args.mem_fraction_static,
        "context_length": args.context_length,
        "attention_backend": args.attention_backend,
        "hicache_ratio": args.hicache_ratio,
        "hicache_size": args.hicache_size,
        "prefetch_policy": args.prefetch_policy,
        "write_policy": args.write_policy,
        "chunked_prefill_size": args.chunked_prefill_size,
        "max_prefill_tokens": args.max_prefill_tokens,
        "hicache_mem_layout": args.hicache_mem_layout,
        "hicache_io_backend": args.hicache_io_backend,
        "enable_metrics": True,
        "env": build_runtime_env(args, config, store),
        "log_name": log_name,
    }
    return spec


def tier_limits(effective):
    """The pool sizes of one cache tier, to check whether a tier could have served
    a reuse.

    L1 is the KV pool the server reports, in tokens per rank. L2 is the HiCache
    host pool, which SGLang sizes as a multiple of the device pool and reports
    back as hicache_ratio; when the pool was set by an absolute size instead, the
    ratio is not reported and L2 stays unknown. L3 has no size this benchmark can
    read, so only the backend name is recorded.
    """
    l1_pool_tokens = effective.get("max_total_num_tokens")
    if not effective.get("enable_hierarchical_cache"):
        return {
            "l1_pool_tokens": l1_pool_tokens,
            "l2_pool_tokens": 0,
            "storage_backend": None,
        }
    ratio = effective.get("hicache_ratio")
    l2_pool_tokens = int(l1_pool_tokens * ratio) if l1_pool_tokens and ratio else None
    return {
        "l1_pool_tokens": l1_pool_tokens,
        "l2_pool_tokens": l2_pool_tokens,
        "storage_backend": effective.get("hicache_storage_backend"),
    }


def run_e2e_phase(
    plan, model_config, model_meta, args, jsonl_path, work_dir, cleanup, errors
):
    ports = {}
    records = []
    # Server configuration that actually took effect, per cache tier, for the manifest
    effective = {}

    for tier in plan.tiers:
        store = None
        if tier in ("mooncake",):
            binary = resolve_mooncake_binary(args.mooncake_master)
            store = MooncakeStoreService(
                binary,
                work_dir,
                port=args.store_port,
                metadata_port=args.store_metadata_port,
                log_name=f"mooncake_master_{tier}.log",
            )
            # Register before starting, so a failure during startup is still reaped
            cleanup.add(store)
            store.start()
            print(
                f"[e2e] mooncake master started: {binary} "
                f"RPC port {args.store_port}, HTTP metadata port "
                f"{args.store_metadata_port}"
            )

        tier_configs = [c for c in plan.e2e_configs if c.cache_tier == tier]
        port = args.port or find_free_port()
        ports[tier] = port
        spec = build_server_spec(
            plan, tier_configs[0], args, port, f"sglang_{tier}.log", store
        )
        with SGLangServer(spec, work_dir) as server:
            print(f"[e2e] tier {tier} server is up: {server.base_url}")
            info = server.server_info()
            effective[tier] = {
                "enable_hierarchical_cache": info.get("enable_hierarchical_cache"),
                "attention_backend": info.get("attention_backend"),
                "decode_attention_backend": info.get("decode_attention_backend"),
                "prefill_attention_backend": info.get("prefill_attention_backend"),
                "page_size": info.get("page_size"),
                "max_total_num_tokens": info.get("max_total_num_tokens"),
                "chunked_prefill_size": info.get("chunked_prefill_size"),
                "max_prefill_tokens": info.get("max_prefill_tokens"),
                "hicache_mem_layout": info.get("hicache_mem_layout"),
                "hicache_io_backend": info.get("hicache_io_backend"),
                "hicache_storage_backend": info.get("hicache_storage_backend"),
                "hicache_storage_prefetch_policy": info.get(
                    "hicache_storage_prefetch_policy"
                ),
                "hicache_write_policy": info.get("hicache_write_policy"),
                "hicache_ratio": info.get("hicache_ratio"),
                "server_info_available": bool(info),
            }
            tier_meta = tier_limits(effective[tier])
            print(
                f"[e2e] tier {tier} pools: L1 {tier_meta['l1_pool_tokens']} tokens, "
                f"L2 {tier_meta['l2_pool_tokens']} tokens, "
                f"storage backend {tier_meta['storage_backend']}"
            )
            for config in tier_configs:
                for case in plan.cases:
                    for repeat_index in range(config.repeats):
                        run_id = f"{config.config_id}-{case.case_id}-rep{repeat_index}"
                        print(f"[e2e] {run_id}")
                        produced = run_e2e_case(
                            server,
                            config,
                            case,
                            run_id,
                            repeat_index,
                            model_meta["vocab_size"],
                            kv_meta=model_meta,
                            tier_meta=tier_meta,
                        )
                        records.extend(produced)
                        with open(jsonl_path, "a", encoding="utf-8") as handle:
                            for record in produced:
                                handle.write(json.dumps(record, default=str) + "\n")
                    # Flush the local cache after every load point so nothing leaks
                    # into the next one. A failed flush would make the following
                    # cold miss and eviction results incomparable, so it is fatal.
                    server.flush_cache()
        cleanup.release(store)
        if store is not None:
            store.stop()

    return records, effective


def _run_kernel_in_subprocess(argv, errors):
    """Run the kernel phase in a child process.

    The kernel phase leaves a CUDA context holding memory on GPU 0. When the
    end-to-end phase then starts a TP server, unequal free memory across ranks
    fails SGLang's memory balance check. The child releases it on exit.
    """
    command = [
        sys.executable,
        "-m",
        "benchmarks.attention_kv",
        *(argv if argv is not None else sys.argv[1:]),
        "--kernel-worker",
        "--skip-e2e",
    ]
    print(f"[kernel] running in a separate process: {' '.join(command)}")
    completed = subprocess.run(command, check=False)
    if completed.returncode != 0:
        errors.append(f"kernel phase child exited with {completed.returncode}")


def detect_tp_size(explicit):
    """Resolve the tensor parallel size, defaulting to the visible GPU count.

    Hardcoding a count makes a machine with fewer GPUs fail only when the server
    starts, which puts the error a long way from its cause.
    """
    if explicit:
        return explicit
    count = manifest_module.visible_gpu_count()
    if count < 1:
        raise RuntimeError(
            "no visible GPU, cannot determine the tensor parallel size; "
            "check the driver and CUDA_VISIBLE_DEVICES"
        )
    print(f"[setup] --tp-size not given, using the {count} visible GPUs as TP={count}")
    return count


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.dry_run and not args.model:
        parser.error("a real run needs --model; --dry-run accepts a placeholder")

    if not args.dry_run:
        args.tp_size = detect_tp_size(args.tp_size)

    plan = build_plan_from_args(args)

    if args.dry_run:
        print(plan.describe())
        print()
        print(
            "The above is dry-run output; nothing was started and no GPU memory taken."
        )
        return 0

    repo_dir = args.repo_dir or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )
    result_dir = args.result_dir or default_result_dir()
    work_dir = os.path.join(result_dir, "logs")
    os.makedirs(work_dir, exist_ok=True)

    model_config = load_model_kv_config(args.model)
    if model_config.is_mla:
        raise SystemExit("this benchmark only supports standard GQA, found MLA")

    aggregate, per_rank = model_config.kv_bytes_per_token(plan.tp_size)
    per_page, per_page_rank = model_config.kv_bytes_per_page(
        plan.page_size, plan.tp_size
    )
    model_meta = {
        "vocab_size": model_config.vocab_size,
        "kv_bytes_per_token_aggregate": aggregate,
        "kv_bytes_per_token_per_rank": per_rank,
    }
    print(
        f"[setup] model {model_config.model_type} "
        f"{model_config.num_layers} layers "
        f"{model_config.num_key_value_heads} KV heads "
        f"head_dim {model_config.head_dim} dtype {model_config.torch_dtype}"
    )
    print(
        f"[setup] KV bytes per token: {aggregate} aggregate, "
        f"{per_rank} per rank (TP={plan.tp_size}); {per_page} per page aggregate"
    )

    with open(os.path.join(result_dir, "plan.json"), "w", encoding="utf-8") as handle:
        json.dump(plan.as_dict(), handle, indent=2)
        handle.write("\n")

    cleanup = CleanupRegistry()
    cleanup.install_signal_handlers()
    errors = []
    effective_server_config = {}

    if args.kernel_worker:
        # Separate process entry point: kernel phase only. The kernel phase leaves
        # a CUDA context in this process, while the end-to-end phase needs equal
        # free memory on every rank, so the two cannot share one process.
        if args.kernel_warmup < 10:
            errors.append("kernel warmup iterations below 10")
        if args.kernel_timed < 100:
            errors.append("kernel timed iterations below 100")
        try:
            run_kernel_phase(
                plan,
                model_config,
                os.path.join(result_dir, "kernel.jsonl"),
                args.device,
                args.kernel_warmup,
                args.kernel_timed,
                errors,
            )
        finally:
            cleanup.cleanup()
        for message in errors:
            print(f"[kernel] failed: {message}", file=sys.stderr)
        return 1 if errors else 0

    try:
        if plan.kernel:
            if plan.e2e:
                _run_kernel_in_subprocess(argv, errors)
            else:
                if args.kernel_warmup < 10:
                    errors.append("kernel warmup iterations below 10")
                if args.kernel_timed < 100:
                    errors.append("kernel timed iterations below 100")
                run_kernel_phase(
                    plan,
                    model_config,
                    os.path.join(result_dir, "kernel.jsonl"),
                    args.device,
                    args.kernel_warmup,
                    args.kernel_timed,
                    errors,
                )
        if plan.e2e:
            _, effective_server_config = run_e2e_phase(
                plan,
                model_config,
                model_meta,
                args,
                os.path.join(result_dir, "e2e.jsonl"),
                work_dir,
                cleanup,
                errors,
            )
    finally:
        cleanup.cleanup()

    manifest = manifest_module.build_manifest(
        repo_dir,
        args.model,
        model_config,
        plan,
        " ".join(sys.argv),
        extra={
            "result_dir": os.path.abspath(result_dir),
            "kernel_warmup": args.kernel_warmup,
            "kernel_timed": args.kernel_timed,
            "planned_failures": errors,
            "effective_server_config": effective_server_config,
        },
        git_override=git_override_from_args(args),
    )
    manifest_module.write_json(os.path.join(result_dir, "manifest.json"), manifest)
    _finalize(result_dir, manifest)

    if errors:
        print("this run has failures:", file=sys.stderr)
        for message in errors:
            print(f"  - {message}", file=sys.stderr)
        return 1
    print(f"[done] results in {result_dir}")
    return 0


def _finalize(result_dir, manifest):
    """Aggregate the JSONL into CSV and a report."""
    kernel_records = []
    e2e_records = []
    kernel_path = os.path.join(result_dir, "kernel.jsonl")
    e2e_path = os.path.join(result_dir, "e2e.jsonl")
    if os.path.isfile(kernel_path):
        kernel_records = report_module.load_records(kernel_path)
    if os.path.isfile(e2e_path):
        e2e_records = report_module.load_records(e2e_path)

    kernel_rows = report_module.kernel_summary(kernel_records)
    page_size = (manifest.get("model") or {}).get("page_size") or 64
    e2e_rows = report_module.aggregate_e2e(e2e_records, page_size)
    warnings = report_module.check_repeat_stability(e2e_rows)

    summary = {
        "manifest": manifest,
        "kernel": kernel_rows,
        "e2e": e2e_rows,
        "repeat_stability_warnings": warnings,
    }
    manifest_module.write_json(os.path.join(result_dir, "summary.json"), summary)
    report_module.write_csv(
        os.path.join(result_dir, "e2e_summary.csv"),
        e2e_rows,
        columns=report_module.CSV_COLUMNS,
    )
    report_module.write_csv(os.path.join(result_dir, "kernel_summary.csv"), kernel_rows)
    notes = report_module.collect_unavailable_notes(e2e_rows)
    markdown = report_module.build_markdown_summary(
        e2e_rows, kernel_rows, manifest, warnings, notes
    )
    with open(os.path.join(result_dir, "report.md"), "w", encoding="utf-8") as handle:
        handle.write(markdown)
        handle.write("\n")


if __name__ == "__main__":
    sys.exit(main())
