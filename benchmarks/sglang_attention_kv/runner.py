import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
import sys
import time

from . import kernel_summary as summary_module
from .cases import (
    BRANCH_CHOICES,
    DEFAULT_KV_SOURCE_PROTOCOL,
    DEFAULT_TIMED,
    DEFAULT_WARMUP,
    KV_SOURCES,
    KV_SOURCE_PROTOCOLS,
    LAYOUTS,
    MINIMUM_TIMED,
    MINIMUM_WARMUP,
    MODES,
    build_plan_from_args,
    resolve_extend_branch,
    use_paged_default,
)
from .model_config import load_model_kv_config

DEFAULT_RESULT_ROOT = "artifacts/sglang-attention-kv"


def build_parser():
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.sglang_attention_kv",
        description=(
            "Measure what one attention step spends on the paged KV cache. The step "
            "is three timed windows — the index mapping, the backend plan and the "
            "per-layer loop of attention and KV write — with the step measured as a "
            "whole beside them, and the KV write and the attention read priced "
            "separately in passes of their own. Runs over the history lengths, query "
            "lengths, batch shapes, page sizes and page layouts a deployment runs "
            "with."
        ),
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="minimal step matrix, to verify connectivity; exactly one of --quick or --full",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="full step matrix; exactly one of --quick or --full",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the step matrix and exit; touches no GPU",
    )
    parser.add_argument(
        "--model", default=None, help="model directory, must contain config.json"
    )
    parser.add_argument(
        "--tp-size",
        type=int,
        default=None,
        help=(
            "tensor parallel size; omitted means take the visible GPU count, which "
            "--dry-run prints as auto (visible GPU count)"
        ),
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        default=None,
        choices=list(MODES),
        help=(
            "steps to measure: prefill computes a whole sequence, extend computes a "
            "chunk behind a cached prefix, decode computes one token"
        ),
    )
    parser.add_argument(
        "--input-lens",
        type=int,
        nargs="+",
        default=None,
        help=(
            "history lengths: a prefill step computes this many tokens, an extend "
            "or decode step caches this many"
        ),
    )
    parser.add_argument(
        "--chunk-lens",
        type=int,
        nargs="+",
        default=None,
        help=(
            "query lengths of an extend step, one step per entry, over the history "
            "lengths of --input-lens"
        ),
    )
    parser.add_argument(
        "--batch-sizes",
        type=int,
        nargs="+",
        default=None,
        help="sequences per step; a batch above one is measured with equal lengths and with ragged lengths",
    )
    parser.add_argument(
        "--page-sizes",
        type=int,
        nargs="+",
        default=None,
        help="tokens per page of the paged KV cache",
    )
    parser.add_argument(
        "--layouts",
        nargs="+",
        default=None,
        choices=list(LAYOUTS),
        help="physical page order: contiguous, or pages drawn at random",
    )
    parser.add_argument(
        "--extend-branch",
        default=None,
        choices=list(BRANCH_CHOICES),
        help=(
            "the FlashInferAttnBackend lane for prefill and extend: "
            "ragged_prefix_merge is what a server runs while "
            "SGLANG_FLASHINFER_USE_PAGED is False (the default), paged_extend is "
            "the single paged call it runs with that variable set, including "
            "prefill without a cached history"
        ),
    )
    parser.add_argument(
        "--kv-sources",
        nargs="+",
        default=None,
        choices=list(KV_SOURCES),
        help=(
            "where a step's cached history is held: gpu reads it from the pool, "
            "host stages it in host memory and moves it in through Mooncake's "
            "Transfer Engine before the step"
        ),
    )
    parser.add_argument(
        "--kv-source-protocol",
        default=DEFAULT_KV_SOURCE_PROTOCOL,
        choices=list(KV_SOURCE_PROTOCOLS),
        help=(
            "transport Mooncake moves the history over when --kv-sources includes "
            f"host, {DEFAULT_KV_SOURCE_PROTOCOL} unless given"
        ),
    )
    parser.add_argument("--seed", type=int, default=42, help="random seed")
    parser.add_argument(
        "--device",
        default="cuda:0",
        help="device the step runs on, cuda:0 unless given",
    )
    parser.add_argument(
        "--result-dir",
        default=None,
        help=(
            f"result directory, defaults to {DEFAULT_RESULT_ROOT}/<timestamp> "
            f"inside the repository; point it outside the checkout to keep the "
            f"working tree clean"
        ),
    )
    parser.add_argument(
        "--kernel-warmup",
        type=int,
        default=DEFAULT_WARMUP,
        help=f"warmup iterations per step, {DEFAULT_WARMUP} unless given",
    )
    parser.add_argument(
        "--kernel-timed",
        type=int,
        default=DEFAULT_TIMED,
        help=f"timed iterations per step, {DEFAULT_TIMED} unless given",
    )
    parser.add_argument("--repo-dir", default=None, help="repository root of this run")
    parser.add_argument("--git-commit", default=None, help="commit the run belongs to")
    parser.add_argument("--git-subject", default=None, help="commit subject")
    parser.add_argument("--git-branch", default=None, help="branch the run belongs to")
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


def check_iteration_counts(warmup, timed):
    """The counts a run may take, checked before any GPU memory is taken.

    A run with no timed iteration has no sample to summarise, and a negative count
    is not a sample either, so both fail here rather than inside the measurement.
    """
    if timed <= 0:
        raise ValueError(f"--kernel-timed must be at least 1, got {timed}")
    if warmup < 0:
        raise ValueError(f"--kernel-warmup cannot be negative, got {warmup}")
    return warmup, timed


def visible_gpu_count():
    """Number of GPUs visible to this process, honouring CUDA_VISIBLE_DEVICES."""
    import torch

    if not torch.cuda.is_available():
        return 0
    return torch.cuda.device_count()


def detect_tp_size(explicit):
    """Resolve the tensor parallel size, defaulting to the visible GPU count.

    Hardcoding a count makes a machine with fewer GPUs fail deep inside the first
    step, which puts the error a long way from its cause. An explicit size is taken
    as given: `0` is a size the user asked for and not an omission, so it is
    refused here instead of being replaced by the visible count.
    """
    if explicit is not None:
        if explicit < 1:
            raise ValueError(f"--tp-size must be at least 1, got {explicit}")
        return explicit
    count = visible_gpu_count()
    if count < 1:
        raise RuntimeError(
            "no visible GPU, cannot determine the tensor parallel size; "
            "check the driver and CUDA_VISIBLE_DEVICES"
        )
    print(f"[setup] --tp-size not given, using the {count} visible GPUs as TP={count}")
    return count


def run_steps(
    plan, device, jsonl_path, warmup, timed, errors, seed, extend_branch, kv_protocol
):
    from .kernel_bench import run_case

    print(f"[steps] {len(plan.cases)} steps")
    # Truncate: a rerun into the same result directory must not leave a reader
    # with the records of two runs in one file.
    with open(jsonl_path, "w", encoding="utf-8") as handle:
        for case in plan.cases:
            print(
                f"[steps] {case.label} "
                f"context={case.context_tokens} tokens "
                f"pages={case.pages} "
                f"page_capacity={case.kv_page_capacity_bytes() / 2**20:.1f} MiB"
            )
            record = run_case(
                case,
                device,
                warmup=warmup,
                timed=timed,
                seed=seed,
                extend_branch=extend_branch,
                kv_protocol=kv_protocol,
            )
            handle.write(json.dumps(record, default=str) + "\n")
            handle.flush()
            for name, check in record["correctness"].items():
                if not check["passed"]:
                    errors.append(f"{case.label} {name} check failed")


def _run(command, timeout=30):
    """The trimmed output of a command, or None when it did not run.

    A tool this machine does not have is part of what the record describes, so a
    probe that cannot run leaves a missing value behind.
    """
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def probe_gpus():
    """The cards this process can see, read from nvidia-smi."""
    info = {
        "gpu_names": [],
        "gpu_count": 0,
        "per_gpu_memory_total_mib": [],
        "per_gpu_memory_used_mib": [],
        "driver_version": None,
    }
    query = _run(
        [
            "nvidia-smi",
            "--query-gpu=index,name,memory.total,memory.used,driver_version",
            "--format=csv,noheader",
        ]
    )
    if not query:
        return info
    for line in query.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 5:
            continue
        info["gpu_names"].append(parts[1])
        info["per_gpu_memory_total_mib"].append(parts[2])
        info["per_gpu_memory_used_mib"].append(parts[3])
        info["driver_version"] = parts[4]
    info["gpu_count"] = len(info["gpu_names"])
    return info


def probe_torch():
    """The torch build a run used. A record is written at the end of a run, and a
    run cannot reach that point without torch, so the import here is a lookup of
    the loaded module rather than a capability probe."""
    import torch

    info = {
        "torch_version": torch.__version__,
        "cuda_runtime": torch.version.cuda,
        "device_count": None,
        "device_names": [],
    }
    if torch.cuda.is_available():
        info["device_count"] = torch.cuda.device_count()
        info["device_names"] = [
            torch.cuda.get_device_name(index)
            for index in range(torch.cuda.device_count())
        ]
    return info


def probe_package(name):
    """The version and path of a package the run loaded."""
    module = __import__(name)
    return {
        "version": str(module.__version__),
        "path": getattr(module, "__file__", None),
    }


def probe_git(repo_dir):
    """The commit the run measured, detected from the tree the run started in."""
    return {
        "commit": _run(["git", "-C", repo_dir, "rev-parse", "HEAD"]),
        "subject": _run(["git", "-C", repo_dir, "log", "-1", "--pretty=%s"]),
        "branch": _run(["git", "-C", repo_dir, "rev-parse", "--abbrev-ref", "HEAD"]),
        "dirty": bool(_run(["git", "-C", repo_dir, "status", "--porcelain"])),
        "source": "detected",
    }


def probe_model(model_path):
    """The path, revision marker and config hash of the model a run loaded."""
    info = {"path": model_path, "revision": None, "config_sha": None}
    if not model_path or not os.path.isdir(model_path):
        return info
    for name in (".mv", ".msc", "revision.txt"):
        candidate = os.path.join(model_path, name)
        if os.path.isfile(candidate):
            with open(candidate, "r", encoding="utf-8", errors="replace") as handle:
                info["revision"] = handle.read().strip()[:200]
            break
    config_path = os.path.join(model_path, "config.json")
    if os.path.isfile(config_path):
        with open(config_path, "rb") as handle:
            info["config_sha"] = hashlib.sha256(handle.read()).hexdigest()[:16]
    return info


def probe_mooncake():
    """The Mooncake distribution installed here, named the way its own metadata names
    it: the wheel is built per CUDA flavour, so the name varies with the build."""
    providers = importlib.metadata.packages_distributions().get("mooncake", [])
    if not providers:
        return None
    return {
        "distribution": providers[0],
        "version": importlib.metadata.version(providers[0]),
    }


def record_environment(
    repo_dir,
    model_path,
    model_config,
    plan,
    command_line,
    run_facts,
    git_override=None,
):
    """The machine, the versions and the sources one result directory describes.

    git_override states the commit when the tree under test has no complete .git
    checkout, which is how a run on a copied tree names what it measured.
    """
    repo = probe_git(repo_dir)
    if repo["commit"] is None and git_override:
        repo = {
            "commit": git_override["commit"],
            "subject": git_override["subject"],
            "branch": git_override["branch"],
            "dirty": git_override["dirty"],
            "source": "explicit_from_driver",
            "repo_dir": repo_dir,
        }

    record = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "epoch_seconds": time.time(),
        "command": command_line,
        "repo": repo,
        "hostname": platform.node(),
        "python_version": sys.version.split()[0],
        "gpu": probe_gpus(),
        "torch": probe_torch(),
        "flashinfer": probe_package("flashinfer"),
        "sglang": probe_package("sglang"),
        "model": probe_model(model_path),
        "workload": plan.as_dict() if plan is not None else None,
    }
    record.update(run_facts)
    if plan is not None and "host" in tuple(plan.kv_sources):
        # What the moves ran through, and where they were told to write: the record of
        # a step states the rest, per case.
        record["kv_transfer"] = {
            "mooncake": probe_mooncake(),
            "protocol": run_facts.get("kv_source_protocol"),
            "endpoint": "this process",
        }
    if model_config is not None:
        aggregate, per_rank = model_config.kv_bytes_per_token(plan.tp_size)
        record["model"]["kv_config"] = model_config.as_dict()
        record["model"]["dtype"] = model_config.torch_dtype
        record["model"]["tp_size"] = plan.tp_size
        record["model"]["kv_bytes_per_token_aggregate"] = aggregate
        record["model"]["kv_bytes_per_token_per_rank"] = per_rank
    return record


def write_json(path, payload):
    """One JSON document of a result directory."""
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
        handle.write("\n")


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.dry_run and not args.model:
        parser.error("a real run needs --model; --dry-run accepts a placeholder")

    # Both are checked before anything takes GPU memory, and both apply to a
    # dry-run as well: it prints the run that would happen.
    check_iteration_counts(args.kernel_warmup, args.kernel_timed)
    # The extend branch is resolved here and not only on the measuring path, so a
    # dry-run states the branch the run would replay, including the case where
    # SGLANG_FLASHINFER_USE_PAGED disagrees with --extend-branch.
    extend_branch = resolve_extend_branch(args.extend_branch)

    if not args.dry_run:
        args.tp_size = detect_tp_size(args.tp_size)

    plan = build_plan_from_args(args)
    plan.finalize()

    if args.dry_run:
        print(plan.describe())
        print(
            f"prefill/extend lane: {extend_branch}; "
            f"SGLANG_FLASHINFER_USE_PAGED={use_paged_default()} in this process"
        )
        print()
        print(
            "The above is dry-run output; nothing was started and no GPU memory taken."
        )
        return 0

    repo_dir = args.repo_dir or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )
    result_dir = args.result_dir or default_result_dir()
    os.makedirs(result_dir, exist_ok=True)

    model_config = load_model_kv_config(args.model)
    if model_config.attention_layout != "dense":
        raise SystemExit(
            f"{args.model} has a {model_config.attention_layout} attention layout "
            f"({model_config.attention_layout_evidence}); this benchmark measures "
            f"models whose layers all keep a full-length KV cache"
        )

    plan.finalize(model_config)

    aggregate, per_rank = model_config.kv_bytes_per_token(plan.tp_size)
    print(
        f"[setup] model {model_config.model_type} "
        f"{model_config.num_layers} layers "
        f"{model_config.num_key_value_heads} KV heads "
        f"head_dim {model_config.head_dim} dtype {model_config.torch_dtype}"
    )
    print(
        f"[setup] KV bytes per token: {aggregate} aggregate, "
        f"{per_rank} per rank (TP={plan.tp_size})"
    )

    write_json(os.path.join(result_dir, "plan.json"), plan.as_dict())

    errors = []
    if args.kernel_warmup < MINIMUM_WARMUP:
        errors.append(f"warmup iterations below {MINIMUM_WARMUP}")
    if args.kernel_timed < MINIMUM_TIMED:
        errors.append(f"timed iterations below {MINIMUM_TIMED}")

    print(
        f"[setup] prefill/extend lane: {extend_branch}; "
        f"SGLANG_FLASHINFER_USE_PAGED={use_paged_default()} in this process"
    )

    run_steps(
        plan,
        args.device,
        os.path.join(result_dir, "kernel.jsonl"),
        args.kernel_warmup,
        args.kernel_timed,
        errors,
        args.seed,
        extend_branch,
        args.kv_source_protocol,
    )

    manifest = record_environment(
        repo_dir=repo_dir,
        model_path=args.model,
        model_config=model_config,
        plan=plan,
        command_line=" ".join(sys.argv),
        run_facts={
            "result_dir": os.path.abspath(result_dir),
            "device": args.device,
            "kernel_warmup": args.kernel_warmup,
            "kernel_timed": args.kernel_timed,
            "planned_failures": errors,
            "extend_branch": extend_branch,
            "flashinfer_use_paged_env": use_paged_default(),
            "kv_source_protocol": args.kv_source_protocol,
        },
        git_override=git_override_from_args(args),
    )
    write_json(os.path.join(result_dir, "manifest.json"), manifest)
    finalize(result_dir, manifest)

    if errors:
        print("this run has failures:", file=sys.stderr)
        for message in errors:
            print(f"  - {message}", file=sys.stderr)
        return 1
    print(f"[done] results in {result_dir}")
    return 0


def finalize(result_dir, manifest):
    """Aggregate the JSONL of the run into one summary and one CSV table."""
    records = summary_module.load_records(os.path.join(result_dir, "kernel.jsonl"))
    rows = summary_module.kernel_summary(records)
    payload = {
        "manifest": manifest,
        "steps": rows,
    }
    write_json(os.path.join(result_dir, "summary.json"), payload)
    summary_module.write_csv(
        os.path.join(result_dir, "kernel_summary.csv"),
        rows,
        summary_module.CSV_COLUMNS,
    )


if __name__ == "__main__":
    sys.exit(main())
