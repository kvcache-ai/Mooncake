import argparse
import json
import os
import sys
import time

from . import kernel_summary as summary_module
from . import manifest as manifest_module
from .cases import (
    BRANCH_CHOICES,
    DEFAULT_TIMED,
    DEFAULT_WARMUP,
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
            "which branch of FlashInferAttnBackend the extend steps replay: "
            "ragged_prefix_merge is what a server runs while "
            "SGLANG_FLASHINFER_USE_PAGED is False (the default), paged_extend is "
            "the single paged prefill call it runs with that variable set"
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
        help=f"result directory, defaults to {DEFAULT_RESULT_ROOT}/<timestamp>",
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


def detect_tp_size(explicit):
    """Resolve the tensor parallel size, defaulting to the visible GPU count.

    Hardcoding a count makes a machine with fewer GPUs fail deep inside the first
    step, which puts the error a long way from its cause.
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


def run_steps(plan, device, jsonl_path, warmup, timed, errors, seed, extend_branch):
    from .kernel_bench import run_case

    print(f"[steps] {len(plan.cases)} steps")
    results = []
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
            )
            handle.write(json.dumps(record, default=str) + "\n")
            handle.flush()
            results.append(record)
            for name, check in record["correctness"].items():
                if not check["passed"]:
                    errors.append(f"{case.label} {name} check failed")
    return results


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.dry_run and not args.model:
        parser.error("a real run needs --model; --dry-run accepts a placeholder")

    if not args.dry_run:
        args.tp_size = detect_tp_size(args.tp_size)

    plan = build_plan_from_args(args)
    plan.finalize()

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

    with open(os.path.join(result_dir, "plan.json"), "w", encoding="utf-8") as handle:
        json.dump(plan.as_dict(), handle, indent=2)
        handle.write("\n")

    errors = []
    if args.kernel_warmup < MINIMUM_WARMUP:
        errors.append(f"warmup iterations below {MINIMUM_WARMUP}")
    if args.kernel_timed < MINIMUM_TIMED:
        errors.append(f"timed iterations below {MINIMUM_TIMED}")

    extend_branch = resolve_extend_branch(args.extend_branch)
    print(
        f"[setup] extend steps replay {extend_branch}; "
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
    )

    manifest = manifest_module.build_manifest(
        repo_dir,
        args.model,
        model_config,
        plan,
        " ".join(sys.argv),
        extra={
            "result_dir": os.path.abspath(result_dir),
            "device": args.device,
            "kernel_warmup": args.kernel_warmup,
            "kernel_timed": args.kernel_timed,
            "planned_failures": errors,
            "extend_branch": extend_branch,
            "flashinfer_use_paged_env": use_paged_default(),
        },
        git_override=git_override_from_args(args),
    )
    manifest_module.write_json(os.path.join(result_dir, "manifest.json"), manifest)
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
    manifest_module.write_json(os.path.join(result_dir, "summary.json"), payload)
    summary_module.write_csv(os.path.join(result_dir, "kernel_summary.csv"), rows)


if __name__ == "__main__":
    sys.exit(main())
