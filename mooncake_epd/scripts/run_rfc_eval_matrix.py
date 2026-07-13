from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.benchmarks.rfc_eval_matrix import write_rfc_eval_matrix_artifacts  # noqa: E402


def parse_args() -> argparse.Namespace:
    artifacts = REPO_ROOT / "artifacts"
    ap = argparse.ArgumentParser(description="Assemble RFC §8 evaluation matrix artifacts.")
    ap.add_argument("--phase6", default=str(artifacts / "phase6_metrics.json"))
    ap.add_argument("--soak", default=str(artifacts / "real_soak_report_post_kvdir.json"))
    ap.add_argument("--traces", default=str(artifacts / "workflow_traces.json"))
    ap.add_argument("--workload-manifest", default=None)
    ap.add_argument("--manifest-out", default=str(artifacts / "rfc_dev_small_manifest.json"))
    ap.add_argument("--matrix-out", default=str(artifacts / "rfc_eval_matrix.json"))
    ap.add_argument("--report-json-out", default=str(artifacts / "rfc_eval_report.json"))
    ap.add_argument("--report-md-out", default=str(artifacts / "rfc_eval_report.md"))
    ap.add_argument("--mixed-size", type=int, default=20)
    ap.add_argument("--dataset-root", default=None, help="Path to mooncake_test_dataset; when set, build real RFC §8 dataset artifacts.")
    ap.add_argument("--dataset-split", default="dev-small")
    ap.add_argument("--dataset-chat-split", default="dev-small")
    ap.add_argument("--dataset-eval-out", default=str(artifacts / "mooncake_dataset_eval.json"))
    ap.add_argument("--dataset-manifest-out", default=str(artifacts / "mooncake_dataset_manifest.json"))
    ap.add_argument("--run-workflow-traces", action="store_true")
    ap.add_argument("--run-ablations", action="store_true")
    ap.add_argument("--run-soak", action="store_true")
    ap.add_argument("--run-serving-e2e", action="store_true")
    ap.add_argument("--run-serving-baseline-compare", action="store_true")
    ap.add_argument("--ablation-output", default=str(artifacts / "real_ablation_report.json"))
    ap.add_argument("--ablation-baselines", nargs="+", default=None)
    ap.add_argument("--ablation-max-new-tokens", type=int, default=None)
    ap.add_argument("--soak-output", default=str(artifacts / "real_soak_report_post_kvdir.json"))
    ap.add_argument("--serving-e2e-workdir", default="/tmp/mooncake_epd_serving_e2e")
    ap.add_argument("--serving-e2e-summary", default=str(artifacts / "serving_e2e_summary.json"))
    ap.add_argument("--feature-handle-e2e-summary", default=str(artifacts / "feature_handle_e2e_summary.json"))
    ap.add_argument("--feature-handle-paired-summary", default=str(artifacts / "feature_handle_paired_benchmark.json"))
    ap.add_argument("--online-direct-e2e-summary", default=str(artifacts / "online_direct_e2e_summary.json"))
    ap.add_argument("--run-feature-handle-e2e", action="store_true")
    ap.add_argument("--run-online-direct-e2e", action="store_true")
    ap.add_argument("--run-feature-handle-paired-benchmark", action="store_true")
    ap.add_argument("--feature-handle-e2e-workdir", default="/tmp/mooncake_epd_feature_handle_e2e")
    ap.add_argument("--feature-handle-e2e-request", default=None)
    ap.add_argument("--feature-handle-e2e-store-dir", default=None)
    ap.add_argument("--feature-handle-e2e-local-hostname", default="127.0.0.1")
    ap.add_argument("--feature-handle-e2e-timeout", type=float, default=900.0)
    ap.add_argument("--feature-handle-e2e-request-timeout", type=float, default=300.0)
    ap.add_argument("--feature-handle-e2e-encoder-device", default="cuda:5")
    ap.add_argument("--feature-handle-e2e-prefill-gpu", type=int, default=3)
    ap.add_argument("--feature-handle-e2e-decode-gpu", type=int, default=4)
    ap.add_argument("--feature-handle-e2e-owner-shards", type=int, default=1)
    ap.add_argument("--feature-handle-e2e-max-group-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--feature-handle-e2e-max-transfer-descriptors", type=int, default=128)
    ap.add_argument("--feature-handle-e2e-max-transfer-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--feature-handle-e2e-kv-directory-rpc-url", default=None)
    ap.add_argument("--online-direct-e2e-workdir", default="/tmp/mooncake_epd_online_direct_e2e")
    ap.add_argument("--online-direct-e2e-local-hostname", default="127.0.0.1")
    ap.add_argument("--online-direct-e2e-timeout", type=float, default=900.0)
    ap.add_argument("--online-direct-e2e-request-timeout", type=float, default=300.0)
    ap.add_argument("--online-direct-e2e-encoder-device", default="cuda:5")
    ap.add_argument("--online-direct-e2e-encoder-port", type=int, default=8330)
    ap.add_argument("--online-direct-e2e-prefill-gpu", type=int, default=3)
    ap.add_argument("--online-direct-e2e-decode-gpu", type=int, default=4)
    ap.add_argument("--online-direct-e2e-owner-shards", type=int, default=1)
    ap.add_argument("--online-direct-e2e-max-group-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--online-direct-e2e-max-transfer-descriptors", type=int, default=128)
    ap.add_argument("--online-direct-e2e-max-transfer-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--online-direct-e2e-kv-directory-rpc-url", default=None)
    ap.add_argument("--feature-handle-paired-workdir", default="/tmp/mooncake_epd_feature_handle_paired")
    ap.add_argument("--feature-handle-paired-request", default=None)
    ap.add_argument("--feature-handle-paired-local-hostname", default="127.0.0.1")
    ap.add_argument("--feature-handle-paired-timeout", type=float, default=900.0)
    ap.add_argument("--feature-handle-paired-request-timeout", type=float, default=300.0)
    ap.add_argument("--feature-handle-paired-prefill-gpu", type=int, default=3)
    ap.add_argument("--feature-handle-paired-decode-gpu", type=int, default=4)
    ap.add_argument("--feature-handle-paired-owner-shards", type=int, default=1)
    ap.add_argument("--feature-handle-paired-between-mode-sleep-s", type=float, default=8.0)
    ap.add_argument("--feature-handle-paired-max-group-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--feature-handle-paired-max-transfer-descriptors", type=int, default=128)
    ap.add_argument("--feature-handle-paired-max-transfer-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--feature-handle-paired-kv-directory-rpc-url", default=None)
    ap.add_argument("--serving-baseline-compare-out", default=str(artifacts / "serving_baseline_compare.json"))
    ap.add_argument("--serving-e2e-local-hostname", default="127.0.0.1")
    ap.add_argument("--serving-e2e-allow-loopback", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--serving-e2e-max-dataset-requests", type=int, default=0)
    ap.add_argument("--serving-e2e-dataset-families", nargs="+", default=None)
    ap.add_argument("--serving-e2e-dataset-request-timeout", type=float, default=180.0)
    ap.add_argument("--serving-e2e-dataset-concurrency", type=int, default=1)
    ap.add_argument("--serving-e2e-dataset-qps", type=float, default=0.0)
    ap.add_argument("--serving-e2e-dataset-arrival-schedule", default=None)
    ap.add_argument("--serving-e2e-dataset-deadline-ms", type=float, default=0.0)
    ap.add_argument("--serving-e2e-dataset-goodput-slo-ms", type=float, default=30000.0)
    ap.add_argument("--serving-e2e-streaming-metrics", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--serving-e2e-warmup-requests", type=int, default=0)
    ap.add_argument("--serving-e2e-owner-shards", type=int, default=1)
    ap.add_argument("--serving-e2e-kv-directory-rpc-url", default=None)
    ap.add_argument("--serving-baseline-compare-local-hostname", default="127.0.0.1")
    ap.add_argument("--serving-baseline-compare-allow-loopback", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--serving-baseline-compare-max-dataset-requests", type=int, default=5)
    ap.add_argument("--serving-baseline-compare-dataset-families", nargs="+", default=["W0", "W1", "W2", "W3", "W4"])
    ap.add_argument("--serving-baseline-compare-dataset-request-timeout", type=float, default=180.0)
    ap.add_argument("--serving-baseline-compare-dataset-max-input-len", type=int, default=4096)
    ap.add_argument("--serving-baseline-compare-dataset-request-max-tokens", type=int, default=128)
    ap.add_argument("--serving-baseline-compare-dataset-image-max-pixels", type=int, default=1_003_520)
    ap.add_argument("--serving-baseline-compare-dataset-skip-oversized", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--serving-baseline-compare-baseline-gpu", type=int, default=2)
    ap.add_argument("--serving-baseline-compare-warmup-requests", type=int, default=1)
    ap.add_argument("--serving-baseline-compare-baseline-warmup-requests", type=int, default=1)
    ap.add_argument("--serving-baseline-compare-owner-shards", type=int, default=4)
    ap.add_argument("--serving-baseline-compare-kv-directory-rpc-url", default=None)
    ap.add_argument("--serving-baseline-compare-proxy-url", default=None)
    ap.add_argument("--serving-baseline-compare-high-overlap-threshold", type=float, default=0.70)
    ap.add_argument("--serving-baseline-compare-min-pass-rate", type=float, default=0.80)
    ap.add_argument("--serving-baseline-compare-min-same-finish-reason-rate", type=float, default=0.80)
    ap.add_argument("--serving-baseline-compare-min-avg-token-jaccard", type=float, default=0.75)
    ap.add_argument("--serving-baseline-compare-min-token-jaccard", type=float, default=0.60)
    ap.add_argument("--run-regression-gate", action="store_true")
    ap.add_argument("--enforce-regression-gate", action="store_true")
    return ap.parse_args()


def _build_ablation_command(args: argparse.Namespace) -> list[str]:
    script = REPO_ROOT / "benchmarks" / "real_ablation_runner.py"
    cmd = [sys.executable, str(script), "--output", args.ablation_output]
    if args.ablation_baselines:
        cmd.extend(["--baselines", *args.ablation_baselines])
    if args.ablation_max_new_tokens is not None:
        cmd.extend(["--max-new-tokens", str(int(args.ablation_max_new_tokens))])
    return cmd


def _build_regression_gate_command(*, report_json_out: str, matrix_out: str) -> list[str]:
    script = REPO_ROOT / "scripts" / "check_rfc_regressions.py"
    return [
        sys.executable,
        str(script),
        "--report",
        report_json_out,
        "--matrix",
        matrix_out,
    ]


def _build_serving_e2e_command(args: argparse.Namespace) -> list[str]:
    script = REPO_ROOT / "scripts" / "run_vllm_serving_e2e.py"
    cmd = [
        sys.executable,
        str(script),
        "--workdir",
        args.serving_e2e_workdir,
        "--local-hostname",
        args.serving_e2e_local_hostname,
        f"--{'allow-loopback' if args.serving_e2e_allow_loopback else 'no-allow-loopback'}",
        "--dataset-request-timeout",
        str(float(args.serving_e2e_dataset_request_timeout)),
        "--dataset-concurrency",
        str(int(args.serving_e2e_dataset_concurrency)),
        "--dataset-qps",
        str(float(args.serving_e2e_dataset_qps)),
        "--dataset-deadline-ms",
        str(float(args.serving_e2e_dataset_deadline_ms)),
        "--dataset-goodput-slo-ms",
        str(float(args.serving_e2e_dataset_goodput_slo_ms)),
        f"--{'dataset-streaming-metrics' if args.serving_e2e_streaming_metrics else 'no-dataset-streaming-metrics'}",
        "--warmup-requests",
        str(int(args.serving_e2e_warmup_requests)),
        "--owner-shards",
        str(int(args.serving_e2e_owner_shards)),
    ]
    if args.dataset_root:
        cmd.extend(
            [
                "--dataset-root",
                args.dataset_root,
                "--dataset-chat-split",
                args.dataset_chat_split,
                "--max-dataset-requests",
                str(int(args.serving_e2e_max_dataset_requests)),
            ]
        )
    if args.serving_e2e_dataset_families:
        cmd.extend(["--dataset-families", *args.serving_e2e_dataset_families])
    if args.serving_e2e_dataset_arrival_schedule:
        cmd.extend(["--dataset-arrival-schedule", args.serving_e2e_dataset_arrival_schedule])
    if args.serving_e2e_kv_directory_rpc_url:
        cmd.extend(["--kv-directory-rpc-url", args.serving_e2e_kv_directory_rpc_url])
    return cmd


def _build_feature_handle_e2e_command(args: argparse.Namespace) -> list[str]:
    script = REPO_ROOT / "scripts" / "run_vllm_feature_handle_e2e.py"
    cmd = [
        sys.executable,
        str(script),
        "--workdir",
        args.feature_handle_e2e_workdir,
        "--local-hostname",
        args.feature_handle_e2e_local_hostname,
        "--timeout",
        str(float(args.feature_handle_e2e_timeout)),
        "--request-timeout",
        str(float(args.feature_handle_e2e_request_timeout)),
        "--encoder-device",
        args.feature_handle_e2e_encoder_device,
        "--prefill-gpu",
        str(int(args.feature_handle_e2e_prefill_gpu)),
        "--decode-gpu",
        str(int(args.feature_handle_e2e_decode_gpu)),
        "--owner-shards",
        str(int(args.feature_handle_e2e_owner_shards)),
        "--max-group-bytes",
        str(int(args.feature_handle_e2e_max_group_bytes)),
        "--max-transfer-descriptors",
        str(int(args.feature_handle_e2e_max_transfer_descriptors)),
        "--max-transfer-bytes",
        str(int(args.feature_handle_e2e_max_transfer_bytes)),
    ]
    if args.feature_handle_e2e_request:
        cmd.extend(["--request", args.feature_handle_e2e_request])
    if args.feature_handle_e2e_store_dir:
        cmd.extend(["--store-dir", args.feature_handle_e2e_store_dir])
    if args.feature_handle_e2e_kv_directory_rpc_url:
        cmd.extend(["--kv-directory-rpc-url", args.feature_handle_e2e_kv_directory_rpc_url])
    return cmd


def _build_online_direct_e2e_command(args: argparse.Namespace) -> list[str]:
    script = REPO_ROOT / "scripts" / "run_vllm_online_direct_e2e.py"
    cmd = [
        sys.executable,
        str(script),
        "--workdir",
        args.online_direct_e2e_workdir,
        "--local-hostname",
        args.online_direct_e2e_local_hostname,
        "--timeout",
        str(float(args.online_direct_e2e_timeout)),
        "--request-timeout",
        str(float(args.online_direct_e2e_request_timeout)),
        "--encoder-device",
        args.online_direct_e2e_encoder_device,
        "--encoder-port",
        str(int(args.online_direct_e2e_encoder_port)),
        "--prefill-gpu",
        str(int(args.online_direct_e2e_prefill_gpu)),
        "--decode-gpu",
        str(int(args.online_direct_e2e_decode_gpu)),
        "--owner-shards",
        str(int(args.online_direct_e2e_owner_shards)),
        "--max-group-bytes",
        str(int(args.online_direct_e2e_max_group_bytes)),
        "--max-transfer-descriptors",
        str(int(args.online_direct_e2e_max_transfer_descriptors)),
        "--max-transfer-bytes",
        str(int(args.online_direct_e2e_max_transfer_bytes)),
    ]
    if args.online_direct_e2e_kv_directory_rpc_url:
        cmd.extend(["--kv-directory-rpc-url", args.online_direct_e2e_kv_directory_rpc_url])
    return cmd

def _build_feature_handle_paired_command(args: argparse.Namespace) -> list[str]:
    script = REPO_ROOT / "scripts" / "run_feature_handle_paired_benchmark.py"
    request = args.feature_handle_paired_request or args.feature_handle_e2e_request
    if not request:
        raise SystemExit("--run-feature-handle-paired-benchmark requires --feature-handle-paired-request or --feature-handle-e2e-request")
    cmd = [
        sys.executable,
        str(script),
        "--workdir",
        args.feature_handle_paired_workdir,
        "--output",
        args.feature_handle_paired_summary,
        "--request",
        request,
        "--local-hostname",
        args.feature_handle_paired_local_hostname,
        "--timeout",
        str(float(args.feature_handle_paired_timeout)),
        "--request-timeout",
        str(float(args.feature_handle_paired_request_timeout)),
        "--prefill-gpu",
        str(int(args.feature_handle_paired_prefill_gpu)),
        "--decode-gpu",
        str(int(args.feature_handle_paired_decode_gpu)),
        "--owner-shards",
        str(int(args.feature_handle_paired_owner_shards)),
        "--between-mode-sleep-s",
        str(float(args.feature_handle_paired_between_mode_sleep_s)),
        "--max-group-bytes",
        str(int(args.feature_handle_paired_max_group_bytes)),
        "--max-transfer-descriptors",
        str(int(args.feature_handle_paired_max_transfer_descriptors)),
        "--max-transfer-bytes",
        str(int(args.feature_handle_paired_max_transfer_bytes)),
    ]
    if args.feature_handle_paired_kv_directory_rpc_url:
        cmd.extend(["--kv-directory-rpc-url", args.feature_handle_paired_kv_directory_rpc_url])
    return cmd

def _build_serving_baseline_compare_command(args: argparse.Namespace) -> list[str]:
    script = REPO_ROOT / "scripts" / "run_serving_baseline_compare.py"
    cmd = [
        sys.executable,
        str(script),
        "--workdir",
        str(Path(args.serving_baseline_compare_out).resolve().parent / "serving_baseline_compare_workdir"),
        "--output",
        args.serving_baseline_compare_out,
        "--local-hostname",
        args.serving_baseline_compare_local_hostname,
        f"--{'allow-loopback' if args.serving_baseline_compare_allow_loopback else 'no-allow-loopback'}",
        "--dataset-root",
        args.dataset_root,
        "--dataset-chat-split",
        args.dataset_chat_split,
        "--max-dataset-requests",
        str(int(args.serving_baseline_compare_max_dataset_requests)),
        "--dataset-request-timeout",
        str(float(args.serving_baseline_compare_dataset_request_timeout)),
        "--dataset-max-input-len",
        str(int(args.serving_baseline_compare_dataset_max_input_len)),
        "--dataset-request-max-tokens",
        str(int(args.serving_baseline_compare_dataset_request_max_tokens)),
        "--dataset-image-max-pixels",
        str(int(args.serving_baseline_compare_dataset_image_max_pixels)),
        f"--{'dataset-skip-oversized' if args.serving_baseline_compare_dataset_skip_oversized else 'no-dataset-skip-oversized'}",
        "--baseline-gpu",
        str(int(args.serving_baseline_compare_baseline_gpu)),
        "--warmup-requests",
        str(int(args.serving_baseline_compare_warmup_requests)),
        "--baseline-warmup-requests",
        str(int(args.serving_baseline_compare_baseline_warmup_requests)),
        "--owner-shards",
        str(int(args.serving_baseline_compare_owner_shards)),
        "--high-overlap-threshold",
        str(float(args.serving_baseline_compare_high_overlap_threshold)),
        "--min-pass-rate",
        str(float(args.serving_baseline_compare_min_pass_rate)),
        "--min-same-finish-reason-rate",
        str(float(args.serving_baseline_compare_min_same_finish_reason_rate)),
        "--min-avg-token-jaccard",
        str(float(args.serving_baseline_compare_min_avg_token_jaccard)),
        "--min-token-jaccard",
        str(float(args.serving_baseline_compare_min_token_jaccard)),
    ]
    if args.serving_baseline_compare_dataset_families:
        cmd.extend(["--dataset-families", *args.serving_baseline_compare_dataset_families])
    if args.serving_baseline_compare_kv_directory_rpc_url:
        cmd.extend(["--kv-directory-rpc-url", args.serving_baseline_compare_kv_directory_rpc_url])
    if args.serving_baseline_compare_proxy_url:
        cmd.extend(["--proxy-url", args.serving_baseline_compare_proxy_url])
    return cmd


def main() -> None:
    args = parse_args()
    executed_steps: dict[str, object] = {}
    serving_e2e_summary_path = args.serving_e2e_summary
    serving_baseline_compare_path = args.serving_baseline_compare_out
    feature_handle_e2e_summary_path = args.feature_handle_e2e_summary
    feature_handle_paired_summary_path = args.feature_handle_paired_summary
    online_direct_e2e_summary_path = args.online_direct_e2e_summary

    if args.run_workflow_traces:
        subprocess_run = __import__("subprocess").run
        script = REPO_ROOT / "scripts" / "export_workflow_traces.py"
        subprocess_run([sys.executable, str(script), "--output", args.traces], check=True)
        executed_steps["workflow_traces"] = args.traces

    if args.run_ablations:
        subprocess_run = __import__("subprocess").run
        subprocess_run(_build_ablation_command(args), check=True)
        executed_steps["ablations"] = args.ablation_output

    if args.run_soak:
        subprocess_run = __import__("subprocess").run
        script = REPO_ROOT / "benchmarks" / "real_model_soak.py"
        subprocess_run([sys.executable, str(script), "--output", args.soak_output, "--rounds", "1"], check=True)
        executed_steps["soak"] = args.soak_output

    if args.run_serving_e2e:
        subprocess_run = __import__("subprocess").run
        subprocess_run(_build_serving_e2e_command(args), check=True)
        executed_steps["serving_e2e"] = args.serving_e2e_workdir
        serving_e2e_summary_path = str(Path(args.serving_e2e_workdir) / "serving_e2e_summary.json")

    if args.run_feature_handle_e2e:
        subprocess_run = __import__("subprocess").run
        subprocess_run(_build_feature_handle_e2e_command(args), check=True)
        executed_steps["feature_handle_e2e"] = args.feature_handle_e2e_workdir
        feature_handle_e2e_summary_path = str(Path(args.feature_handle_e2e_workdir) / "feature_handle_e2e_summary.json")

    if args.run_online_direct_e2e:
        subprocess_run = __import__("subprocess").run
        subprocess_run(_build_online_direct_e2e_command(args), check=True)
        executed_steps["online_direct_e2e"] = args.online_direct_e2e_workdir
        online_direct_e2e_summary_path = str(Path(args.online_direct_e2e_workdir) / "online_direct_e2e_summary.json")

    if args.run_feature_handle_paired_benchmark:
        subprocess_run = __import__("subprocess").run
        subprocess_run(_build_feature_handle_paired_command(args), check=True)
        executed_steps["feature_handle_paired_benchmark"] = args.feature_handle_paired_workdir
        feature_handle_paired_summary_path = args.feature_handle_paired_summary

    if args.run_serving_baseline_compare:
        if not args.dataset_root:
            raise SystemExit("--run-serving-baseline-compare requires --dataset-root")
        subprocess_run = __import__("subprocess").run
        subprocess_run(_build_serving_baseline_compare_command(args), check=True)
        executed_steps["serving_baseline_compare"] = args.serving_baseline_compare_out
        serving_baseline_compare_path = args.serving_baseline_compare_out

    matrix = write_rfc_eval_matrix_artifacts(
        phase6_path=args.phase6,
        soak_path=args.soak,
        workflow_traces_path=args.traces,
        workload_manifest_path=(args.dataset_manifest_out if args.dataset_root and args.workload_manifest is None else args.workload_manifest),
        ablation_report_path=args.ablation_output,
        serving_e2e_summary_path=serving_e2e_summary_path,
        serving_baseline_compare_path=serving_baseline_compare_path,
        feature_handle_e2e_summary_path=feature_handle_e2e_summary_path,
        feature_handle_paired_benchmark_path=feature_handle_paired_summary_path,
        online_direct_e2e_summary_path=online_direct_e2e_summary_path,
        manifest_output_path=args.manifest_out,
        matrix_output_path=args.matrix_out,
        report_json_path=args.report_json_out,
        report_md_path=args.report_md_out,
        mixed_size=args.mixed_size,
        dataset_root=args.dataset_root,
        dataset_eval_output_path=args.dataset_eval_out if args.dataset_root else None,
        dataset_split=args.dataset_split,
        dataset_chat_split=args.dataset_chat_split,
    )
    regression_gate: dict[str, object] | None = None
    if args.run_regression_gate or args.enforce_regression_gate:
        gate_cmd = _build_regression_gate_command(
            report_json_out=args.report_json_out,
            matrix_out=args.matrix_out,
        )
        gate_proc = subprocess.run(
            gate_cmd,
            check=args.enforce_regression_gate,
            capture_output=True,
            text=True,
        )
        regression_gate = json.loads(gate_proc.stdout)
        executed_steps["regression_gate"] = regression_gate.get("pass")
    print(json.dumps(
        {
            "matrix_out": args.matrix_out,
            "report_json_out": args.report_json_out,
            "report_md_out": args.report_md_out,
            "manifest_out": args.manifest_out,
            "dataset_eval_out": args.dataset_eval_out if args.dataset_root else None,
            "dataset_manifest_out": args.dataset_manifest_out if args.dataset_root else None,
            "baseline_coverage": matrix.get("baseline_coverage", {}),
            "alignment": matrix.get("alignment", {}),
            "serving_correctness": (matrix.get("report", {}) or {}).get("serving_correctness_table", {}),
            "feature_handle_e2e": (matrix.get("report", {}) or {}).get("feature_handle_e2e_table", {}),
            "online_direct_e2e": (matrix.get("report", {}) or {}).get("online_direct_e2e_table", {}),
            "feature_handle_paired": (matrix.get("report", {}) or {}).get("feature_handle_paired_table", {}),
            "regression_gate": regression_gate,
            "executed_steps": executed_steps,
        },
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()
