# The command line's own checks: the counts a run may take, the tensor parallel size
# it resolves, the dry-run it prints, and the environment record a result carries.
# The first three fail where the argument is read, before a run takes any GPU
# memory; the record names the versions a run loaded, so it skips where the stack a
# run needs is missing.

import hashlib
import json
import platform
import sys

import pytest

from benchmarks.sglang_attention_kv import runner
from benchmarks.sglang_attention_kv.cases import build_plan_from_args
from benchmarks.sglang_attention_kv.model_config import load_model_kv_config


def test_iteration_counts_are_refused_before_a_run_starts():
    assert runner.check_iteration_counts(10, 100) == (10, 100)
    assert runner.check_iteration_counts(0, 1) == (0, 1)
    with pytest.raises(ValueError, match="--kernel-timed must be at least 1"):
        runner.check_iteration_counts(10, 0)
    with pytest.raises(ValueError, match="--kernel-timed must be at least 1"):
        runner.check_iteration_counts(10, -1)
    with pytest.raises(ValueError, match="--kernel-warmup cannot be negative"):
        runner.check_iteration_counts(-1, 100)


def test_an_explicit_tensor_parallel_size_is_taken_as_given():
    """`0` is a size the user asked for, not an omission: it is refused instead of
    being replaced by the visible GPU count."""
    assert runner.detect_tp_size(4) == 4
    assert runner.detect_tp_size(1) == 1
    with pytest.raises(ValueError, match="--tp-size must be at least 1"):
        runner.detect_tp_size(0)
    with pytest.raises(ValueError, match="--tp-size must be at least 1"):
        runner.detect_tp_size(-2)


def test_the_default_tensor_parallel_size_is_the_visible_gpu_count(monkeypatch):
    monkeypatch.setattr(runner, "visible_gpu_count", lambda: 8)
    assert runner.detect_tp_size(None) == 8
    monkeypatch.setattr(runner, "visible_gpu_count", lambda: 0)
    with pytest.raises(RuntimeError, match="no visible GPU"):
        runner.detect_tp_size(None)


def test_a_dry_run_resolves_the_branch_before_printing(monkeypatch, capsys):
    """A dry-run states the branch the run would replay, so it resolves it the same
    way: with SGLANG_FLASHINFER_USE_PAGED set and --extend-branch disagreeing, it
    fails instead of printing a branch the run would not take."""
    monkeypatch.setenv("SGLANG_FLASHINFER_USE_PAGED", "1")
    with pytest.raises(ValueError, match="disagrees with SGLANG_FLASHINFER_USE_PAGED"):
        runner.main(["--full", "--dry-run", "--extend-branch", "ragged_prefix_merge"])

    monkeypatch.delenv("SGLANG_FLASHINFER_USE_PAGED", raising=False)
    code = runner.main(
        ["--quick", "--dry-run", "--extend-branch", "paged_extend", "--tp-size", "4"]
    )
    printed = capsys.readouterr().out
    assert code == 0
    assert "paged_extend" in printed
    assert "SGLANG_FLASHINFER_USE_PAGED=False" in printed


def test_a_dry_run_refuses_counts_a_run_could_not_summarise(capsys):
    with pytest.raises(ValueError, match="--kernel-timed must be at least 1"):
        runner.main(["--quick", "--dry-run", "--kernel-timed", "0"])


def test_the_environment_record_names_the_machine_and_the_versions(tmp_path):
    """A result directory has to say which machine, which versions and which model
    produced it, so a reader can tell two results apart."""
    pytest.importorskip("torch")
    pytest.importorskip("flashinfer")
    pytest.importorskip("sglang")
    import torch

    model_dir = tmp_path / "model"
    model_dir.mkdir()
    body = {
        "num_hidden_layers": 4,
        "num_attention_heads": 8,
        "num_key_value_heads": 4,
        "hidden_size": 1024,
        "torch_dtype": "bfloat16",
    }
    (model_dir / "config.json").write_text(json.dumps(body), encoding="utf-8")

    model_config = load_model_kv_config(model_dir)
    args = runner.build_parser().parse_args(
        ["--full", "--model", str(model_dir), "--tp-size", "4"]
    )
    plan = build_plan_from_args(args).finalize(model_config)

    record = runner.record_environment(
        repo_dir=str(tmp_path),
        model_path=str(model_dir),
        model_config=model_config,
        plan=plan,
        command_line="benchmarks.sglang_attention_kv --full",
        run_facts={"device": "cuda:0"},
    )

    assert record["command"] == "benchmarks.sglang_attention_kv --full"
    assert record["hostname"] == platform.node()
    assert record["python_version"] == sys.version.split()[0]
    assert record["device"] == "cuda:0"
    # nvidia-smi lists the cards it can see, and the count is the list it parsed.
    assert record["gpu"]["gpu_count"] == len(record["gpu"]["gpu_names"])
    assert record["torch"]["torch_version"] == torch.__version__
    assert record["torch"]["cuda_runtime"] == torch.version.cuda
    assert record["flashinfer"]["path"] and record["flashinfer"]["version"]
    assert record["sglang"]["path"] and record["sglang"]["version"]
    assert (
        record["model"]["config_sha"]
        == hashlib.sha256((model_dir / "config.json").read_bytes()).hexdigest()[:16]
    )
    assert record["model"]["tp_size"] == 4
    assert record["model"]["kv_bytes_per_token_per_rank"] > 0
    assert record["workload"]["tp_size"] == 4
    # The temporary tree is not a checkout, so the probe reports finding none.
    assert record["repo"]["commit"] is None
    assert record["repo"]["source"] == "detected"


def test_the_environment_record_takes_the_commit_it_is_given(tmp_path):
    """A tree with no checkout states its commit through the arguments, which is how
    a run on a copied tree names what it measured."""
    pytest.importorskip("torch")
    pytest.importorskip("flashinfer")
    pytest.importorskip("sglang")

    record = runner.record_environment(
        repo_dir=str(tmp_path),
        model_path=None,
        model_config=None,
        plan=None,
        command_line="benchmarks.sglang_attention_kv",
        run_facts={},
        git_override={
            "commit": "c0ffee1",
            "subject": "a subject",
            "branch": "a-branch",
            "dirty": None,
        },
    )

    assert record["repo"]["commit"] == "c0ffee1"
    assert record["repo"]["branch"] == "a-branch"
    assert record["repo"]["source"] == "explicit_from_driver"
    assert record["repo"]["repo_dir"] == str(tmp_path)
    assert record["workload"] is None
    assert record["model"] == {"path": None, "revision": None, "config_sha": None}
