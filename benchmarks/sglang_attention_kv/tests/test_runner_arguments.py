# The command line's own checks, without a GPU: the counts a run may take, the
# tensor parallel size it resolves, and the dry-run it prints. Each of these fails
# where the argument is read, before a run takes any GPU memory.

import pytest

from benchmarks.sglang_attention_kv import runner


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
    monkeypatch.setattr(runner.manifest_module, "visible_gpu_count", lambda: 8)
    assert runner.detect_tp_size(None) == 8
    monkeypatch.setattr(runner.manifest_module, "visible_gpu_count", lambda: 0)
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
