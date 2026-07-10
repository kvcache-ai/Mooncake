from __future__ import annotations

from mooncake_epd.scripts.run_serving_baseline_compare import (
    _build_compare_gate,
    _comparison_flags,
    _normalize_text,
    _string_metrics,
)
from mooncake_epd.scripts.run_vllm_serving_e2e import _extract_choice_text


def test_normalize_text_collapses_case_punctuation_and_spaces():
    assert _normalize_text(" Hello,\nMooncake-EPD!!  ") == "hello mooncake epd"


def test_string_metrics_reports_normalized_match_and_overlap():
    metrics = _string_metrics(
        "The room contains a sofa, table, and lamp.",
        "the room contains a sofa table and lamp",
    )

    assert metrics["exact_match"] is False
    assert metrics["normalized_exact_match"] is True
    assert metrics["token_jaccard"] == 1.0


def test_extract_choice_text_handles_chat_message_and_delta_shapes():
    choice_message = {
        "message": {
            "content": [
                {"type": "text", "text": "hello"},
                {"type": "text", "text": " world"},
            ]
        }
    }
    choice_delta = {
        "delta": {
            "content": [
                {"type": "text", "text": "stream"},
                {"type": "text", "text": " text"},
            ]
        }
    }

    assert _extract_choice_text(choice_message) == "hello world"
    assert _extract_choice_text(choice_delta) == "stream text"


def test_comparison_flags_treat_same_finish_reason_and_high_overlap_as_pass_like():
    baseline = {
        "finish_reason": "length",
        "usage": {"completion_tokens": 32},
    }
    serving = {
        "finish_reason": "length",
        "usage": {"completion_tokens": 30},
    }
    text_metrics = {
        "normalized_exact_match": False,
        "token_jaccard": 0.82,
    }

    flags = _comparison_flags(baseline, serving, text_metrics, high_overlap_threshold=0.7)

    assert flags["same_finish_reason"] is True
    assert flags["high_overlap"] is True
    assert flags["normalized_exact_or_high_overlap"] is True
    assert flags["both_truncated"] is True
    assert flags["completion_tokens_delta"] == -2


def test_build_compare_gate_passes_with_high_overlap_truncation_rows():
    rows = [
        {
            "text_metrics": {"token_jaccard": 1.0},
            "comparison_flags": {
                "same_finish_reason": True,
                "high_overlap": True,
                "normalized_exact_or_high_overlap": True,
                "completion_tokens_delta": 0,
            },
        },
        {
            "text_metrics": {"token_jaccard": 0.74},
            "comparison_flags": {
                "same_finish_reason": True,
                "high_overlap": True,
                "normalized_exact_or_high_overlap": True,
                "completion_tokens_delta": -2,
            },
        },
    ]

    gate = _build_compare_gate(
        rows,
        high_overlap_threshold=0.7,
        min_pass_rate=0.8,
        min_same_finish_reason_rate=0.8,
        min_avg_token_jaccard=0.75,
        min_token_jaccard=0.6,
    )

    assert gate["pass_recommendation"] is True
    assert gate["same_finish_reason_rate"] == 1.0
    assert gate["normalized_exact_or_high_overlap_rate"] == 1.0
    assert gate["min_token_jaccard"] == 0.74
    assert gate["gate_failures"] == []


def test_compare_gate_accepts_structural_equivalence_for_short_truncated_probe():
    flags = _comparison_flags(
        {
            "finish_reason": "length",
            "usage": {"prompt_tokens": 1008, "completion_tokens": 16},
        },
        {
            "finish_reason": "length",
            "usage": {"prompt_tokens": 1008, "completion_tokens": 16},
        },
        {"normalized_exact_match": False, "token_jaccard": 0.35},
        high_overlap_threshold=0.7,
    )
    rows = [{"text_metrics": {"token_jaccard": 0.35}, "comparison_flags": flags}]

    gate = _build_compare_gate(
        rows,
        high_overlap_threshold=0.7,
        min_pass_rate=0.8,
        min_same_finish_reason_rate=0.8,
        min_avg_token_jaccard=0.75,
        min_token_jaccard=0.6,
    )

    assert flags["structural_completion_equivalent"] is True
    assert flags["control_plane_equivalent"] is True
    assert gate["pass_recommendation"] is True
    assert gate["control_plane_equivalent_rate"] == 1.0
    assert gate["structural_completion_equivalent_rate"] == 1.0
    assert gate["gate_failures"] == []
    assert gate["gate_warnings"]


def test_build_compare_gate_fails_when_finish_reason_and_overlap_drop_below_threshold():
    rows = [
        {
            "text_metrics": {"token_jaccard": 0.55},
            "comparison_flags": {
                "same_finish_reason": False,
                "high_overlap": False,
                "normalized_exact_or_high_overlap": False,
                "completion_tokens_delta": 8,
            },
        },
        {
            "text_metrics": {"token_jaccard": 0.65},
            "comparison_flags": {
                "same_finish_reason": True,
                "high_overlap": False,
                "normalized_exact_or_high_overlap": False,
                "completion_tokens_delta": 4,
            },
        },
    ]

    gate = _build_compare_gate(
        rows,
        high_overlap_threshold=0.7,
        min_pass_rate=0.8,
        min_same_finish_reason_rate=0.8,
        min_avg_token_jaccard=0.75,
        min_token_jaccard=0.6,
    )

    assert gate["pass_recommendation"] is False
    assert gate["same_finish_reason_rate"] == 0.5
    assert gate["normalized_exact_or_high_overlap_rate"] == 0.0
    assert any("same_finish_reason_rate" in item for item in gate["gate_failures"])
    assert any("avg_token_jaccard" in item for item in gate["gate_failures"])
