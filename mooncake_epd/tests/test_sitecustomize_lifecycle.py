from __future__ import annotations

from dataclasses import dataclass

from sitecustomize import _filter_stale_kv_completions


@dataclass
class _ConnectorOutput:
    finished_recving: set[str] | None
    finished_sending: set[str] | None


def test_filter_stale_kv_completions_preserves_live_requests_only():
    output = _ConnectorOutput(
        finished_recving={"recv-live", "recv-stale"},
        finished_sending={"send-live", "send-stale"},
    )

    stale = _filter_stale_kv_completions(
        output,
        {"recv-live": object(), "send-live": object()},
    )

    assert output.finished_recving == {"recv-live"}
    assert output.finished_sending == {"send-live"}
    assert stale == {
        "finished_recving": ["recv-stale"],
        "finished_sending": ["send-stale"],
    }


def test_filter_stale_kv_completions_keeps_empty_or_absent_signals():
    output = _ConnectorOutput(finished_recving=None, finished_sending=set())

    assert _filter_stale_kv_completions(output, {}) == {}
    assert output.finished_recving is None
    assert output.finished_sending == set()
