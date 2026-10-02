import random
import socket

import pytest

from benchmarks.attention_kv.config import E2EConfig, build_workload_cases
from benchmarks.attention_kv.e2e import (
    SGLangServer,
    build_prompts,
    find_free_port,
    merge_output_ids,
    shared_prefix,
)


def test_merge_output_ids_handles_cumulative_chunks():
    """By default SGLang sends the complete list so far in each chunk."""
    accumulated = []
    for incoming in ([11], [11, 12], [11, 12, 13]):
        accumulated = merge_output_ids(accumulated, incoming)
    assert accumulated == [11, 12, 13]


def test_merge_output_ids_handles_incremental_chunks():
    """With incremental_streaming_output on, each chunk carries only the delta."""
    accumulated = []
    for incoming in ([11], [12], [13]):
        accumulated = merge_output_ids(accumulated, incoming)
    assert accumulated == [11, 12, 13]


def test_merge_output_ids_appends_when_not_a_prefix():
    accumulated = merge_output_ids([1, 2], [3, 4])
    assert accumulated == [1, 2, 3, 4]


def test_merge_output_ids_keeps_full_list_on_final_chunk():
    accumulated = merge_output_ids([1, 2], [1, 2, 3, 4, 5])
    assert accumulated == [1, 2, 3, 4, 5]


def test_merge_output_ids_does_not_mutate_inputs():
    accumulated = [1, 2]
    incoming = [1, 2, 3]
    merged = merge_output_ids(accumulated, incoming)
    merged.append(9)
    assert accumulated == [1, 2]
    assert incoming == [1, 2, 3]


def test_multiturn_context_is_previous_input_plus_full_output():
    """A multi-turn round's next input is the previous input plus the full output."""
    prompt = [7, 8, 9]
    chunks = ([21], [21, 22], [21, 22, 23])
    generated = []
    for chunk in chunks:
        generated = merge_output_ids(generated, chunk)
    assert prompt + generated == [7, 8, 9, 21, 22, 23]


def _unreachable_port():
    """Hold a port briefly and release it, leaving a port nothing is listening on."""
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


def test_flush_cache_raises_after_retries(tmp_path):
    """A failed flush has to raise rather than return False, or the run would
    continue on a stale cache."""
    port = _unreachable_port()
    server = SGLangServer(
        {
            "python": "python3",
            "model_path": "placeholder",
            "tp_size": 1,
            "page_size": 64,
            "port": port,
            "mem_fraction_static": 0.5,
            "cache_tier": "gpu_only",
            "log_name": "unused.log",
        },
        str(tmp_path),
    )
    with pytest.raises(RuntimeError, match="flush_cache"):
        server.flush_cache(attempts=2, interval=0.0)


def test_find_free_port_returns_a_bindable_port():
    port = find_free_port()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", port))


def _partial_hit_case(input_len=512, num_requests=4):
    case = build_workload_cases(
        input_lens=(input_len,), output_lens=(1,), num_requests=num_requests
    )[0]
    return case


def test_partial_hit_shared_prefix_is_half_the_input():
    case = _partial_hit_case()
    rng = random.Random(1)
    shared = shared_prefix("partial_hit", case, rng, 1000)
    assert len(shared) == case.input_len // 2


def test_no_measured_prompt_equals_the_warmup_prompt():
    """The warmup may only cache the shared prefix. Warming with a whole request
    would cache that request's own suffix, and the measured batch would then show a
    better hit rate than the pattern it is labelled with."""
    case = _partial_hit_case(num_requests=4)
    for pattern in ("full_hit", "partial_hit"):
        rng = random.Random(f"{pattern}:seed")
        shared = shared_prefix(pattern, case, rng, 1000)
        prompts = build_prompts(pattern, case, rng, 1000, shared)
        assert len(prompts) == case.num_requests
        for prompt in prompts:
            assert prompt[: len(shared)] == shared
        if pattern == "partial_hit":
            # Every request, the first included, has its own suffix beyond the
            # shared prefix, so all of them hit exactly the shared half
            assert all(len(prompt) == case.input_len for prompt in prompts)
            assert all(prompt != shared for prompt in prompts)
            assert len({tuple(prompt) for prompt in prompts}) == case.num_requests


def test_full_hit_prompts_are_all_the_shared_prefix():
    case = _partial_hit_case()
    rng = random.Random(7)
    shared = shared_prefix("full_hit", case, rng, 1000)
    prompts = build_prompts("full_hit", case, rng, 1000, shared)
    assert all(prompt == shared for prompt in prompts)
    assert len(shared) == case.input_len


def test_cold_miss_has_no_shared_prefix_and_no_repeated_prompt():
    case = _partial_hit_case()
    rng = random.Random(3)
    assert shared_prefix("cold_miss", case, rng, 1000) is None
    prompts = build_prompts("cold_miss", case, rng, 1000, None)
    assert len({tuple(prompt) for prompt in prompts}) == case.num_requests


def test_hit_patterns_without_a_prefix_are_rejected_when_none_is_given():
    case = _partial_hit_case()
    rng = random.Random(4)
    with pytest.raises(ValueError, match="shared prefix"):
        build_prompts("partial_hit", case, rng, 1000, None)


def test_multiturn_has_no_shared_prefix():
    case = _partial_hit_case()
    rng = random.Random(5)
    assert shared_prefix("multiturn", case, rng, 1000) is None


def test_partial_hit_config_reports_the_pattern_it_measures():
    config = E2EConfig(cache_tier="host", hit_pattern="partial_hit")
    assert config.hit_pattern == "partial_hit"
    assert config.rounds == 1
