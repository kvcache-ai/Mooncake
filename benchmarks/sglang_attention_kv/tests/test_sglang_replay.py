# The replay against SGLang's own structures. These need a GPU and SGLang, so they
# skip where either is missing; the point of them is that the KV a step reads is
# the KV that was written through the token table, which is what a page table that
# moved between the write and the read breaks.

import pytest

torch = pytest.importorskip("torch")

if not torch.cuda.is_available():
    pytest.skip("needs a CUDA device", allow_module_level=True)

pytest.importorskip("sglang")

from benchmarks.sglang_attention_kv.cases import (  # noqa: E402
    BRANCH_PAGED_EXTEND,
    BRANCH_RAGGED_PREFIX_MERGE,
    KernelCase,
    short_case,
)
from benchmarks.sglang_attention_kv import kernel_bench  # noqa: E402
from benchmarks.sglang_attention_kv.kernel_bench import (  # noqa: E402
    DEFERRED_SYNC_ITERATIONS,
    FETCH_ITERATIONS,
    PLAN_SAMPLES,
    one_iteration,
    run_case,
)
from benchmarks.sglang_attention_kv.sglang_replay import (  # noqa: E402
    VERIFY_SOURCE,
    SglangStep,
)
from benchmarks.sglang_attention_kv.stats import STEP_PHASES  # noqa: E402

DEVICE = "cuda:0"


def step_case(
    mode,
    prefix_lens,
    new_lens,
    page_size=64,
    layout="random",
    layers=2,
    qo_heads=8,
    kv_heads=2,
    kv_source="gpu",
):
    return KernelCase(
        mode=mode,
        prefix_lens=prefix_lens,
        new_lens=new_lens,
        page_size=page_size,
        layout=layout,
        num_layers=layers,
        num_qo_heads=qo_heads,
        num_kv_heads=kv_heads,
        head_dim=128,
        dtype="bfloat16",
        kv_source=kv_source,
    )


def prepared(case, extend_branch=BRANCH_RAGGED_PREFIX_MERGE, seed=17):
    step = SglangStep(case, DEVICE, seed=seed, extend_branch=extend_branch)
    step.prepare()
    indices = step.build_indices()
    step.plan(indices)
    # The branch decides the order, as it does in the server.
    if step.reads_before_write:
        step.read(step.make_query())
        step.write_kv(indices)
    else:
        step.write_kv(indices)
        step.read(step.make_query())
    return step, indices


@pytest.mark.parametrize("page_size", [1, 64])
@pytest.mark.parametrize("layout", ["contiguous", "random"])
@pytest.mark.parametrize("mode", ["prefill", "extend", "decode"])
def test_the_step_passes_every_check(mode, layout, page_size):
    prefix_lens = (0,) if mode == "prefill" else (512,)
    new_lens = {"prefill": (128,), "extend": (128,), "decode": (1,)}[mode]
    step, indices = prepared(
        step_case(mode, prefix_lens, new_lens, page_size=page_size, layout=layout)
    )
    assert step.check_indices(indices)["passed"]
    assert step.check_history()["passed"]
    assert step.check_attention(step.make_query())["passed"]


@pytest.mark.parametrize(
    "extend_branch", [BRANCH_RAGGED_PREFIX_MERGE, BRANCH_PAGED_EXTEND]
)
def test_both_extend_branches_pass_their_checks(extend_branch):
    step, indices = prepared(
        step_case("extend", (512,), (128,)), extend_branch=extend_branch
    )
    assert step.branch == extend_branch
    assert step.check_indices(indices)["passed"]
    assert step.check_history()["passed"]
    assert step.check_attention(step.make_query())["passed"]


def test_the_history_can_arrive_through_mooncake():
    """With a host source the pool starts empty and Mooncake moves the history in,
    so the content check that follows a step is also the check on that move: the
    rows that arrive have to be exactly the ones the benchmark generated."""
    case = step_case("decode", (512,), (1,), kv_source="host")
    step = SglangStep(case, DEVICE, seed=5)
    step.prepare()
    assert step.history is not None
    # One entry per pool page per layer per tensor: the history's 512 tokens fill
    # eight pages at page size 64, over two layers, K and V.
    assert step.history.entries == 8 * case.num_layers * 2

    # Nothing has been moved yet, so the pool does not hold the history.
    assert not step.check_history()["passed"]

    fetched = step.fetch_kv()
    assert fetched == case.kv_bytes(512)
    indices = step.build_indices()
    step.write_kv(indices)
    torch.cuda.synchronize()
    assert step.check_history()["passed"]

    # A source row the step reads, changed behind the benchmark's back, has to reach
    # the pool and fail the check: the check is on the move, not only on the write.
    host = step.history.buffers[(case.num_layers - 1, "v")]
    host[4] = host[5]
    step.fetch_kv()
    torch.cuda.synchronize()
    assert not step.check_history()["passed"]
    step.close()


def test_a_step_whose_history_is_in_the_pool_moves_nothing():
    """The pool source keeps its history where the step reads it, and states no
    fetch rather than a window around nothing."""
    case = step_case("decode", (512,), (1,))
    step = SglangStep(case, DEVICE, seed=5)
    step.prepare()
    assert step.history is None
    assert step.fetch_kv() == 0
    assert step.as_dict()["history_holder"] is None
    step.close()


def test_a_step_with_no_history_holds_nothing_whatever_its_source_says():
    """A prefill has no cached history, so a host source has nothing to stage: the
    engine refuses a zero-length registration, and the step records no holder and
    moves nothing."""
    case = step_case("prefill", (0,), (256,), kv_source="host")
    step = SglangStep(case, DEVICE, seed=5)
    step.prepare()
    assert step.history is None
    assert step.fetch_kv() == 0
    assert step.as_dict()["history_holder"] is None
    assert step.as_dict()["kv_source"] == "host"
    step.close()


def test_the_content_check_catches_a_step_that_never_wrote():
    """The write is what the check reads, so a step whose read ran but whose write did
    not is a failure rather than a pass on the warmup's state."""
    case = step_case("decode", (256,), (1,), layers=2)
    step = SglangStep(case, DEVICE, seed=3)
    step.prepare()
    indices = step.build_indices()
    step.plan(indices)
    step.read(step.make_query())
    torch.cuda.synchronize()
    assert not step.check_history()["passed"]
    step.close()


def test_a_poisoned_source_fails_the_check_after_the_measured_move(monkeypatch):
    """The check follows the move the run measured rather than an earlier one: a
    source row changed before the run reaches the pool, and the check fails there."""
    case = step_case("decode", (512,), (1,), kv_source="host", layers=2)
    original = SglangStep.prepare

    def poison(self):
        original(self)
        # The arithmetic check builds a short step of its own, which has no history.
        if self.history is not None:
            host = self.history.buffers[(self.case.num_layers - 1, "v")]
            host[3] = host[3] + 1.0
        return self

    monkeypatch.setattr(SglangStep, "prepare", poison)
    result = run_case(case, DEVICE, warmup=1, timed=2)
    assert not result["correctness"]["history"]["passed"]
    assert result["witness"]["write_calls"] == 2 * case.num_layers


def test_a_non_finite_output_fails_the_sampled_check():
    """A comparison against a NaN is false rather than true, so an output full of
    them would otherwise report no difference at all."""
    case = step_case("decode", (256,), (1,))
    step, _ = prepared(case)
    query = step.make_query()
    output = torch.full_like(step.read(query), float("nan"))
    torch.cuda.synchronize()
    result = step.check_attention_rows(query, output, case.num_layers - 1)
    assert result["non_finite"] > 0
    assert not result["passed"]


def test_the_sampled_rows_include_one_that_must_not_see_the_future():
    """A step's last query sees the whole context, so a mask error leaves it looking
    right; the first and middle rows are the ones that catch it."""
    case = step_case("extend", (512,), (128,), layers=1)
    step, _ = prepared(case)
    query = step.make_query()
    output = step.read(query)
    torch.cuda.synchronize()
    result = step.check_attention_rows(query, output, case.num_layers - 1)
    assert result["passed"]
    assert result["rows"] == [
        {"sequence": 0, "query": 0},
        {"sequence": 0, "query": 63},
        {"sequence": 0, "query": 127},
    ]

    # The middle row, replaced by attention that saw the tokens after it.
    layer_id = case.num_layers - 1
    keys, values = step._expected_kv(layer_id, 0)
    repeat = case.group_ratio()
    keys = keys.float().transpose(0, 1).repeat_interleave(repeat, dim=0)
    values = values.float().transpose(0, 1).repeat_interleave(repeat, dim=0)
    middle = (case.new_lens[0] - 1) // 2
    row = query[middle].float().unsqueeze(1)
    scores = torch.matmul(row, keys.transpose(-1, -2)) * (1.0 / case.head_dim**0.5)
    unmasked = torch.matmul(torch.softmax(scores, dim=-1), values).squeeze(1)
    output[middle] = unmasked.to(output.dtype)
    torch.cuda.synchronize()
    assert not step.check_attention_rows(query, output, layer_id)["passed"]


def test_a_writer_that_stopped_writing_fails_the_write_path_check(monkeypatch):
    """Every timed iteration writes the same K/V to the same slots, so the content
    check would pass on the warmup's values. This is the check that reads back a
    payload the step never wrote, and a writer that stopped writing cannot pass it."""
    case = step_case("decode", (256,), (1,), layers=2)
    step, indices = prepared(case)
    assert step.check_write_path(indices, source=VERIFY_SOURCE)["passed"]

    monkeypatch.setattr(
        SglangStep, "write_payload", lambda self, layer_id, indices, key, value: None
    )
    result = step.check_write_path(indices, source=VERIFY_SOURCE + 1_000)
    assert not result["passed"]
    assert result["mismatched_elements"] > 0


def test_a_measured_move_keeps_its_first_move_and_its_samples():
    """The move a request pays when the cache comes back is kept apart from the ones
    that follow it, and every move of the pass is kept in order with the parts it was
    timed in, so a summary is never divided by another summary's figure."""
    case = step_case("decode", (512,), (1,), kv_source="host", layers=2)
    result = run_case(case, DEVICE, warmup=1, timed=2)
    fetch = result["fetch"]
    assert fetch["first"]["total_ms"] > 0
    assert len(fetch["steady"]["samples"]) == FETCH_ITERATIONS
    for sample in fetch["steady"]["samples"]:
        assert set(sample) == {"total_ms", "descriptors_ms", "submit_ms", "device_ms"}
        assert sample["total_ms"] > 0
    assert fetch["steady"]["total"]["p50"] > 0
    assert fetch["steady"]["submit"]["p50"] > 0
    assert result["correctness"]["write_path"]["passed"]


def test_the_branch_decides_the_order():
    """SGLang's ragged branches compute the attention and save the KV cache
    afterwards; the paged branches write first."""
    assert SglangStep(
        step_case("prefill", (0,), (128,)), DEVICE, seed=1
    ).reads_before_write
    assert SglangStep(
        step_case("extend", (512,), (128,)), DEVICE, seed=1
    ).reads_before_write
    assert not SglangStep(
        step_case("decode", (512,), (1,)), DEVICE, seed=1
    ).reads_before_write
    paged = SglangStep(
        step_case("extend", (512,), (128,)),
        DEVICE,
        seed=1,
        extend_branch=BRANCH_PAGED_EXTEND,
    )
    assert not paged.reads_before_write


def test_the_merge_branch_reads_the_history_through_the_page_table():
    """The merge branch's paged side reads the cached history, not the new tokens,
    so the CSR stream is the history's rows."""
    step, indices = prepared(step_case("extend", (512,), (128,)))
    assert indices.paged_indices.numel() == 512
    assert step.check_indices(indices)["passed"]
    expected = step.token_pool.req_to_token[int(step.req_pool_indices[0]), :512].long()
    assert indices.paged_indices.long().tolist() == expected.tolist()


@pytest.mark.parametrize("page_size", [1, 64])
def test_the_csr_stream_is_the_token_table_row(page_size):
    step, indices = prepared(
        step_case("decode", (512, 256), (1, 1), page_size=page_size)
    )
    expected = step._expected_rows()
    assert indices.paged_indices.long().tolist() == expected.tolist()


def test_a_csr_stream_of_the_wrong_length_fails_the_index_check():
    """A stream that names a different number of rows cannot be compared slot by
    slot, so the check reports it as a failure and states how long each side is."""
    step, indices = prepared(step_case("decode", (512,), (1,)))
    entries = indices.paged_indices.numel()
    indices.paged_indices = indices.paged_indices[:-1]
    result = step.check_indices(indices)
    assert result["passed"] is False
    assert result["stream_entries"] == entries - 1
    assert result["expected_entries"] == entries


def test_the_ragged_prefill_has_no_paged_history_to_name():
    """Its tokens go to the ragged wrapper, so a caller asking this branch for the
    lengths of a paged side is mistaken rather than looking at an empty history."""
    step = SglangStep(step_case("prefill", (0,), (128,)), DEVICE, seed=1)
    with pytest.raises(ValueError, match="reads no paged KV"):
        step._history_lens()


def test_a_measured_case_reports_how_much_the_timed_loop_ran():
    """Every timed iteration writes one layer's K/V per layer and fills one index
    stream. A run states that it did, because the checks read the state those calls
    left and every iteration writes the same content to the same slots."""
    case = step_case("decode", (512,), (1,), layers=3)
    result = run_case(case, DEVICE, warmup=1, timed=4, seed=11)
    assert result["witness"] == {
        "write_calls": 12,
        "index_fills": 4,
        "expected_write_calls": 12,
        "expected_index_fills": 4,
    }


def test_a_timed_loop_that_skipped_its_writes_is_caught(monkeypatch):
    """The guard fires where the work is missing rather than letting the checks
    accept the state the warmup left."""
    monkeypatch.setattr(SglangStep, "write_layer", lambda self, layer_id, indices: None)
    with pytest.raises(RuntimeError, match="write calls"):
        run_case(step_case("decode", (256,), (1,), layers=2), DEVICE, warmup=1, timed=2)


def test_the_sampled_reference_reads_the_output_of_a_timed_iteration():
    """The arithmetic check that runs at full size follows rows of the output the
    timed loop produced, and reads the same content the kernels read: a value the
    step's own read did not produce has to fail it."""
    case = step_case("decode", (512,), (1,))
    result = run_case(case, DEVICE, warmup=1, timed=2, seed=13)
    sampled = result["correctness"]["attention_rows"]
    assert sampled["passed"]
    assert sampled["rows"] == [{"sequence": 0, "query": 0}]

    corrupted = prepared(case)[0]
    # The row to break is in the layer whose output is compared below, and the
    # reference is rebuilt from that layer's content, so a value the read did not
    # produce has to show up.
    layer_id = case.num_layers - 1
    _, value_buffer = corrupted.kv_pool.get_kv_buffer(layer_id)
    row = int(corrupted.token_pool.req_to_token[int(corrupted.req_pool_indices[0]), 4])
    value_buffer[row] = 0.0
    torch.cuda.synchronize()
    query = corrupted.make_query()
    output = corrupted.read(query)
    torch.cuda.synchronize()
    result = corrupted.check_attention_rows(query, output, layer_id)
    assert not result["passed"]
    assert result["max_abs_diff"] > 0.0


def test_the_merge_uses_the_implementation_sglang_picks():
    """FlashInfer's merge kernel holds a bounded number of heads in its thread
    block; above that the backend calls SGLang's Triton merge instead. The replay
    follows that choice, so a case above the bound measures the call a server makes
    and does not fail to launch."""
    wide, _ = prepared(step_case("extend", (512,), (128,), qo_heads=8, kv_heads=2))
    assert wide.merge_state_impl == "flashinfer"
    assert wide.as_dict()["merge_state_impl"] == "flashinfer"

    many, indices = prepared(
        step_case("extend", (512,), (128,), qo_heads=80, kv_heads=8)
    )
    assert many.merge_state_impl == "triton"
    assert many.as_dict()["merge_state_impl"] == "triton"
    assert many.check_indices(indices)["passed"]
    assert many.check_history()["passed"]


def test_build_indices_does_not_synchronise_with_the_host(monkeypatch):
    """The timed window must hold no device-to-host copy, so nothing in it may
    read a value back off the GPU."""
    step = SglangStep(step_case("decode", (512,), (1,)), DEVICE, seed=3)
    step.prepare()

    def refuse(self):
        raise AssertionError("build_indices read a tensor value back to the host")

    monkeypatch.setattr(torch.Tensor, "item", refuse)
    indices = step.build_indices()
    assert indices.paged_indptr[-1].numel() == 1


def test_the_history_reads_what_was_written_for_every_layer():
    step, indices = prepared(step_case("decode", (768,), (1,), layers=3))
    result = step.check_history()
    assert result["mismatched_k_elements"] == 0
    assert result["mismatched_v_elements"] == 0
    assert result["tokens_checked"] == 769 * 3


def test_a_corrupted_v_is_caught():
    """The content check has to cover V as well as K: a V that was never written
    where the token table points is a broken cache, not a passing check."""
    step, indices = prepared(step_case("decode", (512,), (1,), layers=2))
    assert step.check_history()["passed"]

    _, value_buffer = step.kv_pool.get_kv_buffer(1)
    row = int(step.token_pool.req_to_token[int(step.req_pool_indices[0]), 7])
    value_buffer[row] = 0.0
    torch.cuda.synchronize()

    result = step.check_history()
    assert not result["passed"]
    assert result["mismatched_v_elements"] > 0


def test_a_corrupted_k_is_caught():
    step, indices = prepared(step_case("decode", (512,), (1,), layers=2))
    key_buffer, _ = step.kv_pool.get_kv_buffer(1)
    row = int(step.token_pool.req_to_token[int(step.req_pool_indices[0]), 7])
    key_buffer[row] = 0.0
    torch.cuda.synchronize()

    result = step.check_history()
    assert not result["passed"]
    assert result["mismatched_k_elements"] > 0


def test_the_gather_ledger_counts_both_tensors_of_the_rows_it_moves():
    step, indices = prepared(step_case("extend", (512,), (128,)))
    rows = step.gather_rows(indices).numel()
    expected = (
        2
        * step.case.num_layers
        * rows
        * step.case.num_kv_heads
        * step.case.head_dim
        * 2
    )
    assert step.gather_bytes(indices) == expected
    # The merge branch's probe moves the history's rows, which is what the paged
    # side of that branch reads.
    assert rows == 512


def test_the_allocator_never_hands_out_the_reserved_page():
    for layout in ("contiguous", "random"):
        step, indices = prepared(step_case("extend", (512,), (128,), layout=layout))
        assert int(step.step_slots.min()) >= step.case.page_size
        assert int(indices.paged_indices.min()) >= step.case.page_size


def test_slots_follow_the_page_size():
    for page_size in (1, 16, 64):
        step, _ = prepared(step_case("extend", (256,), (64,), page_size=page_size))
        assert step.step_slots.numel() == 64
        assert step.prefix_slots[0].numel() == 256


def test_a_churned_pool_scatters_the_pages():
    """The step's pages come from a pool that churned, so they are a random
    subset of it rather than the ascending run a fresh pool hands out."""
    step, indices = prepared(
        step_case("extend", (512,), (128,), page_size=64, layout="random")
    )
    pages = sorted({int(slot) // 64 for slot in step.prefix_slots[0].tolist()})
    assert len(pages) == 8
    assert pages != list(range(pages[0], pages[0] + 8))
    assert max(pages) > 8
    assert step.check_history()["passed"]


def test_the_churned_layout_follows_the_seed():
    """Two seeds draw different page orders, and one seed draws the same order
    twice: the layout is a random draw that stays fixed for the run."""
    orders = []
    for seed in (1, 2, 1):
        step = SglangStep(
            step_case("extend", (512,), (128,), layout="random"), DEVICE, seed=seed
        )
        step.prepare()
        orders.append(step.prefix_slots[0].tolist())
    assert orders[0] != orders[1]
    assert orders[0] == orders[2]


def test_a_contiguous_pool_keeps_the_pages_together():
    step, _ = prepared(
        step_case("extend", (512,), (128,), page_size=64, layout="contiguous")
    )
    pages = [int(slot) // 64 for slot in step.prefix_slots[0].tolist()[::64]]
    assert pages == sorted(pages)
    assert pages[-1] - pages[0] == len(pages) - 1


def test_the_attention_check_is_sensitive_to_a_mask_that_is_missing():
    """An extend step computes more than one query per sequence, so its queries
    must not see the tokens after them. The check compares against the causal
    reference, and the same comparison against a reference without the mask has
    to fail: otherwise a kernel that sees the future would pass."""
    step, _ = prepared(
        step_case("extend", (256,), (128,)), extend_branch=BRANCH_PAGED_EXTEND
    )
    query = step.make_query()
    causal = step.check_attention(query)
    unmasked = step.unmasked_reference(query)
    assert causal["passed"]
    assert causal["mask"] == "causal"
    assert not unmasked["passed"], "a kernel that sees the future must not pass"


def test_the_merge_branch_is_causal_as_well():
    step, _ = prepared(step_case("extend", (256,), (128,)))
    query = step.make_query()
    assert step.check_attention(query)["passed"]
    assert not step.unmasked_reference(query)["passed"]


def test_the_read_ledger_splits_the_paged_side_from_the_ragged_side():
    """A merge step reads its history through the paged wrapper and its own tokens
    through the ragged one, so the bytes it reads are those two counts and not the
    whole context of a paged step."""
    step, _ = prepared(step_case("extend", (500,), (100,)))
    case = step.case
    read = step.read_bytes()
    assert read["paged"] == case.kv_bytes(500)
    assert read["ragged"] == case.kv_bytes(100)
    assert read["attention"] == case.kv_bytes(600)
    assert read["attention"] < case.kv_page_capacity_bytes()

    paged_step, _ = prepared(step_case("prefill", (0,), (600,)))
    paged_read = paged_step.read_bytes()
    assert paged_read["paged"] == 0
    assert paged_read["ragged"] == paged_step.case.kv_bytes(600)

    extend_step, _ = prepared(
        step_case("extend", (500,), (100,)), extend_branch=BRANCH_PAGED_EXTEND
    )
    extend_read = extend_step.read_bytes()
    assert extend_read["paged"] == extend_step.case.kv_bytes(600)
    assert extend_read["ragged"] == 0


def test_the_step_has_a_window_of_its_own_beside_the_phase_sum():
    """The three windows are timed separately, so phase_sum is their sum and the
    step is longer than it by the interval between the windows. step_window
    measures the whole step in one span."""
    step, _ = prepared(step_case("extend", (256,), (128,)))
    _, measured, _ = one_iteration(step, step.make_query())
    assert set(measured) == set(STEP_PHASES) | {"step_window", "phase_sum"}
    assert measured["phase_sum"] == pytest.approx(
        sum(measured[name] for name in STEP_PHASES), rel=1e-12
    )
    assert measured["step_window"] >= measured["phase_sum"]


def test_a_case_measures_the_device_it_was_given(monkeypatch):
    """The events, the synchronise after them and the step's tensors have to be on
    the case's device, with the process's current device left on another one. A
    case measured without that binding records its windows on the current device
    and returns from its synchronise before its own device has finished."""
    if torch.cuda.device_count() < 2:
        pytest.skip("needs two visible devices")
    torch.cuda.set_device(0)
    device = "cuda:1"
    case = short_case(step_case("extend", (512,), (128,), layers=4))
    measured = run_case(
        case,
        device,
        warmup=2,
        timed=3,
        seed=3,
        extend_branch=BRANCH_RAGGED_PREFIX_MERGE,
    )
    loop = measured["phases"]["layer_loop"]["p50"]
    window = measured["phases"]["step_window"]["p50"]
    assert measured["device"] == device
    assert torch.cuda.current_device() == 0
    assert measured["correctness"]["indices"]["passed"]
    assert measured["correctness"]["history"]["passed"]
    # A window that belonged to an idle device would hold no work at all.
    assert loop > 0.1, f"layer_loop={loop} looks like an idle device's timeline"
    assert window >= loop


def test_the_checks_read_the_state_the_timed_loop_left(monkeypatch):
    """A step that wrote the wrong values has to fail on its own iteration, not on
    the state the diagnostics rebuilt. The write component rewrites exactly the
    slots the step wrote, so a check that ran after it could not see this."""
    case = short_case(step_case("extend", (256,), (128,)))
    original = kernel_bench.one_iteration

    def corrupting(step, query, defer=None):
        indices, measured, output = original(step, query, defer=defer)
        _, value_buffer = step.kv_pool.get_kv_buffer(0)
        value_buffer[indices.out_cache_loc[0]] = 0.0
        torch.cuda.synchronize()
        return indices, measured, output

    monkeypatch.setattr(kernel_bench, "one_iteration", corrupting)
    result = run_case(case, DEVICE, warmup=1, timed=2)
    history = result["correctness"]["history"]
    assert not history["passed"], "a corrupted write passed its own check"
    assert history["mismatched_v_elements"] > 0


def test_the_decode_wrapper_follows_sglang_tensor_core_policy(monkeypatch):
    """The tensor-core path is decided per case by SGLang's own function, not fixed:
    at bf16 it takes the path from a group size of 4 query heads per KV head up, and
    the environment variable overrides the choice.

    The variable is cleared first: an environment that already sets it would
    otherwise decide the default cases and make this assert the wrong thing.
    """
    monkeypatch.delenv("SGLANG_FLASHINFER_USE_TENSOR_CORE", raising=False)
    wide = prepared(step_case("decode", (512,), (1,), qo_heads=8, kv_heads=2))[0]
    assert wide.decode_use_tensor_cores is True
    assert wide.as_dict()["decode_use_tensor_cores"] is True

    narrow = prepared(step_case("decode", (512,), (1,), qo_heads=8, kv_heads=4))[0]
    assert narrow.decode_use_tensor_cores is False
    assert narrow.as_dict()["decode_use_tensor_cores"] is False

    monkeypatch.setenv("SGLANG_FLASHINFER_USE_TENSOR_CORE", "false")
    off = prepared(step_case("decode", (512,), (1,), qo_heads=8, kv_heads=2))[0]
    assert off.decode_use_tensor_cores is False
    assert off.as_dict()["decode_use_tensor_cores"] is False

    monkeypatch.setenv("SGLANG_FLASHINFER_USE_TENSOR_CORE", "true")
    forced = prepared(step_case("decode", (512,), (1,), qo_heads=8, kv_heads=4))[0]
    assert forced.decode_use_tensor_cores is True


def test_the_page_size_selects_the_allocator_a_server_builds():
    """SGLang takes the token allocator at page size 1 and the paged one above it,
    and the two allocate a step's slots through different entry points: the token
    allocator raises on alloc_extend and alloc_decode. A step has to use the
    allocator its pool's page size selects."""
    token_step, _ = prepared(step_case("extend", (512,), (128,), page_size=1))
    assert token_step.allocator_kind == "token"
    assert token_step.as_dict()["slot_allocator"] == "token"
    assert token_step.step_slots.numel() == 128

    paged_step, _ = prepared(step_case("extend", (512,), (128,), page_size=64))
    assert paged_step.allocator_kind == "paged"
    assert paged_step.step_slots.numel() == 128


def test_the_index_metadata_is_held_rather_than_allocated_per_step():
    """The backend holds its index buffers and fills them in place, so the index
    window is the fill and not the allocation. Two builds hand back the same
    buffers, with the stream refilled."""
    step, _ = prepared(step_case("extend", (512,), (128,)))
    first = step.build_indices()
    torch.cuda.synchronize()
    before = first.paged_indices.clone()
    second = step.build_indices()
    assert second.qo_indptr.data_ptr() == first.qo_indptr.data_ptr()
    assert second.paged_indptr.data_ptr() == first.paged_indptr.data_ptr()
    assert second.paged_indices.data_ptr() == first.paged_indices.data_ptr()
    assert second.paged_last_page_len.data_ptr() == first.paged_last_page_len.data_ptr()
    torch.cuda.synchronize()
    assert torch.equal(second.paged_indices, before)
    assert step.check_indices(second)["passed"]


def test_the_component_passes_run_against_a_plan_of_their_own_indices(monkeypatch):
    """The components read the indices they built, so they have to run against a
    plan of those and not the metadata the last timed iteration left behind. The
    plan is taken outside every window, so it is not part of what a component
    measures."""
    case = short_case(step_case("extend", (256,), (128,)))
    order = []
    measured_step = []
    original_plan = SglangStep.plan

    def record(step, marker):
        # The correctness check builds a short step of its own, which plans too;
        # this test is about the sequence the measured step runs.
        if not measured_step or step is measured_step[0]:
            order.append(marker)

    def plan(self, indices):
        record(self, "plan")
        return original_plan(self, indices)

    def iteration(step, query, defer=None):
        if not measured_step:
            measured_step.append(step)
        record(step, "iteration")
        return original_iteration(step, query, defer=defer)

    def write_component(step, indices):
        record(step, "write_component")
        return original_write(step, indices)

    def attention_component(step, query):
        record(step, "attention_component")
        return original_attention(step, query)

    def gather(step, indices):
        record(step, "gather")
        return original_gather(step, indices)

    original_iteration = kernel_bench.one_iteration
    original_write = kernel_bench.one_write_component
    original_attention = kernel_bench.one_attention_component
    original_gather = kernel_bench.one_gather
    monkeypatch.setattr(SglangStep, "plan", plan)
    monkeypatch.setattr(kernel_bench, "one_iteration", iteration)
    monkeypatch.setattr(kernel_bench, "one_write_component", write_component)
    monkeypatch.setattr(kernel_bench, "one_attention_component", attention_component)
    monkeypatch.setattr(kernel_bench, "one_gather", gather)
    run_case(case, DEVICE, warmup=1, timed=2)
    # Each iteration plans for its own indices inside its window: the warmup, the
    # timed loop, and the deferred-sync pass that follows the checks. In between, the
    # plan the component passes run against is taken outside every window, and the
    # plan's own wall clock is sampled beside it.
    expected = (
        ["iteration", "plan"] * 3
        + ["plan"]
        + [
            "write_component",
            "write_component",
            "attention_component",
            "attention_component",
            "gather",
            "gather",
        ]
        + ["plan"] * PLAN_SAMPLES
        + ["iteration", "plan"] * DEFERRED_SYNC_ITERATIONS
    )
    assert order[: len(expected)] == expected


def test_the_step_loop_interleaves_write_and_read_per_layer(monkeypatch):
    """The step is timed as SGLang runs it: one layer's attention and one layer's
    write, layer by layer, rather than all the writes and then all the reads."""
    step, _ = prepared(step_case("extend", (256,), (128,)))
    order = []
    original_write = step.write_layer
    original_read = step.run_layer

    def write(layer_id, indexed):
        order.append(("write", layer_id))
        return original_write(layer_id, indexed)

    def read(layer_id, query):
        order.append(("read", layer_id))
        return original_read(layer_id, query)

    monkeypatch.setattr(step, "write_layer", write)
    monkeypatch.setattr(step, "run_layer", read)
    _, measured, _ = one_iteration(step, step.make_query())
    assert set(measured) == set(STEP_PHASES) | {"step_window", "phase_sum"}
    # The merge branch reads a layer and then writes it, so the calls alternate
    assert order[0] == ("read", 0)
    assert order[1] == ("write", 0)
    assert order[2] == ("read", 1)
    assert len(order) == 2 * step.case.num_layers


def test_prefill_writes_kv_but_reads_no_paged_stream():
    step, indices = prepared(step_case("prefill", (0,), (128,)))
    assert indices.paged_indices is None
    assert step.check_indices(indices)["passed"]
    assert step.check_history()["passed"]


@pytest.mark.parametrize("page_size", [1, 64])
@pytest.mark.parametrize("layout", ["contiguous", "random"])
def test_paged_prefill_reads_written_kv_causally(page_size, layout, monkeypatch):
    case = step_case(
        "prefill", (0, 0, 0), (128, 73, 19), page_size=page_size, layout=layout
    )
    step, indices = prepared(case, extend_branch=BRANCH_PAGED_EXTEND)
    assert step.branch == BRANCH_PAGED_EXTEND
    assert not step.reads_before_write
    assert indices.paged_indices.numel() == case.new_tokens
    assert step.read_bytes() == {
        "paged": case.kv_bytes(case.new_tokens),
        "ragged": 0,
        "attention": case.kv_bytes(case.new_tokens),
    }

    order = []
    original_write, original_read = step.write_layer, step.run_layer

    def write(layer_id, indexed):
        order.append(("write", layer_id))
        return original_write(layer_id, indexed)

    def read(layer_id, query):
        order.append(("read", layer_id))
        return original_read(layer_id, query)

    monkeypatch.setattr(step, "write_layer", write)
    monkeypatch.setattr(step, "run_layer", read)
    query = step.make_query()
    timed_indices, _, _ = one_iteration(step, query)
    assert order == [
        (operation, layer_id)
        for layer_id in range(case.num_layers)
        for operation in ("write", "read")
    ]
    assert step.check_indices(timed_indices)["passed"]
    assert step.check_history()["passed"]
    assert step.check_attention(query)["passed"]
    assert not step.unmasked_reference(query)["passed"]


def test_the_arith_reference_runs_on_the_short_case():
    case = step_case("decode", (8192,), (1,))
    short = short_case(case)
    assert short.context_lens[0] <= 256 + 1  # the history cap plus the one query
    step, _ = prepared(short)
    result = step.check_attention(step.make_query())
    assert result["passed"]
    assert result["max_rel_diff"] < 1e-2
