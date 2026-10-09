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
    BRANCH_PAGED_DECODE,
    BRANCH_PAGED_EXTEND,
    BRANCH_RAGGED_NO_PREFIX,
    BRANCH_RAGGED_PREFIX_MERGE,
    KernelCase,
    short_case,
)
from benchmarks.sglang_attention_kv import kernel_bench  # noqa: E402
from benchmarks.sglang_attention_kv.kernel_bench import (  # noqa: E402
    PHASES,
    STEP_PHASES,
    derive,
    one_attention_component,
    one_iteration,
    run_case,
)
from benchmarks.sglang_attention_kv.sglang_replay import SglangStep  # noqa: E402

DEVICE = "cuda:0"


def step_case(mode, prefix_lens, new_lens, page_size=64, layout="random", layers=2):
    return KernelCase(
        mode=mode,
        prefix_lens=prefix_lens,
        new_lens=new_lens,
        page_size=page_size,
        layout=layout,
        num_layers=layers,
        num_qo_heads=8,
        num_kv_heads=2,
        head_dim=128,
        dtype="bfloat16",
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
    assert step.check_history(indices)["passed"]
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
    assert step.check_history(indices)["passed"]
    assert step.check_attention(step.make_query())["passed"]


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


def test_each_mode_replays_its_branch():
    assert (
        SglangStep(step_case("prefill", (0,), (128,)), DEVICE, seed=1).branch
        == BRANCH_RAGGED_NO_PREFIX
    )
    assert (
        SglangStep(step_case("extend", (512,), (128,)), DEVICE, seed=1).branch
        == BRANCH_RAGGED_PREFIX_MERGE
    )
    assert (
        SglangStep(step_case("decode", (512,), (1,)), DEVICE, seed=1).branch
        == BRANCH_PAGED_DECODE
    )


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


def test_a_second_index_build_names_the_same_slots():
    """The page table a step reads has to be the one its KV was written through,
    so building the index stream twice must give the same stream."""
    step, first = prepared(step_case("decode", (512,), (1,)))
    second = step.build_indices()
    assert first.paged_indices.long().tolist() == second.paged_indices.long().tolist()
    assert step.check_history(second)["passed"]


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
    result = step.check_history(indices)
    assert result["mismatched_k_elements"] == 0
    assert result["mismatched_v_elements"] == 0
    assert result["tokens_checked"] == 769 * 3


def test_a_corrupted_v_is_caught():
    """The content check has to cover V as well as K: a V that was never written
    where the token table points is a broken cache, not a passing check."""
    step, indices = prepared(step_case("decode", (512,), (1,), layers=2))
    assert step.check_history(indices)["passed"]

    _, value_buffer = step.kv_pool.get_kv_buffer(1)
    row = int(step.token_pool.req_to_token[int(step.req_pool_indices[0]), 7])
    value_buffer[row] = 0.0
    torch.cuda.synchronize()

    result = step.check_history(indices)
    assert not result["passed"]
    assert result["mismatched_v_elements"] > 0


def test_a_corrupted_k_is_caught():
    step, indices = prepared(step_case("decode", (512,), (1,), layers=2))
    key_buffer, _ = step.kv_pool.get_kv_buffer(1)
    row = int(step.token_pool.req_to_token[int(step.req_pool_indices[0]), 7])
    key_buffer[row] = 0.0
    torch.cuda.synchronize()

    result = step.check_history(indices)
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


def test_the_gather_moves_both_k_and_v():
    step, indices = prepared(step_case("decode", (128,), (1,)))
    rows = step.gather_rows(indices)
    last_layer = step.case.num_layers - 1
    key_buffer, value_buffer = step.kv_pool.get_kv_buffer(last_layer)
    moved = step.gather(indices)
    assert torch.equal(moved, value_buffer.index_select(0, rows))
    assert not torch.equal(moved, key_buffer.index_select(0, rows))


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
    assert step.check_history(indices)["passed"]


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


def test_the_attention_rate_divides_the_bytes_the_branch_reads():
    """The rate a row reports divides the KV the attention window covers. A derive
    that divided the page capacity instead would report a different number, so this
    fails if the ledger drifts back to the allocation figure."""
    step, _ = prepared(step_case("extend", (500,), (100,)))
    case = step.case
    read = step.read_bytes()
    query = step.make_query()
    attention_ms = one_attention_component(step, query)
    phases = {name: {"p50": attention_ms} for name in PHASES}
    indices = step.build_indices()
    derived = derive(case, phases, step.gather_bytes(indices), read["attention"])
    per_second = attention_ms / 1000.0
    assert derived["attention_unique_payload_gbps"] == pytest.approx(
        read["attention"] / per_second / 1e9, rel=1e-12
    )
    assert derived["attention_flops_per_unique_payload_byte"] == pytest.approx(
        case.attention_flops() / read["attention"], rel=1e-12
    )
    capacity_rate = case.kv_page_capacity_bytes() / per_second / 1e9
    assert derived["attention_unique_payload_gbps"] != pytest.approx(
        capacity_rate, rel=1e-3
    )


def test_the_step_has_a_window_of_its_own_beside_the_phase_sum(monkeypatch):
    """The three windows are timed separately, so phase_sum is their sum and not
    the step: the device can be idle between them while the host issues the next
    window's calls. step_window measures the whole step in one span."""
    step, _ = prepared(step_case("extend", (256,), (128,)))
    measured = one_iteration(step, step.make_query())
    assert set(measured) == set(STEP_PHASES) | {"step_window", "phase_sum"}
    assert measured["phase_sum"] == pytest.approx(
        sum(measured[name] for name in STEP_PHASES), rel=1e-12
    )
    assert measured["step_window"] >= measured["phase_sum"]


def test_the_component_passes_run_against_a_plan_of_their_own_indices(monkeypatch):
    """The components read the indices they built, so they have to run against a
    plan of those and not the metadata the last timed iteration left behind. The
    plan is taken outside every window, so it is not part of what a component
    measures."""
    case = short_case(step_case("extend", (256,), (128,)))
    order = []
    original_plan = SglangStep.plan

    def plan(self, indices):
        order.append("plan")
        return original_plan(self, indices)

    def iteration(step, query):
        order.append("iteration")
        return original_iteration(step, query)

    def write_component(step, indices):
        order.append("write_component")
        return original_write(step, indices)

    def attention_component(step, query):
        order.append("attention_component")
        return original_attention(step, query)

    def gather(step, indices):
        order.append("gather")
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
    # Each timed iteration plans for its own indices inside its window. The plan
    # that follows them is the one the component passes run against, and it comes
    # before the first component window. The correctness check builds a short step
    # of its own afterwards, which is why only this prefix is compared.
    expected = ["iteration", "plan"] * 3 + [
        "plan",
        "write_component",
        "write_component",
        "attention_component",
        "attention_component",
        "gather",
        "gather",
    ]
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
    measured = one_iteration(step, step.make_query())
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
    assert step.check_history(indices)["passed"]


def test_the_arith_reference_runs_on_the_short_case():
    case = step_case("decode", (8192,), (1,))
    short = short_case(case)
    assert short.context_lens[0] <= 256 + 1  # the history cap plus the one query
    step, _ = prepared(short)
    result = step.check_attention(step.make_query())
    assert result["passed"]
    assert result["max_rel_diff"] < 1e-2
