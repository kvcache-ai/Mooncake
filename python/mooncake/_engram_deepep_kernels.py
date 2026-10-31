"""GPU index translation and byte output assembly; RDMA is handled by DeepEP."""

import triton
import triton.language as tl


@triton.jit
def route(
    IDS,
    META,
    MAPPED,
    REMOTE,
    count: tl.constexpr,
    heads: tl.constexpr,
    s0: tl.constexpr,
    s1: tl.constexpr,
    slot: tl.constexpr,
    shard: tl.constexpr,
    B: tl.constexpr,
):
    i = tl.program_id(0) * B + tl.arange(0, B)
    column = i % heads
    value = tl.load(IDS + i // heads * s0 + column * s1, i < count, -1).to(tl.int64)
    owner = tl.load(META + column)
    start = tl.load(META + heads + column)
    rows = tl.load(META + 2 * heads + column)
    offset = tl.load(META + 3 * heads + column)
    valid = (value >= offset) & (value - offset < rows)
    tl.device_assert((i >= count) | (value == -1) | valid, "Engram row out of bounds")
    mapped = tl.where(valid, owner * slot + start + value - offset, -1)
    tl.store(MAPPED + i, mapped, i < count)
    tl.store(REMOTE + i, tl.where(owner != shard, mapped, -1), i < count)


@triton.jit
def finish(
    MAPPED,
    LOCAL,
    FETCHED,
    OUT,
    count: tl.constexpr,
    heads: tl.constexpr,
    width: tl.constexpr,
    stride: tl.constexpr,
    slot: tl.constexpr,
    shard: tl.constexpr,
    o0: tl.constexpr,
    o1: tl.constexpr,
    B: tl.constexpr,
):
    LOCAL = LOCAL.to(tl.pointer_type(tl.uint8))
    row = tl.program_id(0)
    b = tl.arange(0, B)
    mapped = tl.load(MAPPED + row)
    live = mapped >= 0
    local = live & (mapped // slot == shard)
    a = tl.load(LOCAL + (mapped % slot) * stride + b, local & (b < width), 0)
    r = tl.load(FETCHED + row * stride + b, live & ~local & (b < width), 0)
    tl.store(
        OUT + row // heads * o0 + row % heads * o1 + b,
        tl.where(local, a, r),
        live & (b < width),
    )
