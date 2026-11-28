from __future__ import annotations

import asyncio

import pytest

from mooncake.reshard.weight.streaming import WeightStreamCoordinator


def publish(coordinator, ordinal, *, generation=1):
    key = f"{generation}-{ordinal}"
    coordinator.publish(key, {"metadata": {"generation": generation, "writer_rank": 0}})
    coordinator.publish_chunk(generation, 0, ordinal, f"token-{ordinal}", key)


def coordinator(*, slots=2, chunks=3):
    control = WeightStreamCoordinator(slot_count=slots)
    control.configure(["target-a", "target-b"], writer_world_size=1)
    control.begin_source(1, 0)
    control.stream_header(
        1,
        0,
        {
            "writer_rank": 0,
            "writer_world_size": 1,
            "chunks": chunks,
            "payload_bytes": chunks,
        },
    )
    return control


@pytest.mark.parametrize("slots", [1, 2, 4])
def test_backpressure_releases_only_after_all_consumers(slots):
    async def scenario():
        control = coordinator(slots=slots, chunks=slots + 1)
        for ordinal in range(slots):
            publish(control, ordinal)
        waiter = asyncio.create_task(control.wait_reusable(1, 0, slots))
        await asyncio.sleep(0)
        assert not waiter.done()
        control.ack_chunk(1, 0, 0, "token-0", "target-a")
        await asyncio.sleep(0)
        assert not waiter.done()
        control.ack_chunk(1, 0, 0, "token-0", "target-b")
        assert await waiter is True
        publish(control, slots)
        assert control.get_chunk(1, 0, slots)["bounded"]["slot"] == 0
        with pytest.raises(ValueError, match="retired"):
            control.get_chunk(1, 0, 0)
        with pytest.raises(ValueError, match="chunk CUDA"):
            control.acknowledge(1, "target-a")

    asyncio.run(scenario())


def test_chunk_release_and_framework_generation_completion_are_distinct():
    control = coordinator(chunks=1)
    publish(control, 0)
    for target in ("target-a", "target-b"):
        control.ack_chunk(1, 0, 0, "token-0", target)
    assert control.reusable(1, 0, 2)
    with pytest.raises(ValueError, match="still leased"):
        control.begin_source(2, 0)
    control.acknowledge(1, "target-a")
    with pytest.raises(ValueError, match="still leased"):
        control.begin_source(2, 0)
    control.acknowledge(1, "target-b")
    control.begin_source(2, 0)
    assert control.state()["generation"] == 2


def test_failure_is_sticky_for_slot_reuse_and_ack():
    control = coordinator()
    publish(control, 0)
    control.fail_source(1, 0, "uncertain completion")
    for operation in (
        lambda: control.reusable(1, 0, 1),
        lambda: control.ack_chunk(1, 0, 0, "token-0", "target-a"),
        lambda: control.acknowledge(1, "target-a"),
    ):
        with pytest.raises(RuntimeError, match="uncertain completion"):
            operation()
    with pytest.raises(ValueError, match="still leased"):
        control.begin_source(2, 0)


def test_source_failure_wakes_blocked_producer_without_releasing_slot():
    async def scenario():
        control = coordinator(slots=1, chunks=2)
        publish(control, 0)
        waiter = asyncio.create_task(control.wait_reusable(1, 0, 1))
        await asyncio.sleep(0)
        assert not waiter.done()
        control.fail_source(1, 0, "uncertain completion")
        with pytest.raises(RuntimeError, match="uncertain completion"):
            await asyncio.wait_for(waiter, timeout=1)
        with pytest.raises(RuntimeError, match="uncertain completion"):
            control.reusable(1, 0, 1)
        with pytest.raises(RuntimeError, match="uncertain completion"):
            control.ack_chunk(1, 0, 0, "token-0", "target-a")
        with pytest.raises(ValueError, match="still leased"):
            control.begin_source(2, 0)

    asyncio.run(scenario())


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "2"])
def test_invalid_slot_count(value):
    with pytest.raises(ValueError, match="slot count"):
        WeightStreamCoordinator(slot_count=value)


def test_duplicate_and_stale_ack_are_rejected_without_releasing_slot():
    control = coordinator()
    publish(control, 0)
    with pytest.raises(ValueError, match="Stale or duplicate"):
        control.ack_chunk(1, 0, 0, "wrong", "target-a")
    control.ack_chunk(1, 0, 0, "token-0", "target-a")
    with pytest.raises(ValueError, match="Stale or duplicate"):
        control.ack_chunk(1, 0, 0, "token-0", "target-a")
    assert not control.reusable(1, 0, 2)


def test_empty_writer_and_known_source_world():
    control = WeightStreamCoordinator()
    control.configure(["target"], writer_world_size=2)
    for writer in (0, 1):
        control.begin_source(1, writer)
        control.stream_header(
            1,
            writer,
            {
                "writer_rank": writer,
                "writer_world_size": 2,
                "chunks": 0,
                "payload_bytes": 0,
            },
        )
        if writer == 0:
            with pytest.raises(ValueError, match="Missing source writers"):
                control.acknowledge(1, "target")
    assert control.acknowledge(1, "target") == 1
