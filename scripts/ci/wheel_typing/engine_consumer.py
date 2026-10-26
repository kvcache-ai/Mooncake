"""Downstream view of ``mooncake.engine``, type-checked against an installed wheel.

This file is only analyzed by mypy (``--strict``) and is never executed.
The ``type: ignore[...]`` lines are negative checks: ``--strict`` enables
``warn_unused_ignores``, so they fail if the stub stops rejecting bad calls.
"""

from __future__ import annotations

from array import array

from typing_extensions import assert_type

from mooncake.engine import (
    ReceivedTensor,
    RpcInterface,
    ScatterTransferCompletionStatus,
    ScatterTransferTicket,
    SharedSegment,
    TransferEngine,
    TransferNotify,
    TransferOpcode,
    create_rpc_client,
)

optional_engine: TransferEngine | None = None


def exercise_transfer_engine(engine: TransferEngine) -> None:
    assert_type(engine.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", ""), int)
    assert_type(engine.get_rpc_port(), int)

    buffer = engine.allocate_managed_buffer(4096)
    assert_type(buffer, int)
    assert_type(engine.register_memory(buffer, 4096), int)
    assert_type(engine.batch_register_memory([buffer], [4096]), int)

    assert_type(engine.transfer_sync_write("peer:1", buffer, 0, 4096), int)
    assert_type(engine.transfer_sync_read("peer:1", buffer, 0, 4096), int)
    assert_type(engine.batch_transfer_sync_write("peer:1", [buffer], [0], [4096]), int)
    batch_id = engine.batch_transfer_async_read("peer:1", (buffer,), (0,), (4096,))
    assert_type(engine.get_batch_transfer_status([batch_id]), int)
    assert_type(engine.transfer_check_status(batch_id), int)

    assert_type(
        engine.transfer_sync(
            "peer:1",
            buffer,
            0,
            4096,
            TransferOpcode.Write,
            TransferNotify("name", "msg"),
        ),
        int,
    )
    assert_type(
        engine.batch_transfer_async(
            "peer:1", [buffer], [0], [4096], TransferEngine.TransferOpcode.Read
        ),
        int,
    )

    ticket = engine.scatter_transfer_sync_write_with_ticket(
        "peer:1", [buffer], [4096], [0], [4096], [[0]], [[0]], [[4096]]
    )
    assert_type(ticket, ScatterTransferTicket)
    assert_type(ticket.drain(1000), ScatterTransferCompletionStatus)
    assert_type(ticket.drained, bool)

    addresses = array("Q", [buffer])
    assert_type(
        engine.batch_transfer_sync_write_arrays(
            "peer:1", addresses, array("Q", [0]), array("Q", [4096])
        ),
        int,
    )
    engine.batch_transfer_sync_read_arrays("peer:1", [buffer], [0], [4096])  # type: ignore[arg-type]

    assert_type(engine.write_bytes_to_buffer(buffer, b"payload", 7), int)
    assert_type(engine.read_bytes_from_buffer(buffer, 7), bytes)
    assert_type(engine.get_notifies(), list[TransferNotify])
    assert_type(engine.get_local_topology(), str)
    assert_type(engine.send_probe("peer:1"), int)
    assert_type(engine.get_engine_ptr(), int)
    assert_type(engine.unregister_memory(buffer), int)
    assert_type(engine.free_managed_buffer(buffer, 4096), int)

    engine.transfer_sync_write("peer:1", "0x0", 0, 4096)  # type: ignore[arg-type]
    engine.transfer_sync("peer:1", buffer, 0, 4096, 1)  # type: ignore[arg-type]
    engine.not_a_binding()  # type: ignore[attr-defined]


def exercise_shared_segment() -> None:
    segment, blob = SharedSegment.create("segment", 1 << 20, world_size=2, rank_id=0)
    assert_type(segment, SharedSegment)
    assert_type(blob, bytes)
    segment.complete([blob, blob])
    assert_type(segment.ready(), bool)
    assert_type(segment.base_addr(), int)
    assert_type(SharedSegment.supported(), bool)


def exercise_rpc_interface() -> None:
    client = create_rpc_client()
    assert_type(client, RpcInterface)

    def on_tensor(tensor: ReceivedTensor) -> None:
        assert_type(tensor.shape, list[int])

    client.set_tensor_receive_callback(on_tensor)
    assert_type(client.send_data("127.0.0.1:8080", b"payload"), int)
