"""Exercise the task-state API from an installed Mooncake wheel."""

import json
import tempfile
from pathlib import Path

import mooncake.tent as mooncake_tent
import tent


assert tent.TaskCongestionState is mooncake_tent.TaskCongestionState
assert tent.TransferEngine is mooncake_tent.TransferEngine

with tempfile.TemporaryDirectory() as directory:
    config = Path(directory) / "tent.json"
    config.write_text(
        json.dumps(
            {
                "metadata_type": "p2p",
                "rpc_server_hostname": "127.0.0.1",
                "rpc_server_port": 0,
                "transports": {
                    "tcp": {"enable": True},
                    "hp_tcp": {"enable": False},
                    "rdma": {"enable": False},
                    "shm": {"enable": False},
                },
            }
        )
    )
    engine = tent.TransferEngine(str(config))
    assert engine.available()
    with engine.allocate_memory_guard(4096) as address:
        engine.register_local_memory(address, 4096)
        try:
            batch_id = engine.allocate_transfer_batch(1)
            try:
                request = tent.Request(
                    tent.OpCode.WRITE,
                    address,
                    tent.LOCAL_SEGMENT_ID,
                    address,
                    64,
                    transport_hint=tent.TransportType.TCP,
                )
                engine.submit_transfer(batch_id, [request])
                state = engine.get_task_congestion_state(batch_id, 0)
                detail = engine.get_task_congestion_detail(batch_id, 0)
                assert state == tent.TaskCongestionState.UNKNOWN
                assert detail["state"] == state
                assert detail["attempt_kind"] == tent.TaskCongestionAttemptKind.OTHER
                assert detail["reason"] is None
            finally:
                engine.free_transfer_batch(batch_id)
        finally:
            engine.unregister_local_memory(address, 4096)

print("installed TENT task-state API: OK")
