"""TCPStore helpers shared by Mooncake EP buffer implementations."""

import pickle
import socket
from contextlib import contextmanager
from datetime import timedelta
from typing import Any, Iterator, List, Tuple


@contextmanager
def fetch_peer_metadata(
    store: Any,
    rank: int,
    remote_ranks: List[int],
    peer_metadata: Tuple[Any, ...],
    timeout_ms: int,
) -> Iterator[List[Tuple[str, Any]]]:
    """Exchange legacy ``MOONCAKE_EP/{src}/{dst}`` envelopes unchanged."""
    if store is None:
        raise RuntimeError("Mooncake peer metadata exchange requires a TCPStore.")

    outgoing_keys = [f"MOONCAKE_EP/{rank}/{remote}" for remote in remote_ranks]
    incoming_keys = [f"MOONCAKE_EP/{remote}/{rank}" for remote in remote_ranks]
    envelope = pickle.dumps((socket.gethostname(), peer_metadata))
    consumed_incoming_keys = []
    try:
        for key in outgoing_keys:
            store.set(key, envelope)
        if incoming_keys:
            store.wait(incoming_keys, timedelta(milliseconds=timeout_ms))
            remote_metadata = store.multi_get(incoming_keys)
            consumed_incoming_keys.extend(incoming_keys)
        else:
            remote_metadata = []
        yield [pickle.loads(metadata) for metadata in remote_metadata]
    finally:
        for key in consumed_incoming_keys:
            store.delete_key(key)


class TCPStoreCollectives:
    """Small fixed-world Store collectives with per-instance, per-round keys."""

    def __init__(
        self,
        store: Any,
        rank: int,
        world_size: int,
        namespace: str,
        timeout_secs: int,
    ) -> None:
        if not namespace:
            raise ValueError("store_namespace must be a non-empty string.")
        if timeout_secs <= 0:
            raise ValueError("store_timeout_secs must be positive.")
        if world_size <= 0 or not 0 <= rank < world_size:
            raise ValueError("rank must be within the positive Store world size.")
        if store is None:
            raise RuntimeError("ElasticBuffer.create_elastic requires a TCPStore.")

        self.store = store
        self.rank = rank
        self.world_size = world_size
        self.namespace = namespace
        self.timeout = timedelta(seconds=timeout_secs)
        self._round = 0

    def all_gather(self, operation: str, value: Any) -> List[Any]:
        round_idx = self._round
        self._round += 1
        round_key = f"MOONCAKE_EP_ELASTIC/{self.namespace}/{round_idx}"
        payload_keys = [f"{round_key}/rank/{rank}" for rank in range(self.world_size)]
        ack_key = f"{round_key}/readers"

        self.store.set(
            payload_keys[self.rank], pickle.dumps((operation, self.world_size, value))
        )
        self.store.wait(payload_keys, self.timeout)
        records = [pickle.loads(item) for item in self.store.multi_get(payload_keys)]
        operations = {record[0] for record in records}
        world_sizes = {record[1] for record in records}
        if operations != {operation}:
            raise RuntimeError(
                "ElasticBuffer Store collective order differs across ranks: "
                f"expected {operation!r}, got {sorted(operations)!r}"
            )
        if world_sizes != {self.world_size}:
            raise RuntimeError(
                "ElasticBuffer Store world sizes differ across ranks: "
                f"expected {self.world_size}, got {sorted(world_sizes)!r}"
            )

        values = [record[2] for record in records]
        if self.store.add(ack_key, 1) == self.world_size:
            for key in payload_keys:
                self.store.delete_key(key)
            self.store.delete_key(ack_key)
        return values

    def broadcast(self, operation: str, value: Any, root: int = 0) -> Any:
        if not 0 <= root < self.world_size:
            raise ValueError("broadcast root is outside the Store world.")
        values = self.all_gather(
            operation,
            value if self.rank == root else None,
        )
        return values[root]

    def barrier(self, operation: str = "barrier") -> None:
        self.all_gather(operation, None)
