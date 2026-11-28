"""Framework-neutral bounded source slots and weight-generation completion.

Consumers call ``ack_chunk`` only after the SDK transfer and any asynchronous
consumption of that chunk have completed. A chunk acknowledgement permits
source-slot reuse; ``acknowledge`` is separate and follows framework post-load
completion. This coordinator carries metadata only and does not establish GPU
fences, retain allocations, or make overlapping source/target writes safe.

Host it on one serialized event loop. A framework may wrap it in an actor, but
neither Ray nor a particular model/runtime is required by this module.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Iterable, Mapping
from typing import Any, cast


class WeightStreamCoordinator:
    """Coordinate bounded publication and completion for one consumer fleet."""

    def __init__(self, timeout_seconds: float = 300, slot_count: int = 2) -> None:
        if type(slot_count) is not int or slot_count <= 0:
            raise ValueError("Source slot count must be a positive integer")
        self.slot_count = slot_count
        self.objects: dict[str, Mapping[str, Any]] = {}
        self.slots: dict[str, Any] = {}
        self.consumers: set[str] | None = None
        self.writer_world_size: int | None = None
        self.generation = 0
        self.acks: set[str] = set()
        self.source_writers: set[int] = set()
        self.timeout_seconds = timeout_seconds
        self.headers: dict[tuple[int, int], Mapping[str, Any]] = {}
        self.chunks: dict[tuple[int, int, int], dict[str, Any]] = {}
        self.bound_objects: set[str] = set()
        self.failures: dict[tuple[int, int], str] = {}
        self.reuse_events: dict[tuple[int, int, int], asyncio.Event] = {}
        self.slot_reuses = 0

    def configure(
        self, consumers: Iterable[str], writer_world_size: int | None = None
    ) -> None:
        consumers = tuple(consumers)
        if writer_world_size is not None and (
            type(writer_world_size) is not int or writer_world_size <= 0
        ):
            raise ValueError("Invalid source world size")
        if self.consumers is not None or len(set(consumers)) != len(consumers):
            raise ValueError("Consumer identities must be configured once")
        if not consumers or any(not isinstance(c, str) or not c for c in consumers):
            raise ValueError("Consumer identities must be nonempty strings")
        self.consumers = set(consumers)
        self.writer_world_size = writer_world_size

    def begin_source(self, generation: int, writer: int) -> None:
        if (
            type(generation) is not int
            or generation < 1
            or type(writer) is not int
            or writer < 0
        ):
            raise ValueError("Invalid source identity")
        if not self.consumers or generation < self.generation:
            raise ValueError("Unconfigured or stale publication")
        if generation > self.generation:
            if generation != self.generation + 1 or (
                self.generation and self.acks != self.consumers
            ):
                raise ValueError("Previous GPU generation still leased")
            self.objects.clear()
            self.slots.clear()
            self.headers.clear()
            self.chunks.clear()
            self.bound_objects.clear()
            self.failures.clear()
            self.reuse_events.clear()
            self.slot_reuses = 0
            self.generation = generation
            self.acks = set()
            self.source_writers = set()
        if self.writer_world_size is not None and writer >= self.writer_world_size:
            raise ValueError("Writer outside configured source world")
        if self.acks:
            raise ValueError("Cannot acquire new writer after final consumer ACK")
        if writer in self.source_writers:
            raise ValueError("Source epoch already acquired")
        self.source_writers.add(writer)

    def publish(self, key: str, value: Mapping[str, Any]) -> None:
        metadata = value["metadata"]
        if (
            metadata["generation"] != self.generation
            or metadata["writer_rank"] not in self.source_writers
        ):
            raise ValueError("Publication requires an acquired source epoch")
        if key in self.objects:
            raise ValueError("Publication is immutable")
        self.objects[key] = copy.deepcopy(value)

    def describe(self, key: str) -> Mapping[str, Any]:
        value = self.objects[key]
        if value["metadata"]["generation"] != self.generation:
            raise ValueError("Retired source generation")
        return copy.deepcopy(value)

    def put(self, key: str, value: Any) -> None:
        if key in self.slots:
            raise ValueError("Manifest slot already published")
        self.slots[key] = value

    def get_batch(self, keys: Iterable[str]) -> list[Any]:
        return [self.slots.get(key) for key in keys]

    def _acknowledge_generation(self, generation: int, consumer: str) -> int:
        if (
            generation != self.generation
            or consumer not in cast(set[str], self.consumers)
            or consumer in self.acks
        ):
            raise ValueError("Invalid GPU completion acknowledgement")
        self.acks.add(consumer)
        return len(self.acks)

    def _generation_state(self) -> dict[str, Any]:
        return dict(
            generation=self.generation,
            consumers=sorted(self.consumers or []),
            acknowledgements=sorted(self.acks),
            objects=len(self.objects),
        )

    def stream_header(
        self, generation: int, writer: int, header: Mapping[str, Any]
    ) -> None:
        if generation != self.generation or writer not in self.source_writers:
            raise ValueError("Stream header requires acquired source generation")
        key = (generation, writer)
        if (
            key in self.headers
            or type(header.get("chunks")) is not int
            or type(header.get("payload_bytes")) is not int
            or header["chunks"] < 0
            or header["payload_bytes"] < 0
            or (header["chunks"] == 0) != (header["payload_bytes"] == 0)
        ):
            raise ValueError("Invalid or duplicate stream header")
        if header.get("writer_rank") != writer:
            raise ValueError("Header writer mismatch")
        world = header.get("writer_world_size")
        if world is not None:
            self._check_world_size(world)
        self.headers[key] = copy.deepcopy(header)

    def get_headers(
        self, generation: int, writers: int
    ) -> list[Mapping[str, Any] | None]:
        if generation != self.generation:
            raise ValueError("Retired stream generation")
        self._check_world_size(writers)
        self.check_failure(generation)
        return [self.headers.get((generation, writer)) for writer in range(writers)]

    def _check_world_size(self, writers: int) -> None:
        if type(writers) is not int or writers <= 0:
            raise ValueError("Invalid source world size")
        if self.writer_world_size is None:
            if any(w >= writers for w in self.source_writers):
                raise ValueError("Existing writer outside source world")
            self.writer_world_size = writers
        elif self.writer_world_size != writers:
            raise ValueError("Source world size changed")

    def fail_source(self, generation: int, writer: int, error: str) -> None:
        if generation != self.generation or writer not in self.source_writers:
            raise ValueError("Stale source failure")
        self.failures[(generation, writer)] = error
        for (g, _, _), event in self.reuse_events.items():
            if g == generation:
                event.set()

    def check_failure(self, generation: int) -> None:
        failures = {
            writer: error
            for (g, writer), error in self.failures.items()
            if g == generation
        }
        if failures:
            raise RuntimeError(
                f"Bounded source failed; allocations remain leased: {failures}"
            )

    def reusable(self, generation: int, writer: int, ordinal: int) -> bool:
        self.check_failure(generation)
        if generation != self.generation or writer not in self.source_writers:
            raise ValueError("Stale stream slot request")
        if type(ordinal) is not int or ordinal < 0:
            raise ValueError("Invalid chunk ordinal")
        previous = self.chunks.get((generation, writer, ordinal - self.slot_count))
        return ordinal < self.slot_count or (
            previous is not None and previous["acks"] == self.consumers
        )

    async def wait_reusable(self, generation: int, writer: int, ordinal: int) -> bool:
        if self.reusable(generation, writer, ordinal):
            return True
        key = (generation, writer, ordinal - self.slot_count)
        event = self.reuse_events.setdefault(key, asyncio.Event())
        await asyncio.wait_for(event.wait(), timeout=self.timeout_seconds)
        self.check_failure(generation)
        if not self.reusable(generation, writer, ordinal):
            raise RuntimeError("Stream completion did not release the source slot")
        return True

    def publish_chunk(
        self, generation: int, writer: int, ordinal: int, token: str, object_key: str
    ) -> None:
        self.check_failure(generation)
        if generation != self.generation or writer not in self.source_writers:
            raise ValueError("Stale stream publication")
        header = self.headers[(generation, writer)]
        key = (generation, writer, ordinal)
        if (
            not 0 <= ordinal < header["chunks"]
            or key in self.chunks
            or not isinstance(token, str)
            or not token
            or not self.reusable(generation, writer, ordinal)
        ):
            raise ValueError("Chunk slot is not exclusively reusable")
        if object_key in self.bound_objects:
            raise ValueError("A publication cannot be rebound to another chunk")
        value = dict(self.objects[object_key])
        if (
            value["metadata"]["generation"] != generation
            or value["metadata"]["writer_rank"] != writer
        ):
            raise ValueError("Chunk publication identity mismatch")
        value["bounded"] = dict(
            writer_rank=writer,
            ordinal=ordinal,
            token=token,
            slot=ordinal % self.slot_count,
        )
        self.chunks[key] = dict(value=value, acks=set[str]())
        self.bound_objects.add(object_key)
        self.slot_reuses += int(ordinal >= self.slot_count)

    def get_chunk(
        self,
        generation: int,
        writer: int,
        ordinal: int,
        known_digest: str | None = None,
    ) -> dict[str, Any] | None:
        self.check_failure(generation)
        if generation != self.generation:
            raise ValueError("Retired stream generation")
        if (generation, writer, ordinal + self.slot_count) in self.chunks:
            raise ValueError("Stream slot lease has retired")
        entry = self.chunks.get((generation, writer, ordinal))
        if entry is None:
            return None
        result = copy.deepcopy(entry["value"])
        if (
            known_digest is not None
            and result["metadata"].get("static_digest") == known_digest
        ):
            result["metadata"].pop("record_table", None)
        return result

    def ack_chunk(
        self, generation: int, writer: int, ordinal: int, token: str, consumer: str
    ) -> bool:
        self.check_failure(generation)
        if generation != self.generation or consumer not in cast(
            set[str], self.consumers
        ):
            raise ValueError("Invalid stream consumer generation")
        entry = self.chunks[(generation, writer, ordinal)]
        if entry["value"]["bounded"]["token"] != token or consumer in entry["acks"]:
            raise ValueError("Stale or duplicate chunk completion")
        entry["acks"].add(consumer)
        if entry["acks"] == self.consumers:
            event = self.reuse_events.get((generation, writer, ordinal))
            if event is not None:
                event.set()
        return True

    def acknowledge(self, generation: int, consumer: str) -> int:
        self.check_failure(generation)
        if self.writer_world_size is not None and self.source_writers != set(
            range(self.writer_world_size)
        ):
            raise ValueError("Missing source writers at final completion")
        headers = [h for (g, _), h in self.headers.items() if g == generation]
        if not headers or len(headers) != len(self.source_writers):
            raise ValueError("Missing source headers at final completion")
        for header in headers:
            for ordinal in range(header["chunks"]):
                entry = self.chunks.get((generation, header["writer_rank"], ordinal))
                if entry is None or consumer not in entry["acks"]:
                    raise ValueError("Final completion precedes chunk CUDA completion")
        return self._acknowledge_generation(generation, consumer)

    def state(self) -> dict[str, Any]:
        entries = [
            entry for (g, _, _), entry in self.chunks.items() if g == self.generation
        ]
        return dict(
            self._generation_state(),
            bounded_chunks_published=len(entries),
            bounded_chunks_acked=sum(
                entry["acks"] == self.consumers for entry in entries
            ),
            full_consumer_coverage=bool(entries)
            and all(entry["acks"] == self.consumers for entry in entries),
            slot_reuses=self.slot_reuses,
            unsafe_reuse=0,
        )
