"""Part backend adapter for execution with explicitly supplied manifests.

The method names below are an adapter contract, not APIs supplied by the
upstream Store client today. A compatible Part implementation is required.
It must bind every object to an immutable manifest identity and Part count,
reject incompatible partial writes, and exclude unavailable or mismatched
objects from read snapshots. Snapshot aliases identify ``(page key, Part)``;
they are not additional Store keys. Reads must not silently re-query an alias
or substitute a new object after the snapshot is prepared.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .multipart import MultipartReadContext


class KVCacheStoreError(RuntimeError):
    """A Part backend operation or result contract failed."""


PartTemplateRead = Callable[
    [Sequence[object], Sequence[int], Sequence[int], Sequence[str], Sequence[int]],
    Sequence[bool],
]


class MultipartBackend:
    def __init__(self, store: Any) -> None:
        self.store = store
        for name in (
            "batch_put_parts_from",
            "prepare_get_parts_snapshot",
            "prepare_get_into_ranges_template",
            "get_into_ranges_from_template",
        ):
            if not callable(getattr(store, name, None)):
                raise KVCacheStoreError(
                    f"Part backend lacks required capability: {name}"
                )

    def put_parts(
        self,
        page_keys: Sequence[str],
        part_index: int,
        part_count: int,
        manifest_key: str,
        pointers: Sequence[Sequence[int]],
        lengths: Sequence[Sequence[int]],
    ) -> tuple[bool, ...]:
        """Return one status per page: zero succeeds, including a safe duplicate."""
        results: Sequence[int] = self.store.batch_put_parts_from(
            page_keys, part_index, part_count, manifest_key, pointers, lengths
        )
        if len(results) != len(page_keys) or any(type(v) is not int for v in results):
            raise KVCacheStoreError("Part upload result shape differs")
        return tuple(status == 0 for status in results)

    def prepare_template(
        self, destinations: Sequence[int], sources: Sequence[int], sizes: Sequence[int]
    ) -> object:
        return self.store.prepare_get_into_ranges_template(destinations, sources, sizes)

    def ranged_reader(self, context: MultipartReadContext) -> PartTemplateRead:
        manifests = {m.layout.layout_id: m for m in context.manifests}
        sources = [manifests[i] for i in context.layout_ids]
        snapshot: object | None = None

        def read(
            templates: Sequence[object],
            buffers: Sequence[int],
            indices: Sequence[int],
            aliases: Sequence[str],
            offsets: Sequence[int],
        ) -> Sequence[bool]:
            nonlocal snapshot
            if snapshot is None:
                snapshot = self.store.prepare_get_parts_snapshot(
                    context.page_keys,
                    [m.manifest_key for m in sources],
                    [len(m.layout.shards) for m in sources],
                )
            return self.store.get_into_ranges_from_template(
                snapshot, templates, buffers, indices, aliases, offsets
            )

        return read
