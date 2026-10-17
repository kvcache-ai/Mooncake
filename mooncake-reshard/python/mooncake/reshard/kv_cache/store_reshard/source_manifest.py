"""Source layout manifest: the stored description of how a page was written.

One manifest describes one source layout for a fixed page size and is
shared by every page written with that layout. It carries the model's
semantic identity, the global KV descriptor, and the layer set plus
KV-head interval covered by each Part. Byte geometry (part sizes, K/V
offsets, strides) is derived from this description rather than stored, so
there is exactly one representation of the layout.

Manifests contain no page token hash, GPU pointer, process-local address
or replica address: only the layout, which stays valid after the writer
process exits.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from ..types import require_integer, require_integer_tuple
from .layout import KVCacheHostLayout
from .model_domain import ModelSemanticDomain

__all__ = [
    "SourcePartCoverage",
    "SourceLayoutManifest",
]


@dataclass(frozen=True)
class SourcePartCoverage:
    """The logical coverage of one Part, in global coordinates."""

    part_index: int
    layer_ids: tuple[int, ...]
    head_start: int
    head_count: int

    def __post_init__(self) -> None:
        require_integer(self.part_index, "part_index")
        layers = require_integer_tuple(self.layer_ids, "layer_ids")
        if not layers:
            raise ValueError("layer_ids must not be empty")
        if len(layers) != len(set(layers)):
            raise ValueError("layer_ids must be unique")
        object.__setattr__(self, "layer_ids", tuple(sorted(layers)))
        require_integer(self.head_start, "head_start")
        require_integer(self.head_count, "head_count", minimum=1)

    def contains_layer(self, global_layer_id: int) -> bool:
        return global_layer_id in self.layer_ids

    def contains_head(self, global_head_id: int) -> bool:
        return self.head_start <= global_head_id < self.head_start + self.head_count

    def canonical_content(self) -> dict[str, object]:
        return {
            "part_index": self.part_index,
            "layer_ids": list(self.layer_ids),
            "head_start": self.head_start,
            "head_count": self.head_count,
        }


@dataclass(frozen=True)
class SourceLayoutManifest:
    """Immutable, content-addressed description of one source layout."""

    domain: ModelSemanticDomain
    layout: KVCacheHostLayout
    parts: tuple[SourcePartCoverage, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.domain, ModelSemanticDomain):
            raise ValueError("domain must be a ModelSemanticDomain")  # noqa: TRY004
        if not isinstance(self.layout, KVCacheHostLayout):
            raise ValueError(  # noqa: TRY004
                "layout must be a KVCacheHostLayout"
            )
        parts = tuple(self.parts)
        if not parts:
            raise ValueError("parts must not be empty")
        for part in parts:
            if not isinstance(part, SourcePartCoverage):
                raise ValueError(  # noqa: TRY004
                    "parts must all be SourcePartCoverage"
                )
        indices = [part.part_index for part in parts]
        if sorted(indices) != list(range(len(parts))):
            raise ValueError("part_index values must be 0..N-1 exactly once")
        self._validate_coverage(parts)

    def _validate_coverage(self, parts: tuple[SourcePartCoverage, ...]) -> None:
        total_heads = self.domain.total_kv_heads
        # Every global layer must be covered by at least one part.
        for layer_id in self.domain.global_layer_ids:
            if not any(part.contains_layer(layer_id) for part in parts):
                raise ValueError(  # noqa: TRY003
                    f"no part covers global layer {layer_id}"
                )
        # No two parts may overlap: overlapping layer sets AND overlapping
        # head intervals would describe the same bytes twice.
        for index, left in enumerate(parts):
            for right in parts[index + 1 :]:
                if not set(left.layer_ids) & set(right.layer_ids):
                    continue
                if (
                    left.head_start < right.head_start + right.head_count
                    and right.head_start < left.head_start + left.head_count
                ):
                    raise ValueError(  # noqa: TRY003
                        f"parts {left.part_index} and {right.part_index} overlap "
                        "on the same (layer, head) cells"
                    )
        # The (layer, head) cells must also be fully covered: per layer, the
        # parts covering that layer tile the head axis exactly.
        for layer_id in self.domain.global_layer_ids:
            covering = self.parts_for_layer(layer_id)
            boundary = 0
            for part in covering:
                if part.head_start != boundary:
                    raise ValueError(  # noqa: TRY003
                        f"parts covering layer {layer_id} must tile the head "
                        "axis without gaps"
                    )
                boundary = part.head_start + part.head_count
            if boundary != total_heads:
                raise ValueError(  # noqa: TRY003
                    f"parts covering layer {layer_id} must tile the head axis "
                    "exactly"
                )

    @property
    def part_count(self) -> int:
        return len(self.parts)

    def part(self, part_index: int) -> SourcePartCoverage:
        for item in self.parts:
            if item.part_index == part_index:
                return item
        raise KeyError(f"unknown part_index: {part_index}")

    def covering_part(self, global_layer_id: int, global_head_id: int):
        """The part covering one (layer, head) coordinate."""

        for part in self.parts:
            if part.contains_layer(global_layer_id) and part.contains_head(
                global_head_id
            ):
                return part
        raise KeyError(f"no part covers layer={global_layer_id}, head={global_head_id}")

    def parts_for_layer(self, global_layer_id: int) -> tuple[SourcePartCoverage, ...]:
        """Parts covering one global layer, ascending by head_start.

        Layer-sharded writers (TP) yield one part with the full head axis;
        head-sharded writers yield several parts with disjoint intervals.
        """

        require_integer(global_layer_id, "global_layer_id")
        covering = tuple(
            part for part in self.parts if part.contains_layer(global_layer_id)
        )
        return tuple(sorted(covering, key=lambda item: item.head_start))

    def head_split(
        self, global_layer_id: int, head_start: int, head_count: int
    ) -> tuple[tuple[SourcePartCoverage, int, int], ...]:
        """Split a head interval by part boundaries for one layer.

        Returns (part, sub_start, sub_count) triples; an interval that spans
        several parts yields one triple per part, in ascending head order.
        """

        require_integer(global_layer_id, "global_layer_id")
        require_integer(head_start, "head_start")
        require_integer(head_count, "head_count", minimum=1)
        if head_start + head_count > self.domain.total_kv_heads:
            raise ValueError("head interval exceeds total_kv_heads")  # noqa: TRY003
        covering = self.parts_for_layer(global_layer_id)
        if not covering:
            raise KeyError(f"no part covers layer {global_layer_id}")
        splits: list[tuple[SourcePartCoverage, int, int]] = []
        cursor = head_start
        end = head_start + head_count
        for part in covering:
            if part.head_start >= end:
                break
            part_end = part.head_start + part.head_count
            if part_end <= cursor:
                continue
            sub_start = max(cursor, part.head_start)
            sub_end = min(end, part_end)
            splits.append((part, sub_start, sub_end - sub_start))
            cursor = sub_end
            if cursor >= end:
                break
        if cursor != end:
            raise ValueError(  # noqa: TRY003
                "head interval is not fully covered by parts"
            )
        return tuple(splits)

    def canonical_content(self) -> dict[str, object]:
        return {
            "schema": "kv-cache-source-layout-manifest",
            "domain": self.domain.canonical_content(),
            "layout": self.layout.value,
            "parts": [part.canonical_content() for part in self.parts],
        }

    @property
    def digest(self) -> str:
        """Content digest of the manifest."""

        encoded = json.dumps(
            self.canonical_content(), sort_keys=True, separators=(",", ":")
        ).encode()
        return f"sha256:{hashlib.sha256(encoded).hexdigest()}"

    @property
    def digest_token(self) -> str:
        return self.digest.split(":", 1)[1]

    def to_json(self) -> str:
        """Canonical JSON payload including the content digest."""

        content = self.canonical_content()
        content["digest"] = self.digest
        return json.dumps(content, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, payload: str | bytes) -> SourceLayoutManifest:
        """Decode a manifest payload and verify its content digest.

        Rejects a payload whose digest does not match its content: a
        manifest is immutable and content-addressed, so a mismatch means
        the bytes are corrupt or were produced for another layout.
        """

        try:
            decoded = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError(f"manifest payload is not valid JSON: {error}") from error
        if not isinstance(decoded, dict):
            raise ValueError("manifest payload must be a JSON object")
        if decoded.get("schema") != "kv-cache-source-layout-manifest":
            raise ValueError("unexpected manifest schema")
        declared_digest = decoded.get("digest")
        if not isinstance(declared_digest, str) or not declared_digest:
            raise ValueError("manifest payload must carry a digest")
        try:
            domain_content = decoded["domain"]
            domain = ModelSemanticDomain(
                namespace=domain_content["namespace"],
                model_identity=domain_content["model_identity"],
                revision=domain_content["revision"],
                dtype=domain_content["dtype"],
                itemsize=domain_content["itemsize"],
                page_size=domain_content["page_size"],
                global_layer_ids=tuple(domain_content["global_layer_ids"]),
                total_kv_heads=domain_content["total_kv_heads"],
                key_head_dim=domain_content["key_head_dim"],
                value_head_dim=domain_content["value_head_dim"],
            )
            parts = tuple(
                SourcePartCoverage(
                    part_index=item["part_index"],
                    layer_ids=tuple(item["layer_ids"]),
                    head_start=item["head_start"],
                    head_count=item["head_count"],
                )
                for item in decoded["parts"]
            )
            manifest = cls(
                domain=domain,
                layout=KVCacheHostLayout(decoded["layout"]),
                parts=parts,
            )
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"manifest payload is not decodable: {error}") from error
        if manifest.digest != declared_digest:
            raise ValueError(  # noqa: TRY003
                "manifest digest does not match its content: "
                f"declared={declared_digest}, computed={manifest.digest}"
            )
        return manifest
