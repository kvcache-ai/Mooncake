"""Model semantic domain and Store-side logical keys (RFC #4719).

The logical key is the reuse contract of cached KV data: the same token
prefix written under the same model semantics is reusable regardless of the
writer's TP/PP topology or physical layout. TP, PP and rank identities are
deliberately excluded; anything that changes KV *content* (model identity,
revision, dtype, page size, the global layer set, the KV-head count and the
head dims) is part of the domain.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from ..types import (
    require_integer,
    require_integer_tuple,
    require_nonempty_string,
)

__all__ = [
    "ModelSemanticDomain",
]


@dataclass(frozen=True)
class ModelSemanticDomain:
    """Everything that decides whether two KV pages are interchangeable.

    A physical format change (host layout, rank count) leaves the domain
    unchanged; a change in model semantics must change it.
    """

    namespace: str
    model_identity: str
    revision: str
    dtype: str
    itemsize: int
    page_size: int
    global_layer_ids: tuple[int, ...]
    total_kv_heads: int
    key_head_dim: int
    value_head_dim: int

    def __post_init__(self) -> None:
        for name in ("namespace", "model_identity", "revision", "dtype"):
            require_nonempty_string(getattr(self, name), name)
        for name in (
            "itemsize",
            "page_size",
            "total_kv_heads",
            "key_head_dim",
            "value_head_dim",
        ):
            require_integer(getattr(self, name), name, minimum=1)
        layers = require_integer_tuple(self.global_layer_ids, "global_layer_ids")
        if not layers:
            raise ValueError("global_layer_ids must not be empty")
        if len(layers) != len(set(layers)):
            raise ValueError("global_layer_ids must be unique")
        object.__setattr__(self, "global_layer_ids", tuple(sorted(layers)))

    def canonical_content(self) -> dict[str, object]:
        """Canonical JSON-able content used for digest and equality."""

        return {
            "schema": "kv-cache-model-semantic-domain",
            "namespace": self.namespace,
            "model_identity": self.model_identity,
            "revision": self.revision,
            "dtype": self.dtype,
            "itemsize": self.itemsize,
            "page_size": self.page_size,
            "global_layer_ids": list(self.global_layer_ids),
            "total_kv_heads": self.total_kv_heads,
            "key_head_dim": self.key_head_dim,
            "value_head_dim": self.value_head_dim,
        }

    @property
    def digest(self) -> str:
        """Content digest of the domain.

        Stable across processes and versions of this module: the canonical
        encoding is sorted-key JSON over the fields above.
        """

        encoded = json.dumps(
            self.canonical_content(), sort_keys=True, separators=(",", ":")
        ).encode()
        return f"sha256:{hashlib.sha256(encoded).hexdigest()}"

    @property
    def digest_token(self) -> str:
        """Digest without the scheme prefix, for key composition."""

        return self.digest.split(":", 1)[1]

    def logical_page_key(self, backend_prefix: str, token_prefix_hash: str) -> str:
        """Store key of one logical KV page.

        ``page_key = <backend/model prefix>_<token prefix hash>_<model_domain>``
        """

        require_nonempty_string(backend_prefix, "backend_prefix")
        require_nonempty_string(token_prefix_hash, "token_prefix_hash")
        return f"{backend_prefix}_{token_prefix_hash}_{self.digest_token}"

    def manifest_key(self, layout_digest: str) -> str:
        """Store key of the shared source-layout manifest.

        ``manifest_key = kv/<model_domain>/manifests/<layout_digest>``
        """

        require_nonempty_string(layout_digest, "layout_digest")
        return f"kv/{self.digest_token}/manifests/{layout_digest}"
