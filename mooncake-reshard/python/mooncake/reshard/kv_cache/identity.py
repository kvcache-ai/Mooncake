"""Topology-independent semantic identity and logical keys for cached KV."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from functools import cached_property

from .types import KVCacheDescriptor, require_nonempty_string


@dataclass(frozen=True)
class KVCacheModelDomain:
    """Identify compatible KV content without naming its physical owners.

    The framework supplies the weight revision and a fingerprint of settings
    that affect KV values, such as RoPE configuration. Geometry alone cannot
    establish model compatibility. TP/PP ranks, buffer addresses and host
    axis order do not participate in this identity.
    """

    namespace: str
    model_id: str
    model_revision: str
    semantic_fingerprint: str
    descriptor: KVCacheDescriptor

    def __post_init__(self) -> None:
        for name in ("namespace", "model_id", "model_revision", "semantic_fingerprint"):
            require_nonempty_string(getattr(self, name), name)
        if not isinstance(self.descriptor, KVCacheDescriptor):
            raise ValueError("descriptor must be a KVCacheDescriptor")  # noqa: TRY004

    @cached_property
    def digest(self) -> str:
        """Stable semantic-domain digest, compatible with existing cache keys."""
        content = {
            "namespace": self.namespace,
            "model_id": self.model_id,
            "model_revision": self.model_revision,
            "semantic_fingerprint": self.semantic_fingerprint,
            "descriptor": asdict(self.descriptor),
            # Preserve the existing domain encoding. This constant is a
            # compatibility marker, never the active host-memory axis order.
            "object_format": "PLHD",
        }
        encoded = json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()

    def logical_page_key(self, prefix: str, token_prefix_hash: str) -> str:
        """Name one logical page; storage-specific protocol suffixes are external.

        The caller supplies the hash of the complete token prefix, including
        its position/context semantics, rather than only the current page.
        """
        require_nonempty_string(prefix, "prefix")
        require_nonempty_string(token_prefix_hash, "token_prefix_hash")
        return f"{prefix}_{token_prefix_hash}_m{self.digest}"


__all__ = ["KVCacheModelDomain"]
