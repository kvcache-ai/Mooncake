"""Per-sequence state objects for hybrid / SWA models over Mooncake Store.

Implements the adapter contract agreed in Mooncake issue #4803:

- Key namespace ``state/<fingerprint>/<prefix-chain-hash@aligned-boundary>``.
  State objects never collide with engine-written block KV keys.
- The layout fingerprint is versioned itself (``v1-<sha256[:16]>``), so a
  layout change is a clean miss rather than a collision.
- State values are fixed-size opaque slots. Both sides validate the payload
  size; any mismatch (or a missing object) is reported as a miss.
- Availability is published only after a successful write.
- The store reports *facts*: ``block_hit_length`` and ``state_available`` are
  independent fields. Replay policy is the engine's decision, not ours.
- No time-based TTL. Eviction is whatever the backend does; expiry is
  observed through ``is_exist``/``get_size``, never assumed.

The backend is any object satisfying the ``BundleStore`` protocol already
used by ``mooncake.structured_object_store`` (``put``/``get``/``remove``/
``is_exist``), e.g. ``mooncake.store.MooncakeDistributedStore``.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, Sequence

__all__ = [
    "STATE_NAMESPACE",
    "StateBundleStore",
    "StateLayout",
    "StateFingerprintError",
    "StateAlignmentError",
    "StateSizeError",
    "HitReport",
    "StateStoreAdapter",
]

#: Reserved key namespace prefix. Contract tests must keep this disjoint
#: from every engine-side block KV namespace (e.g. ``dsv4kv/...``).
STATE_NAMESPACE = "state"

#: ``<scheme>-<hex>``; the scheme part is the layout version required by
#: the #4803 review ("version the fingerprint itself, not just the layout").
_FINGERPRINT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*-[0-9a-f]+$")

#: Mooncake Store error code returned for an already-missing object.
MISSING_OBJECT_ERROR = -704


class StateBundleStore(Protocol):
    """Minimal backend protocol (a subset of the structured-object one)."""

    def put(self, key: str, value: Any, config: Any = None) -> int: ...

    def get(self, key: str) -> bytes: ...

    def remove(self, key: str, force: bool = False) -> int: ...

    def is_exist(self, key: str) -> int: ...


class StateFingerprintError(ValueError):
    """The fingerprint is malformed or its scheme version is unknown."""


class StateAlignmentError(ValueError):
    """A length is not aligned to the state boundary."""


class StateSizeError(ValueError):
    """The payload size does not match the declared state layout."""


@dataclass(frozen=True)
class StateLayout:
    """Adapter-side fixed layout for one model family.

    ``layer_offsets`` maps layer index -> byte offset of that layer's slot
    inside the fixed-size per-sequence state. The total size is derived and
    must match what the engine writes; a mismatch fails both put and get.
    """

    layer_count: int
    bytes_per_layer: int
    #: Token granularity at which state objects exist (e.g. 128).
    boundary: int = 128
    #: Layout scheme version, embedded in every fingerprint of this layout.
    scheme_version: str = "v1"
    #: Human-readable identity, mixed into fingerprint hashing inputs.
    model_id: str = ""

    @property
    def state_size(self) -> int:
        return self.layer_count * self.bytes_per_layer

    def fingerprint(self, extra: Sequence[str] = ()) -> str:
        """Versioned fingerprint ``<scheme>-<sha256[:16]>``.

        ``extra`` carries everything that changes the state ring layout
        (checkpoint ``config.json`` content, page bytes, state bytes, block
        size, relevant environment variables) so that any layout-affecting
        change yields a different namespace and therefore a clean miss.
        """
        digest = hashlib.sha256()
        for part in (
            self.model_id,
            str(self.layer_count),
            str(self.bytes_per_layer),
            str(self.boundary),
            *extra,
        ):
            digest.update(part.encode("utf-8"))
            digest.update(b"\x00")
        return f"{self.scheme_version}-{digest.hexdigest()[:16]}"


@dataclass(frozen=True)
class HitReport:
    """Facts about a hit resolution. No replay policy lives here.

    The engine combines ``block_hit_length`` with its own replay ability
    (bounded replay vs. exact reconstruction) to pick the deepest usable
    boundary. This type deliberately reports existence only.
    """

    block_hit_length: int = 0
    #: boundary length -> state object present at that boundary
    state_available: Mapping[int, bool] = field(default_factory=dict)


class StateStoreAdapter:
    """Per-sequence fixed-size state objects over a Mooncake-compatible store."""

    def __init__(
        self,
        store: StateBundleStore,
        layout: StateLayout,
        fingerprint: str,
    ) -> None:
        self._store = store
        self._layout = layout
        if not _FINGERPRINT_RE.match(fingerprint):
            raise StateFingerprintError(
                f"fingerprint {fingerprint!r} must match "
                "'<scheme>-<hex>' (e.g. 'v1-0123456789abcdef')"
            )
        if not fingerprint.startswith(f"{layout.scheme_version}-"):
            raise StateFingerprintError(
                f"fingerprint {fingerprint!r} does not use scheme version "
                f"{layout.scheme_version!r} of this adapter"
            )
        self._fingerprint = fingerprint

    # -- key construction -------------------------------------------------

    @property
    def fingerprint(self) -> str:
        return self._fingerprint

    def _check_aligned(self, aligned_length: int) -> None:
        if aligned_length <= 0:
            raise StateAlignmentError(
                f"aligned_length must be positive, got {aligned_length}"
            )
        if aligned_length % self._layout.boundary != 0:
            raise StateAlignmentError(
                f"aligned_length {aligned_length} is not a multiple of the "
                f"{self._layout.boundary}-token state boundary"
            )

    def state_key(self, chain_hash: str, aligned_length: int) -> str:
        """Key of the state object for ``chain_hash`` at ``aligned_length``.

        State objects have no prefix-extension semantics: the key answers
        only for the exact prefix length it was written with.
        """
        self._check_aligned(aligned_length)
        if not chain_hash:
            raise ValueError("chain_hash must be non-empty")
        return f"{STATE_NAMESPACE}/{self._fingerprint}/{chain_hash}"

    # -- write path --------------------------------------------------------

    def put_state(
        self,
        chain_hash: str,
        aligned_length: int,
        payload: bytes | bytearray | memoryview,
    ) -> int:
        """Write one state slot. Only aligned lengths are accepted.

        Availability is published only after the backend put succeeds, so
        a key observed through ``is_exist``/``get`` is a complete write.
        """
        self._check_aligned(aligned_length)
        size = len(memoryview(payload))
        if size != self._layout.state_size:
            raise StateSizeError(
                f"payload is {size} bytes but layout declares "
                f"{self._layout.state_size}"
            )
        key = self.state_key(chain_hash, aligned_length)
        return self._store.put(key, bytes(payload))

    # -- read path ---------------------------------------------------------

    def get_state(
        self,
        chain_hash: str,
        aligned_length: int,
    ) -> bytes | None:
        """Read one state slot. Any failure is a miss, never an exception.

        Covers the documented race: an object present at ``is_exist`` time
        may be evicted before ``get``. The backend then reports
        ``MISSING_OBJECT_ERROR`` (-704) or returns fewer bytes; both are
        treated as a miss.
        """
        self._check_aligned(aligned_length)
        key = self.state_key(chain_hash, aligned_length)
        try:
            value = self._store.get(key)
        except Exception:
            return None
        if not isinstance(value, (bytes, bytearray, memoryview)):
            return None
        if len(memoryview(value)) != self._layout.state_size:
            return None
        return bytes(value)

    def is_available(self, chain_hash: str, aligned_length: int) -> bool:
        self._check_aligned(aligned_length)
        try:
            code = self._store.is_exist(self.state_key(chain_hash, aligned_length))
        except Exception:
            return False
        # Mooncake returns 1 for existing, 0 / negative for missing.
        return int(code) == 1

    def remove_state(self, chain_hash: str, aligned_length: int) -> int:
        self._check_aligned(aligned_length)
        return self._store.remove(self.state_key(chain_hash, aligned_length))

    # -- fact reporting ----------------------------------------------------

    def resolve_hit(
        self,
        block_hit_length: int,
        state_candidates: Sequence[tuple[str, int]],
    ) -> HitReport:
        """Report facts for one prefix query.

        ``block_hit_length`` is the contiguous block-chain hit length the
        engine already computed. ``state_candidates`` are
        ``(chain_hash, aligned_length)`` pairs for boundaries the engine
        could replay to. The adapter only reports availability per boundary;
        choosing among them is engine policy.
        """
        availability: dict[int, bool] = {}
        for chain_hash, aligned_length in state_candidates:
            self._check_aligned(aligned_length)
            availability[aligned_length] = self.is_available(
                chain_hash, aligned_length
            )
        return HitReport(
            block_hit_length=block_hit_length,
            state_available=availability,
        )
