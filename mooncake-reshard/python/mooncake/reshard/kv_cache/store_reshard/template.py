"""Read-template cache: reuse planning results, never live addresses.

A read template holds the *relative* source and target offsets of one plan
(a ``ReshardReadPlan``), keyed by the source manifest's content digest plus
the target binding identity. A cache hit therefore reuses geometry, not
addresses: it never substitutes for checking current object readability and
replica leases, and it never spans a reallocation of the target host pool.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..types import require_integer, require_nonempty_string
from .planner import ReshardReadPlan

__all__ = [
    "ReadTemplate",
    "ReadTemplateCache",
]


@dataclass(frozen=True)
class ReadTemplate:
    """One cached planning result.

    ``manifest_digest`` identifies the source layout; ``target_binding_id``
    identifies the reader's placement and host-pool binding; ``plan`` holds
    relative offsets only. Either side changing invalidates the template,
    because the digest / binding id pair no longer matches.
    """

    manifest_digest: str
    target_binding_id: str
    requirement_fingerprint: str
    plan: ReshardReadPlan

    def __post_init__(self) -> None:
        require_nonempty_string(self.manifest_digest, "manifest_digest")
        require_nonempty_string(self.target_binding_id, "target_binding_id")
        require_nonempty_string(self.requirement_fingerprint, "requirement_fingerprint")
        if not isinstance(self.plan, ReshardReadPlan):
            raise ValueError("plan must be a ReshardReadPlan")  # noqa: TRY004

    @property
    def cache_key(self) -> tuple[str, str, str]:
        return (
            self.manifest_digest,
            self.target_binding_id,
            self.requirement_fingerprint,
        )


class ReadTemplateCache:
    """Bounded, in-process cache of read templates.

    Insertion-order eviction: the oldest template is dropped when the cache
    is full. Invalidating a binding removes every template that reads into
    that binding, which is what a host-pool reallocation requires.
    """

    def __init__(self, max_entries: int = 256) -> None:
        require_integer(max_entries, "max_entries", minimum=1)
        self._max_entries = max_entries
        self._entries: dict[tuple[str, str, str], ReadTemplate] = {}

    @property
    def max_entries(self) -> int:
        return self._max_entries

    def __len__(self) -> int:
        return len(self._entries)

    def get(
        self,
        manifest_digest: str,
        target_binding_id: str,
        requirement_fingerprint: str,
    ) -> ReadTemplate | None:
        """Look up a template, or None on miss."""

        require_nonempty_string(manifest_digest, "manifest_digest")
        require_nonempty_string(target_binding_id, "target_binding_id")
        require_nonempty_string(requirement_fingerprint, "requirement_fingerprint")
        return self._entries.get(
            (manifest_digest, target_binding_id, requirement_fingerprint)
        )

    def put(self, template: ReadTemplate) -> ReadTemplate:
        """Insert (or refresh) a template; returns the stored template."""

        if not isinstance(template, ReadTemplate):
            raise ValueError("template must be a ReadTemplate")  # noqa: TRY004
        self._entries.pop(template.cache_key, None)
        self._entries[template.cache_key] = template
        while len(self._entries) > self._max_entries:
            oldest = next(iter(self._entries))
            del self._entries[oldest]
        return template

    def invalidate_binding(self, target_binding_id: str) -> int:
        """Drop every template reading into ``target_binding_id``.

        Returns the number of removed templates.
        """

        require_nonempty_string(target_binding_id, "target_binding_id")
        stale = [key for key in self._entries if key[1] == target_binding_id]
        for key in stale:
            del self._entries[key]
        return len(stale)

    def clear(self) -> None:
        self._entries.clear()
