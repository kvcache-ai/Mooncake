"""Canonical identities for multimodal assets crossing EPD process boundaries."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping


def stable_multimodal_identity_hash(item: Mapping[str, Any]) -> str:
    """Return the common 16-hex cache identity used by E, P, and D.

    A caller-provided UUID follows vLLM's cached-input contract: it becomes the
    stable identity even when a later request omits the media payload.  Without
    a UUID, the identity remains content-derived for backward compatibility.
    The modality is part of UUID identity to prevent cross-domain aliasing.
    """

    supplied_uuid = item.get("uuid")
    if supplied_uuid is not None and str(supplied_uuid).strip():
        payload = {
            "type": str(item.get("type") or "").strip().lower(),
            "uuid": str(supplied_uuid),
        }
    else:
        payload = {
            key: item.get(key)
            for key in sorted(item)
            if key not in {"detail", "uuid"}
        }
    raw = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]
