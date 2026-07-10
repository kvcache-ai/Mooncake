"""Deterministic request variants for cache-neutral serving benchmarks.

vLLM's prefix cache is a production optimization. It is useful when a
benchmark measures reuse, but it invalidates a raw Prefill/KV transport
comparison when every request has an identical prompt. This module creates a
small, explicit text suffix so paired EPD and baseline runs can exercise
independent Prefill work without relying on cache-disable internals.
"""

from __future__ import annotations

from typing import Any, Dict


REQUEST_VARIATION_NONE = "none"
REQUEST_VARIATION_UNIQUE_PREFIX = "unique_prefix"
# Kept as a CLI-compatible alias for the first benchmark-runner revision.
# The implementation intentionally inserts at the beginning: a suffix still
# shares the entire expensive prompt prefix with vLLM's radix cache.
REQUEST_VARIATION_UNIQUE_SUFFIX = "unique_suffix"
REQUEST_VARIATION_SCHEMA_VERSION = 1


def request_variation_spec(mode: str) -> Dict[str, Any]:
    """Return the artifact-stable definition of a request variation mode."""

    normalized = str(mode or REQUEST_VARIATION_NONE).strip().lower()
    if normalized == REQUEST_VARIATION_UNIQUE_SUFFIX:
        normalized = REQUEST_VARIATION_UNIQUE_PREFIX
    if normalized not in {REQUEST_VARIATION_NONE, REQUEST_VARIATION_UNIQUE_PREFIX}:
        raise ValueError(f"unsupported request variation mode: {mode!r}")
    return {
        "mode": normalized,
        "schema_version": REQUEST_VARIATION_SCHEMA_VERSION,
    }


def apply_request_variation(
    request_body: Dict[str, Any],
    *,
    mode: str,
    phase: str,
    repeat_idx: int,
) -> str | None:
    """Apply a deterministic variant in place and return its identifier.

    ``unique_prefix`` changes the first model-visible tokens so radix-prefix
    lookup cannot reuse an earlier warmup or measurement request. Appending a
    unique suffix is insufficient because radix caching can still reuse the
    full shared prefix. The marker is short and explicitly benchmark-only,
    keeping the original task as the semantic instruction. Both compared
    runners use identical phase/index pairs.
    """

    spec = request_variation_spec(mode)
    if spec["mode"] == REQUEST_VARIATION_NONE:
        return None

    messages = request_body.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ValueError("request variation requires a non-empty messages list")
    last_message = messages[-1]
    if not isinstance(last_message, dict):
        raise ValueError("request variation requires a mapping final message")
    content = last_message.get("content")
    if not isinstance(content, list):
        raise ValueError("request variation requires list-form message content")

    variation_id = (
        f"epd-bench-v{REQUEST_VARIATION_SCHEMA_VERSION}-{phase}-{int(repeat_idx)}"
    )
    prefix = (
        "[Benchmark nonce: "
        f"{variation_id}. This nonce is opaque test metadata; answer the task "
        "below normally and do not mention it.]\n\n"
    )
    for item in reversed(content):
        if isinstance(item, dict) and item.get("type") == "text":
            item["text"] = f"{prefix}{str(item.get('text') or '')}"
            return variation_id
    raise ValueError("request variation requires at least one text content item")
