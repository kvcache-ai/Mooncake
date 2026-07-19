"""Versioned, fail-closed token handoff for the Prefill -> Decode boundary.

The KV handoff alone is not sufficient to safely remove the original
multimodal request from the Decode leg.  Decode must also receive the exact
prompt token sequence that indexed the transferred KV blocks and enough
identity information to reject a stale/mismatched handoff.

This module intentionally contains no vLLM imports.  The proxy and the
repo-local vLLM adapter can therefore validate the same wire contract without
coupling Mooncake's control plane to a particular vLLM package layout.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Sequence


DECODE_TOKEN_ENVELOPE_KIND = "mooncake_epd.decode_token_envelope"
DECODE_TOKEN_ENVELOPE_PROTOCOL_VERSION = 1


class DecodeTokenEnvelopeError(ValueError):
    """Raised when a token envelope cannot be trusted."""


# Only model-visible generation controls belong in the semantics digest.
# Routing metadata, benchmark labels, raw messages, and media bytes are
# deliberately excluded; their model-visible result is represented by the
# prompt token digest and the multimodal feature hashes.
_GENERATION_CONTROL_KEYS = (
    "model",
    "frequency_penalty",
    "logit_bias",
    "logprobs",
    "top_logprobs",
    "max_tokens",
    "max_completion_tokens",
    "n",
    "presence_penalty",
    "response_format",
    "seed",
    "stop",
    "temperature",
    "top_p",
    "use_beam_search",
    "top_k",
    "min_p",
    "repetition_penalty",
    "length_penalty",
    "stop_token_ids",
    "include_stop_str_in_output",
    "ignore_eos",
    "min_tokens",
    "skip_special_tokens",
    "spaces_between_special_tokens",
    "truncate_prompt_tokens",
    "truncation_side",
    "prompt_logprobs",
    "allowed_token_ids",
    "bad_words",
    "structured_outputs",
    "priority",
    "cache_salt",
    "vllm_xargs",
    "repetition_detection",
    "thinking_token_budget",
)

_KV_BINDING_KEYS = (
    "transfer_id",
    "remote_engine_id",
    "remote_bootstrap_addr",
    "remote_block_ids",
    "manifest_id",
    "manifest_version",
    "source_generation",
    "destination_generation",
    "a2a_source_node",
    "a2a_target_node",
    "handoff_id",
)


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise DecodeTokenEnvelopeError(
            f"decode token envelope contains a non-canonical value: {exc}"
        ) from exc


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _normalize_token_ids(raw: Any) -> tuple[int, ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raise DecodeTokenEnvelopeError("prompt_token_ids must be a JSON array")
    tokens: list[int] = []
    for index, value in enumerate(raw):
        if isinstance(value, bool):
            raise DecodeTokenEnvelopeError(
                f"prompt_token_ids[{index}] must be an integer, not bool"
            )
        try:
            token_id = int(value)
        except (TypeError, ValueError) as exc:
            raise DecodeTokenEnvelopeError(
                f"prompt_token_ids[{index}] is not an integer"
            ) from exc
        if token_id < 0:
            raise DecodeTokenEnvelopeError(
                f"prompt_token_ids[{index}] must be non-negative"
            )
        tokens.append(token_id)
    if not tokens:
        raise DecodeTokenEnvelopeError("prompt_token_ids must not be empty")
    return tuple(tokens)


def _normalize_hashes(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(sorted({str(value) for value in values if str(value)}))


def _normalize_placeholder_spans(
    raw: Any,
    *,
    prompt_token_count: int,
) -> Dict[str, list[Dict[str, int]]]:
    if raw in (None, {}):
        return {}
    if not isinstance(raw, Mapping):
        raise DecodeTokenEnvelopeError(
            "multimodal placeholder spans must be an object"
        )

    normalized: Dict[str, list[Dict[str, int]]] = {}
    occupied: list[tuple[int, int, str]] = []
    for raw_modality, raw_ranges in raw.items():
        modality = str(raw_modality or "").strip()
        if not modality:
            raise DecodeTokenEnvelopeError(
                "multimodal placeholder modality must not be empty"
            )
        if not isinstance(raw_ranges, Sequence) or isinstance(
            raw_ranges, (str, bytes, bytearray)
        ):
            raise DecodeTokenEnvelopeError(
                f"multimodal placeholder spans for {modality!r} "
                "must be an array"
            )
        ranges: list[Dict[str, int]] = []
        for index, item in enumerate(raw_ranges):
            if not isinstance(item, Mapping):
                raise DecodeTokenEnvelopeError(
                    f"multimodal placeholder span {modality}[{index}] "
                    "must be an object"
                )
            offset_raw = item.get("offset")
            length_raw = item.get("length")
            if isinstance(offset_raw, bool) or isinstance(length_raw, bool):
                raise DecodeTokenEnvelopeError(
                    f"multimodal placeholder span {modality}[{index}] "
                    "must use integer offset/length"
                )
            try:
                offset = int(offset_raw)
                length = int(length_raw)
            except (TypeError, ValueError) as exc:
                raise DecodeTokenEnvelopeError(
                    f"multimodal placeholder span {modality}[{index}] "
                    "has invalid offset/length"
                ) from exc
            if offset < 0 or length <= 0:
                raise DecodeTokenEnvelopeError(
                    f"multimodal placeholder span {modality}[{index}] "
                    "requires offset >= 0 and length > 0"
                )
            end = offset + length
            if end > prompt_token_count:
                raise DecodeTokenEnvelopeError(
                    f"multimodal placeholder span {modality}[{index}] "
                    "exceeds the prompt token sequence"
                )
            ranges.append({"offset": offset, "length": length})
            occupied.append((offset, end, f"{modality}[{index}]"))
        if ranges:
            normalized[modality] = sorted(
                ranges, key=lambda item: (item["offset"], item["length"])
            )

    occupied.sort(key=lambda item: (item[0], item[1], item[2]))
    for previous, current in zip(occupied, occupied[1:]):
        if current[0] < previous[1]:
            raise DecodeTokenEnvelopeError(
                "multimodal placeholder spans overlap: "
                f"{previous[2]} and {current[2]}"
            )
    return {key: normalized[key] for key in sorted(normalized)}


def _normalize_render_mm_hashes(raw: Any) -> Dict[str, list[str]]:
    if raw in (None, {}):
        return {}
    if not isinstance(raw, Mapping):
        raise DecodeTokenEnvelopeError(
            "Prefill renderer multimodal hashes must be an object"
        )
    result: Dict[str, list[str]] = {}
    for raw_modality, raw_hashes in raw.items():
        modality = str(raw_modality or "").strip()
        if not modality:
            raise DecodeTokenEnvelopeError(
                "Prefill renderer multimodal hash modality is empty"
            )
        if not isinstance(raw_hashes, Sequence) or isinstance(
            raw_hashes, (str, bytes, bytearray)
        ):
            raise DecodeTokenEnvelopeError(
                f"Prefill renderer multimodal hashes for {modality!r} "
                "must be an array"
            )
        hashes = [str(value or "").strip() for value in raw_hashes]
        if not hashes or any(not value for value in hashes):
            raise DecodeTokenEnvelopeError(
                f"Prefill renderer multimodal hashes for {modality!r} "
                "contain an empty identity"
            )
        result[modality] = hashes
    return {key: result[key] for key in sorted(result)}


def _normalize_render_mm_metadata(raw: Any) -> Dict[str, list[Dict[str, Any]]]:
    if raw in (None, {}):
        return {}
    if not isinstance(raw, Mapping):
        raise DecodeTokenEnvelopeError(
            "Prefill renderer multimodal structural metadata must be an object"
        )
    allowed_keys = {
        "image_grid_thw",
        "video_grid_thw",
        "second_per_grid_ts",
        "image_sizes",
    }
    result: Dict[str, list[Dict[str, Any]]] = {}
    for raw_modality, raw_items in raw.items():
        modality = str(raw_modality or "").strip()
        if not modality or not isinstance(raw_items, Sequence) or isinstance(
            raw_items, (str, bytes, bytearray)
        ):
            raise DecodeTokenEnvelopeError(
                "Prefill renderer multimodal structural metadata is malformed"
            )
        items: list[Dict[str, Any]] = []
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping) or not raw_item:
                raise DecodeTokenEnvelopeError(
                    f"Prefill renderer structural item for {modality!r} "
                    "must not be empty"
                )
            unknown = set(str(key) for key in raw_item) - allowed_keys
            if unknown:
                raise DecodeTokenEnvelopeError(
                    "Prefill renderer structural metadata contains "
                    f"unsupported keys: {sorted(unknown)}"
                )
            # Canonical JSON round-trip rejects tensors, bytes, NaN and other
            # payloads that could smuggle media through this small contract.
            item = json.loads(_canonical_json(dict(raw_item)))
            if not isinstance(item, dict):
                raise DecodeTokenEnvelopeError(
                    "Prefill renderer structural item must be an object"
                )
            items.append(item)
        result[modality] = items
    return {key: result[key] for key in sorted(result)}


def _selected(mapping: Mapping[str, Any], keys: Sequence[str]) -> Dict[str, Any]:
    return {key: mapping[key] for key in keys if key in mapping}


def _checksum_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        str(key): value
        for key, value in payload.items()
        if str(key) != "envelope_sha256"
    }


@dataclass(frozen=True)
class DecodeTokenEnvelope:
    """Validated Prefill output required by a token-ID Decode request."""

    prompt_token_ids: tuple[int, ...]
    payload: Dict[str, Any]

    @property
    def prompt_token_sha256(self) -> str:
        return str(self.payload["prompt_token_sha256"])

    @property
    def envelope_sha256(self) -> str:
        return str(self.payload["envelope_sha256"])

    def to_dict(self, *, include_prompt_token_ids: bool = True) -> Dict[str, Any]:
        result = dict(self.payload)
        if include_prompt_token_ids:
            result["prompt_token_ids"] = list(self.prompt_token_ids)
        return result


def build_decode_token_envelope(
    *,
    api: str,
    request_body: Mapping[str, Any],
    prefill_response: Mapping[str, Any],
    kv_transfer_params: Mapping[str, Any],
    prefill_worker_id: str,
    decode_worker_id: str,
    model_id_sha256: str = "",
    multimodal_feature_hashes: Iterable[Any] = (),
) -> DecodeTokenEnvelope:
    """Build and self-validate the v1 Decode token envelope.

    ``prefill_response`` must be the response produced by the same prompt-only
    request that created ``kv_transfer_params``.  The returned checksum binds
    the prompt tokens, generation controls, worker identities, KV manifest,
    and multimodal feature identities.
    """

    prompt_token_ids = _normalize_token_ids(
        prefill_response.get("prompt_token_ids")
    )
    prompt_metadata = prefill_response.get("mooncake_epd_prompt_envelope")
    if prompt_metadata is None:
        prompt_metadata = {}
    if not isinstance(prompt_metadata, Mapping):
        raise DecodeTokenEnvelopeError(
            "Prefill prompt envelope metadata must be an object"
        )
    captured_token_ids = prompt_metadata.get("prompt_token_ids")
    if captured_token_ids is not None:
        normalized_captured = _normalize_token_ids(captured_token_ids)
        if normalized_captured != prompt_token_ids:
            raise DecodeTokenEnvelopeError(
                "Prefill prompt envelope token IDs do not match the "
                "prompt-only response"
            )
    captured_token_sha256 = str(
        prompt_metadata.get("prompt_token_sha256") or ""
    )
    if captured_token_sha256 and not hmac.compare_digest(
        captured_token_sha256,
        _sha256(prompt_token_ids),
    ):
        raise DecodeTokenEnvelopeError(
            "Prefill prompt envelope token digest mismatch"
        )
    placeholder_spans = _normalize_placeholder_spans(
        prompt_metadata.get("mm_placeholders"),
        prompt_token_count=len(prompt_token_ids),
    )
    render_mm_hashes = _normalize_render_mm_hashes(
        prompt_metadata.get("mm_hashes")
    )
    render_mm_metadata = _normalize_render_mm_metadata(
        prompt_metadata.get("mm_metadata")
    )
    usage = dict(prefill_response.get("usage") or {})
    observed_prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
    if observed_prompt_tokens and observed_prompt_tokens != len(prompt_token_ids):
        raise DecodeTokenEnvelopeError(
            "Prefill prompt token count does not match returned prompt_token_ids: "
            f"usage={observed_prompt_tokens} ids={len(prompt_token_ids)}"
        )

    generation_controls = _selected(request_body, _GENERATION_CONTROL_KEYS)
    kv_binding = _selected(kv_transfer_params, _KV_BINDING_KEYS)
    required_kv_fields = (
        "transfer_id",
        "remote_engine_id",
        "remote_bootstrap_addr",
        "remote_block_ids",
    )
    missing = [field for field in required_kv_fields if not kv_binding.get(field)]
    if missing:
        raise DecodeTokenEnvelopeError(
            "cannot bind Decode token envelope to incomplete KV handoff: "
            + ", ".join(missing)
        )

    request_model = str(request_body.get("model") or "")
    effective_model_hash = str(model_id_sha256 or "")
    if not effective_model_hash and request_model:
        effective_model_hash = hashlib.sha256(
            request_model.encode("utf-8")
        ).hexdigest()

    normalized_mm_hashes = _normalize_hashes(multimodal_feature_hashes)
    if normalized_mm_hashes and not placeholder_spans:
        raise DecodeTokenEnvelopeError(
            "multimodal Decode token envelope requires captured placeholder "
            "spans from the Prefill render"
        )
    if normalized_mm_hashes:
        if set(render_mm_hashes) != set(placeholder_spans):
            raise DecodeTokenEnvelopeError(
                "Prefill renderer multimodal hash modalities do not match "
                "placeholder modalities"
            )
        for modality, spans in placeholder_spans.items():
            if len(render_mm_hashes.get(modality, [])) != len(spans):
                raise DecodeTokenEnvelopeError(
                    "Prefill renderer multimodal hash count does not match "
                    f"placeholder count for {modality!r}"
                )
        if set(render_mm_metadata) != set(placeholder_spans):
            raise DecodeTokenEnvelopeError(
                "Prefill renderer multimodal structural metadata modalities "
                "do not match placeholder modalities"
            )
        for modality, spans in placeholder_spans.items():
            if len(render_mm_metadata.get(modality, [])) != len(spans):
                raise DecodeTokenEnvelopeError(
                    "Prefill renderer multimodal structural metadata count "
                    f"does not match placeholder count for {modality!r}"
                )

    payload: Dict[str, Any] = {
        "kind": DECODE_TOKEN_ENVELOPE_KIND,
        "version": DECODE_TOKEN_ENVELOPE_PROTOCOL_VERSION,
        "api": str(api),
        "model_id_sha256": effective_model_hash,
        "prompt_token_count": len(prompt_token_ids),
        "prompt_token_sha256": _sha256(prompt_token_ids),
        "generation_controls_sha256": _sha256(generation_controls),
        "kv_binding_sha256": _sha256(kv_binding),
        "multimodal_feature_hashes": list(normalized_mm_hashes),
        "multimodal_placeholder_spans": placeholder_spans,
        "multimodal_placeholder_spans_sha256": _sha256(
            placeholder_spans
        ),
        "render_multimodal_hashes": render_mm_hashes,
        "render_multimodal_hashes_sha256": _sha256(render_mm_hashes),
        "render_multimodal_metadata": render_mm_metadata,
        "render_multimodal_metadata_sha256": _sha256(render_mm_metadata),
        "prefill_worker_id": str(prefill_worker_id),
        "decode_worker_id": str(decode_worker_id),
    }
    payload["envelope_sha256"] = _sha256(payload)
    envelope = DecodeTokenEnvelope(
        prompt_token_ids=prompt_token_ids,
        payload=payload,
    )
    return validate_decode_token_envelope(
        envelope.to_dict(),
        request_body=request_body,
        kv_transfer_params=kv_transfer_params,
        expected_api=api,
        expected_prefill_worker_id=prefill_worker_id,
        expected_decode_worker_id=decode_worker_id,
        expected_model_id_sha256=effective_model_hash,
        expected_multimodal_feature_hashes=multimodal_feature_hashes,
    )


def validate_decode_token_envelope(
    raw: Mapping[str, Any],
    *,
    request_body: Mapping[str, Any],
    kv_transfer_params: Mapping[str, Any],
    expected_api: str,
    expected_prefill_worker_id: str,
    expected_decode_worker_id: str,
    expected_model_id_sha256: str = "",
    expected_multimodal_feature_hashes: Iterable[Any] = (),
) -> DecodeTokenEnvelope:
    """Validate every identity fence before media may be removed."""

    if not isinstance(raw, Mapping):
        raise DecodeTokenEnvelopeError("decode token envelope must be an object")
    if raw.get("kind") != DECODE_TOKEN_ENVELOPE_KIND:
        raise DecodeTokenEnvelopeError("decode token envelope kind mismatch")
    try:
        version = int(raw.get("version", 0) or 0)
    except (TypeError, ValueError) as exc:
        raise DecodeTokenEnvelopeError(
            "decode token envelope version is not an integer"
        ) from exc
    if version != DECODE_TOKEN_ENVELOPE_PROTOCOL_VERSION:
        raise DecodeTokenEnvelopeError(
            "decode token envelope protocol mismatch: "
            f"received={version} expected={DECODE_TOKEN_ENVELOPE_PROTOCOL_VERSION}"
        )

    prompt_token_ids = _normalize_token_ids(raw.get("prompt_token_ids"))
    payload = {
        str(key): value
        for key, value in raw.items()
        if str(key) != "prompt_token_ids"
    }
    expected_checksum = _sha256(_checksum_payload(payload))
    actual_checksum = str(payload.get("envelope_sha256") or "")
    if not actual_checksum or not hmac.compare_digest(
        actual_checksum, expected_checksum
    ):
        raise DecodeTokenEnvelopeError("decode token envelope checksum mismatch")

    checks = {
        "api": (str(payload.get("api") or ""), str(expected_api)),
        "prefill worker": (
            str(payload.get("prefill_worker_id") or ""),
            str(expected_prefill_worker_id),
        ),
        "decode worker": (
            str(payload.get("decode_worker_id") or ""),
            str(expected_decode_worker_id),
        ),
    }
    for label, (actual, expected) in checks.items():
        if actual != expected:
            raise DecodeTokenEnvelopeError(
                f"decode token envelope {label} mismatch: "
                f"received={actual!r} expected={expected!r}"
            )

    if int(payload.get("prompt_token_count", -1)) != len(prompt_token_ids):
        raise DecodeTokenEnvelopeError(
            "decode token envelope prompt token count mismatch"
        )
    if not hmac.compare_digest(
        str(payload.get("prompt_token_sha256") or ""),
        _sha256(prompt_token_ids),
    ):
        raise DecodeTokenEnvelopeError(
            "decode token envelope prompt token digest mismatch"
        )

    controls = _selected(request_body, _GENERATION_CONTROL_KEYS)
    if not hmac.compare_digest(
        str(payload.get("generation_controls_sha256") or ""),
        _sha256(controls),
    ):
        raise DecodeTokenEnvelopeError(
            "decode token envelope generation controls mismatch"
        )
    kv_binding = _selected(kv_transfer_params, _KV_BINDING_KEYS)
    if not hmac.compare_digest(
        str(payload.get("kv_binding_sha256") or ""),
        _sha256(kv_binding),
    ):
        raise DecodeTokenEnvelopeError("decode token envelope KV binding mismatch")

    actual_model_hash = str(payload.get("model_id_sha256") or "")
    expected_model_hash = str(expected_model_id_sha256 or "")
    if expected_model_hash and not hmac.compare_digest(
        actual_model_hash, expected_model_hash
    ):
        raise DecodeTokenEnvelopeError(
            "decode token envelope model identity mismatch"
        )
    actual_mm_hashes = _normalize_hashes(
        payload.get("multimodal_feature_hashes") or ()
    )
    expected_mm_hashes = _normalize_hashes(expected_multimodal_feature_hashes)
    if actual_mm_hashes != expected_mm_hashes:
        raise DecodeTokenEnvelopeError(
            "decode token envelope multimodal feature identity mismatch"
        )
    placeholder_spans = _normalize_placeholder_spans(
        payload.get("multimodal_placeholder_spans"),
        prompt_token_count=len(prompt_token_ids),
    )
    if expected_mm_hashes and not placeholder_spans:
        raise DecodeTokenEnvelopeError(
            "decode token envelope is missing multimodal placeholder spans"
        )
    if not hmac.compare_digest(
        str(payload.get("multimodal_placeholder_spans_sha256") or ""),
        _sha256(placeholder_spans),
    ):
        raise DecodeTokenEnvelopeError(
            "decode token envelope multimodal placeholder digest mismatch"
        )
    render_mm_hashes = _normalize_render_mm_hashes(
        payload.get("render_multimodal_hashes")
    )
    if expected_mm_hashes:
        if set(render_mm_hashes) != set(placeholder_spans):
            raise DecodeTokenEnvelopeError(
                "decode token envelope renderer hash modalities mismatch"
            )
        for modality, spans in placeholder_spans.items():
            if len(render_mm_hashes.get(modality, [])) != len(spans):
                raise DecodeTokenEnvelopeError(
                    "decode token envelope renderer hash count mismatch"
                )
    if not hmac.compare_digest(
        str(payload.get("render_multimodal_hashes_sha256") or ""),
        _sha256(render_mm_hashes),
    ):
        raise DecodeTokenEnvelopeError(
            "decode token envelope renderer hash digest mismatch"
        )
    render_mm_metadata = _normalize_render_mm_metadata(
        payload.get("render_multimodal_metadata")
    )
    if expected_mm_hashes:
        if set(render_mm_metadata) != set(placeholder_spans):
            raise DecodeTokenEnvelopeError(
                "decode token envelope renderer structural metadata "
                "modalities mismatch"
            )
        for modality, spans in placeholder_spans.items():
            if len(render_mm_metadata.get(modality, [])) != len(spans):
                raise DecodeTokenEnvelopeError(
                    "decode token envelope renderer structural metadata "
                    "count mismatch"
                )
    if not hmac.compare_digest(
        str(payload.get("render_multimodal_metadata_sha256") or ""),
        _sha256(render_mm_metadata),
    ):
        raise DecodeTokenEnvelopeError(
            "decode token envelope renderer structural metadata digest "
            "mismatch"
        )

    return DecodeTokenEnvelope(
        prompt_token_ids=prompt_token_ids,
        payload=payload,
    )
