"""Safety policy for exact L1/L2 hidden-state reuse.

Partial-prefix reuse is intentionally *not* a performance toggle.  It remains
disabled unless a model-specific oracle has explicitly validated the adapter;
exact content-addressed reuse has no such semantic approximation.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() not in {"", "0", "false", "no", "off"}


def _env_float(name: str, default: float, *, minimum: float) -> float:
    try:
        value = float(str(os.getenv(name, default)).strip())
    except (TypeError, ValueError):
        value = float(default)
    return max(float(minimum), value)


@dataclass(frozen=True)
class HiddenCachePolicy:
    """Runtime gate and identity configuration for exact distributed reuse."""

    enable_l1: bool = True
    enable_l2: bool = False
    allow_tensor_hash_fallback: bool = False
    allow_partial_prefix_reuse: bool = False
    partial_oracle_validated: bool = False
    lease_seconds: float = 900.0
    model_id: Optional[str] = None
    model_revision: Optional[str] = None
    processor_revision: Optional[str] = None
    output_schema: str = "qwen3vl-image-embeds-v1"

    @property
    def partial_enabled(self) -> bool:
        # The dual condition makes accidental `ALLOW_PARTIAL=1` insufficient.
        return bool(self.allow_partial_prefix_reuse and self.partial_oracle_validated)

    @property
    def has_l2_identity(self) -> bool:
        return bool(
            self.enable_l2
            and self.model_id
            and self.model_revision
            and self.processor_revision
            and self.output_schema
        )

    @classmethod
    def from_env(cls) -> "HiddenCachePolicy":
        return cls(
            enable_l1=_env_bool("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE", True),
            enable_l2=_env_bool("MOONCAKE_EPD_VLLM_MM_HIDDEN_L2", False),
            allow_tensor_hash_fallback=_env_bool(
                "MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_ALLOW_TENSOR_FALLBACK",
                False,
            ),
            allow_partial_prefix_reuse=_env_bool(
                "MOONCAKE_EPD_HIDDEN_CACHE_ALLOW_PARTIAL",
                False,
            ),
            partial_oracle_validated=_env_bool(
                "MOONCAKE_EPD_HIDDEN_CACHE_PARTIAL_ORACLE_VALIDATED",
                False,
            ),
            lease_seconds=_env_float(
                "MOONCAKE_EPD_HIDDEN_CACHE_LEASE_SECONDS",
                900.0,
                minimum=1.0,
            ),
            model_id=(os.getenv("MOONCAKE_EPD_HIDDEN_CACHE_MODEL_ID") or "").strip() or None,
            model_revision=(os.getenv("MOONCAKE_EPD_HIDDEN_CACHE_MODEL_REVISION") or "").strip() or None,
            processor_revision=(
                os.getenv("MOONCAKE_EPD_HIDDEN_CACHE_PROCESSOR_REVISION") or ""
            ).strip()
            or None,
            output_schema=(
                os.getenv("MOONCAKE_EPD_HIDDEN_CACHE_OUTPUT_SCHEMA")
                or "qwen3vl-image-embeds-v1"
            ).strip(),
        )
