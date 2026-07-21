# SPDX-License-Identifier: Apache-2.0
"""SGLang-compatible connector factory for Mooncake Store."""

from __future__ import annotations

from mooncake.model_loader.mooncake_store_connector import (
    MooncakeStoreConnector,
    create_mooncake_connector,
)

__all__ = ["MooncakeStoreConnector", "create_mooncake_connector", "create_remote_connector"]


def create_remote_connector(url: str, device=None, **kwargs) -> MooncakeStoreConnector:
    """Drop-in for ``sglang.srt.connector.create_remote_connector`` (mooncake only)."""
    del device  # Mooncake connector uses Store env config, not torch device.
    return create_mooncake_connector(url, **kwargs)
