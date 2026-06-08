"""
MLA (Multi-head Latent Attention) KVCache Layout

Implements the KVLayout interface for MLA architecture.
"""

from typing import Iterator, Any, Dict
from dataclasses import dataclass

from .interface import KVLayout
from storage.interface import KVKey


# ============================================================================
# MLA Model Configurations
# ============================================================================

# MLA Model configurations
# Source: https://kvcache.ai/tools/kv-cache-calculator/
# MLA architecture: hash_id -> {layer_0: [pages], layer_1: [pages], ...}
MLA_MODEL_CONFIG = {
    # GLM-5: 78 layers, 64 tokens/page
    # KV: 78 layers × 64 tokens × (512+64+128) × 2 = 90,112 bytes/page
    # Per token: 1,408 bytes
    "glm5": {
        "name": "glm5",
        "num_layers": 78,
        "kv_lora_rank": 512,
        "qk_rope_head_dim": 64,
        "index_head_dim": 128,
        "kv_precision_bytes": 2,  # BF16
        "indexer_precision_bytes": 2,  # BF16
    },

    # Kimi-K2.6: 61 layers, 64 tokens/page
    # KV: 61 layers × 64 tokens × (512+64) × 2 = 73,728 bytes/page
    # Per token: 1,152 bytes
    "kimi-k2.6": {
        "name": "kimi-k2.6",
        "num_layers": 61,
        "kv_lora_rank": 512,
        "qk_rope_head_dim": 64,
        "index_head_dim": 0,  # Kimi doesn't use separate indexer
        "kv_precision_bytes": 2,  # BF16
        "indexer_precision_bytes": 0,
    },
}


@dataclass
class KVEntry:
    """Key-value entry for storage

    Contains both the key and the value size information.
    """
    key: KVKey
    value_size_bytes: int


class MLALayout(KVLayout):
    """MLA (Multi-head Latent Attention) KVCache layout

    MLA Architecture:
    - Each (sequence_id, layer_id) pair maps to ONE complete entry
    - Entry contains KV + Indexer data for up to 64 tokens in that layer
    - Value size is fixed per layer

    Value Size Calculation (per layer entry):
        value_size = page_size_tokens × (kv_lora_rank + qk_rope_head_dim + index_head_dim) × precision_bytes

    For GLM-5 with 64 tokens per entry:
        value_size = 64 × (512 + 64 + 128) × 2 = 90,112 bytes = 88 KiB

    Key Pattern: (sequence_id, layer_id)
    Total Keys = len(hash_ids) × num_layers

    Used in: GLM-5, Kimi-K2.6
    """

    def __init__(self, num_layers: int, kv_lora_rank: int, qk_rope_head_dim: int,
                 index_head_dim: int, precision_bytes: int, page_size_tokens: int = 64):
        """Initialize MLA layout

        Args:
            num_layers: Number of transformer layers
            kv_lora_rank: KV LoRA rank dimension
            qk_rope_head_dim: QK rope head dimension
            index_head_dim: Indexer head dimension
            precision_bytes: Precision in bytes (BF16=2, INT8=1, INT4=0.5)
            page_size_tokens: Tokens per page (default 64)
        """
        self.num_layers = num_layers
        self.kv_lora_rank = kv_lora_rank
        self.qk_rope_head_dim = qk_rope_head_dim
        self.index_head_dim = index_head_dim
        self.precision_bytes = precision_bytes
        self.page_size_tokens = page_size_tokens

        # Calculate fixed value size per entry (per layer)
        # Each entry contains KV + Indexer for 64 tokens in that layer
        self.value_size_bytes = (
            page_size_tokens *
            (kv_lora_rank + qk_rope_head_dim + index_head_dim) *
            precision_bytes
        )

    def get_entries(self, request: Any) -> Iterator[KVEntry]:
        """Generate MLA storage entries

        For each sequence_id in hash_ids, generate one entry per layer.

        Yields:
            KVEntry: (key, value_size) for each layer
        """
        for sequence_id in request.hash_ids:
            for layer_id in range(self.num_layers):
                yield KVEntry(
                    key=KVKey(sequence_id=sequence_id, layer_id=layer_id),
                    value_size_bytes=self.value_size_bytes
                )

    def get_key_count(self, request: Any) -> int:
        """Total keys for MLA request

        Returns:
            int: len(hash_ids) × num_layers
        """
        return len(request.hash_ids) * self.num_layers

    def get_value_size(self, key: KVKey) -> int:
        """Get value size for MLA entry

        All MLA entries have the same size.

        Returns:
            int: Value size in bytes
        """
        return self.value_size_bytes

    def keys_per_layer(self) -> int:
        """MLA has 1 key per (sequence, layer)"""
        return 1


# ============================================================================
# Utility Functions
# ============================================================================

def get_model_config(model_name: str) -> dict:
    """Get MLA model configuration by name

    Args:
        model_name: Model identifier (e.g., 'glm5', 'kimi-k2.6')

    Returns:
        dict: Model configuration

    Raises:
        KeyError: If model name is not found
    """
    if model_name not in MLA_MODEL_CONFIG:
        available = ", ".join(MLA_MODEL_CONFIG.keys())
        raise KeyError(f"Unknown model: {model_name}. Available: {available}")
    return MLA_MODEL_CONFIG[model_name].copy()


def create_layout(model_config: dict, page_size_tokens: int = 64) -> MLALayout:
    """Create an MLALayout instance from model configuration

    Args:
        model_config: Model configuration dictionary with fields:
            - num_layers: Number of transformer layers
            - kv_lora_rank: KV LoRA rank dimension
            - qk_rope_head_dim: QK rope head dimension
            - index_head_dim: Indexer head dimension
            - kv_precision_bytes: Precision in bytes (BF16=2, INT8=1)
        page_size_tokens: Tokens per page (default 64)

    Returns:
        MLALayout: Layout instance for MLA architecture

    Raises:
        ValueError: If model configuration is invalid
    """
    required_fields = ['num_layers', 'kv_lora_rank', 'qk_rope_head_dim',
                       'index_head_dim', 'kv_precision_bytes']
    for field in required_fields:
        if field not in model_config:
            raise ValueError(f"Missing required field: {field}")

    return MLALayout(
        num_layers=model_config['num_layers'],
        kv_lora_rank=model_config['kv_lora_rank'],
        qk_rope_head_dim=model_config['qk_rope_head_dim'],
        index_head_dim=model_config['index_head_dim'],
        precision_bytes=model_config['kv_precision_bytes'],
        page_size_tokens=page_size_tokens,
    )
