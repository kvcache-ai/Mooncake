"""
MLA (Multi-head Latent Attention) KVCache Layout

Implements the KVLayout interface for MLA architecture.
"""

from typing import Iterator, Any

from .interface import KVLayout, StorageAccess


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


class MLALayout(KVLayout):
    """MLA (Multi-head Latent Attention) KVCache layout

    MLA Architecture:
    - Each hash_id corresponds to a 512-token chunk
    - Each hash_id maps to ONE complete entry containing all layers
    - Entry contains KV + Indexer data for all layers for 512 tokens
    - Value size is fixed per hash_id (includes all layers)

    Value Size Calculation (per hash_id entry):
        per_layer_size = 512 × (kv_lora_rank + qk_rope_head_dim + index_head_dim) × precision_bytes
        value_size = per_layer_size × num_layers

    For GLM-5 with 512 tokens per entry:
        per_layer_size = 512 × (512 + 64 + 128) × 2 = 720,896 bytes
        value_size = 720,896 × 78 = 56,229,888 bytes = 53.6 MiB

    Key Pattern: hash_id → single entry (all layers included)
    Total Keys = len(hash_ids)

    Used in: GLM-5, Kimi-K2.6
    """

    def __init__(self, num_layers: int, kv_lora_rank: int, qk_rope_head_dim: int,
                 index_head_dim: int, precision_bytes: int, page_size_tokens: int = 512):
        """Initialize MLA layout

        Args:
            num_layers: Number of transformer layers
            kv_lora_rank: KV LoRA rank dimension
            qk_rope_head_dim: QK rope head dimension
            index_head_dim: Indexer head dimension
            precision_bytes: Precision in bytes (BF16=2, INT8=1, INT4=0.5)
            page_size_tokens: Tokens per page (default: 512)
        """
        self.num_layers = num_layers
        self.kv_lora_rank = kv_lora_rank
        self.qk_rope_head_dim = qk_rope_head_dim
        self.index_head_dim = index_head_dim
        self.precision_bytes = precision_bytes
        self.page_size_tokens = page_size_tokens

        # Calculate fixed value size per entry (per hash_id)
        # Each entry contains KV + Indexer for all layers for page_size_tokens
        # Per layer: page_size_tokens × (kv_lora_rank + qk_rope_head_dim + index_head_dim) × precision_bytes
        # Total: per_layer_size × num_layers
        per_layer_size = page_size_tokens * (kv_lora_rank + qk_rope_head_dim + index_head_dim) * precision_bytes
        self.value_size_bytes = per_layer_size * num_layers

        # Store page_size for backward compatibility
        self.page_size = self.value_size_bytes

    def get_operations(self, request: Any) -> Iterator[StorageAccess]:
        """Generate storage access requirements for a request

        For MLA architecture:
        - Each hash_id corresponds to one complete page (512 tokens, all layers)
        - Generate one access requirement per hash_id

        Args:
            request: KVCache request with hash_ids, input_length, output_length

        Yields:
            StorageAccess: Page access requirements
        """
        for hash_id in request.hash_ids:
            yield StorageAccess(
                page_id=hash_id,
                offset_in_page=0,
                length=self.value_size_bytes
            )


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
