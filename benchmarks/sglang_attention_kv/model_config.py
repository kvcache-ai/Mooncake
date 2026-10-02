# The model's KV cache shape, read from its config.json, and the classification
# that decides whether this benchmark can describe the model at all.
#
# Nothing here touches a GPU library, so --help and --dry-run work on a machine
# without one.

import json
from dataclasses import dataclass
from pathlib import Path

# dtype aliases to canonical names. Configs spell bf16 and bfloat16 both ways,
# and tensor construction needs the canonical form, so normalise first.
DTYPE_CANONICAL = {
    "float32": "float32",
    "fp32": "float32",
    "float": "float32",
    "bfloat16": "bfloat16",
    "bf16": "bfloat16",
    "float16": "float16",
    "fp16": "float16",
    "half": "float16",
    "float8_e4m3fn": "float8_e4m3fn",
    "float8_e5m2": "float8_e5m2",
    "fp8": "float8_e4m3fn",
    "int8": "int8",
}

DTYPE_BYTES = {
    "float32": 4,
    "bfloat16": 2,
    "float16": 2,
    "float8_e4m3fn": 1,
    "float8_e5m2": 1,
    "int8": 1,
}

# Fields that mark a model as using MLA
MLA_FIELDS = ("kv_lora_rank", "qk_rope_head_dim")

# Fields that mark a model whose layers do not all keep a dense, full-length KV:
# sliding-window layers keep a window, linear-attention layers are not attention
# at all, and an encoder-decoder model reads a second cache for cross attention.
# A model with any of them does not fit one per-token byte count.
HYBRID_FIELDS = (
    "attention_types",
    "sliding_window_pattern",
    "linear_attn_config",
    "linear_attention_config",
    "mamba_d_state",
    "hybrid_override_pattern",
)

# layer_types entries that mean "this layer keeps a full-length KV"
DENSE_LAYER_TYPES = ("full_attention", "full", "attention", "self_attention")

# Sub-configs that may hold the language model parameters
NESTED_CONFIG_KEYS = ("text_config", "llm_config", "language_config")


def classify_attention_layout(body: dict) -> tuple:
    """Return (layout, evidence). The layout is "dense" when every layer keeps a
    full-length KV, "mla" for the latent layout, and "hybrid" for anything else.
    Only the dense layout has one per-token KV byte count."""
    mla_fields = [name for name in MLA_FIELDS if body.get(name) is not None]
    if mla_fields:
        return "mla", ", ".join(mla_fields)

    layer_types = body.get("layer_types")
    if isinstance(layer_types, list):
        sparse = sorted(
            {str(entry) for entry in layer_types if str(entry) not in DENSE_LAYER_TYPES}
        )
        if sparse:
            return "hybrid", f"layer_types={sparse}"

    window = body.get("sliding_window")
    if window is not None and body.get("use_sliding_window") is not False:
        return "hybrid", f"sliding_window={window}"

    interval = body.get("full_attention_interval")
    if isinstance(interval, int) and interval > 1:
        return "hybrid", f"full_attention_interval={interval}"

    if body.get("is_encoder_decoder") is True:
        return "hybrid", "is_encoder_decoder=True, cross attention reads a second cache"

    mixed = [name for name in HYBRID_FIELDS if body.get(name) is not None]
    if mixed:
        return "hybrid", ", ".join(mixed)

    return "dense", "every layer keeps a full-length KV"


def canonical_torch_dtype(name):
    """Normalise a dtype spelling into the name torch takes directly."""
    key = str(name).lower()
    if key not in DTYPE_CANONICAL:
        raise ValueError(f"unrecognised dtype {name!r}, extend DTYPE_CANONICAL")
    return DTYPE_CANONICAL[key]


def check_positive(name, value):
    """Numeric arguments must be positive, or they become a division by zero or
    an empty sample further down."""
    if value is None or value <= 0:
        raise ValueError(f"{name} must be positive, got {value!r}")
    return value


@dataclass(frozen=True)
class ModelKVConfig:
    """KV cache shape read from the model's config.json."""

    model_path: str
    architectures: tuple
    model_type: str
    num_layers: int
    num_attention_heads: int
    num_key_value_heads: int
    head_dim: int
    hidden_size: int
    vocab_size: int
    torch_dtype: str
    dtype_bytes: int
    is_mla: bool
    attention_layout: str
    attention_layout_evidence: str
    config_source: str

    def kv_bytes_per_token(self, tp_size: int = 1) -> tuple:
        """Return (aggregate, per_rank). aggregate covers every KV head;
        per_rank is what one tensor-parallel rank holds."""
        if self.attention_layout != "dense":
            raise ValueError(
                f"attention layout is {self.attention_layout!r} "
                f"({self.attention_layout_evidence}); the per-token KV byte count "
                f"below describes a model whose layers all keep a full-length KV, "
                f"so this benchmark does not measure this model"
            )
        if self.num_key_value_heads % tp_size != 0:
            raise ValueError(
                f"num_key_value_heads={self.num_key_value_heads} "
                f"is not divisible by tp_size={tp_size}"
            )
        aggregate = (
            self.num_layers
            * 2
            * self.num_key_value_heads
            * self.head_dim
            * self.dtype_bytes
        )
        return aggregate, aggregate // tp_size

    def kv_bytes_per_page(self, page_size: int, tp_size: int = 1) -> tuple:
        aggregate, per_rank = self.kv_bytes_per_token(tp_size)
        return aggregate * page_size, per_rank * page_size

    def as_dict(self) -> dict:
        return {
            "model_path": self.model_path,
            "architectures": list(self.architectures),
            "model_type": self.model_type,
            "num_layers": self.num_layers,
            "num_attention_heads": self.num_attention_heads,
            "num_key_value_heads": self.num_key_value_heads,
            "head_dim": self.head_dim,
            "hidden_size": self.hidden_size,
            "vocab_size": self.vocab_size,
            "torch_dtype": self.torch_dtype,
            "dtype_bytes": self.dtype_bytes,
            "is_mla": self.is_mla,
            "attention_layout": self.attention_layout,
            "attention_layout_evidence": self.attention_layout_evidence,
            "config_source": self.config_source,
            "kv_layout": "NHD",
        }


def _pick_config_dict(raw: dict, source: str) -> tuple:
    """Handle multimodal models that keep the language model parameters nested."""
    if "num_hidden_layers" in raw:
        return raw, source
    for key in NESTED_CONFIG_KEYS:
        nested = raw.get(key)
        if isinstance(nested, dict) and "num_hidden_layers" in nested:
            return nested, f"{source}:{key}"
    raise ValueError(f"{source} has no num_hidden_layers, cannot determine the depth")


def load_config_document(model_path):
    """Read config.json and locate the layer that holds the language model
    parameters.

    load_model_kv_config and the vocabulary read share this entry point so the
    two cannot disagree about nested configs.
    """
    path = Path(model_path)
    config_file = path / "config.json" if path.is_dir() else path
    if not config_file.is_file():
        raise FileNotFoundError(f"model config not found at {config_file}")
    with config_file.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    body, source = _pick_config_dict(raw, str(config_file))
    return path, body, source


def _dtype_from_config(raw: dict) -> str:
    for key in ("torch_dtype", "dtype", "torch_dtype_str"):
        value = raw.get(key)
        if isinstance(value, str):
            return value
    raise ValueError("config.json has no torch_dtype/dtype field")


def load_model_kv_config(model_path, dtype_override=None) -> ModelKVConfig:
    """Read config.json and resolve the KV cache shape. dtype_override replaces
    the declared precision, for instance when the server stores KV as fp8."""
    path, body, source = load_config_document(model_path)

    num_layers = int(body["num_hidden_layers"])
    num_attention_heads = int(body["num_attention_heads"])
    num_key_value_heads = int(body.get("num_key_value_heads", num_attention_heads))
    hidden_size = int(body.get("hidden_size", 0))

    head_dim = body.get("head_dim")
    if head_dim is None:
        if hidden_size <= 0:
            raise ValueError("config.json has neither head_dim nor hidden_size")
        head_dim = hidden_size // num_attention_heads
        head_dim_source = "hidden_size // num_attention_heads"
    else:
        head_dim = int(head_dim)
        head_dim_source = "config.json head_dim"

    attention_layout, attention_layout_evidence = classify_attention_layout(body)

    declared_dtype = str(dtype_override or _dtype_from_config(body))
    torch_dtype = canonical_torch_dtype(declared_dtype)

    return ModelKVConfig(
        model_path=str(path),
        architectures=tuple(body.get("architectures", [])),
        model_type=str(body.get("model_type", "")),
        num_layers=num_layers,
        num_attention_heads=num_attention_heads,
        num_key_value_heads=num_key_value_heads,
        head_dim=head_dim,
        hidden_size=hidden_size,
        vocab_size=read_vocab_size(model_path, body),
        torch_dtype=torch_dtype,
        dtype_bytes=DTYPE_BYTES[torch_dtype],
        is_mla=attention_layout == "mla",
        attention_layout=attention_layout,
        attention_layout_evidence=attention_layout_evidence,
        config_source=f"{source} [{head_dim_source}]",
    )


def read_vocab_size(model_path, body=None):
    """Vocabulary size, through the same nested-config lookup as the KV shape."""
    if body is None:
        _, body, _ = load_config_document(model_path)
    if "vocab_size" not in body:
        raise ValueError(f"{model_path} has no vocab_size in its config.json")
    return int(body["vocab_size"])
