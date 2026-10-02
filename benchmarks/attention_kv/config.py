import json
import math
from dataclasses import dataclass, field
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

# Sub-configs that may hold the language model parameters
NESTED_CONFIG_KEYS = ("text_config", "llm_config", "language_config")


def canonical_torch_dtype(name):
    """Normalise a dtype spelling into the name torch takes directly."""
    key = str(name).lower()
    if key not in DTYPE_CANONICAL:
        raise ValueError(f"unrecognised dtype {name!r}, extend DTYPE_CANONICAL")
    return DTYPE_CANONICAL[key]


def dtype_bytes_of(name):
    return DTYPE_BYTES[canonical_torch_dtype(name)]


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
    config_source: str

    def kv_bytes_per_token(self, tp_size: int = 1) -> tuple:
        """Return (aggregate, per_rank). aggregate covers every KV head;
        per_rank is what one tensor-parallel rank holds."""
        if self.is_mla:
            raise ValueError(
                "MLA uses a latent KV layout that differs from standard GQA, "
                "and this benchmark does not mix the two"
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

    is_mla = any(body.get(field_name) is not None for field_name in MLA_FIELDS)

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
        is_mla=is_mla,
        config_source=f"{source} [{head_dim_source}]",
    )


def read_vocab_size(model_path, body=None):
    """Vocabulary size, through the same nested-config lookup as the KV shape."""
    if body is None:
        _, body, _ = load_config_document(model_path)
    if "vocab_size" not in body:
        raise ValueError(f"{model_path} has no vocab_size in its config.json")
    return int(body["vocab_size"])


@dataclass(frozen=True)
class WorkloadCase:
    """One end-to-end load point."""

    input_len: int
    output_len: int
    num_requests: int
    max_concurrency: int

    def as_dict(self) -> dict:
        return {
            "case_id": self.case_id,
            "input_len": self.input_len,
            "output_len": self.output_len,
            "num_requests": self.num_requests,
            "max_concurrency": self.max_concurrency,
        }

    @property
    def case_id(self) -> str:
        return (
            f"in{self.input_len}-out{self.output_len}"
            f"-n{self.num_requests}-c{self.max_concurrency}"
        )


CACHE_TIERS = ("gpu_only", "host", "mooncake")
HIT_PATTERNS = ("cold_miss", "full_hit", "partial_hit", "multiturn")

# Cache tiers, described in words for the report
CACHE_TIER_DESCRIPTION = {
    "gpu_only": "KV cache in GPU memory only, no external cache",
    "host": "GPU memory plus host memory (HiCache L1 + L2)",
    "mooncake": "GPU memory plus host memory plus Mooncake Store (HiCache L1 + L2 + L3)",
}


@dataclass(frozen=True)
class E2EConfig:
    """A cache tier crossed with a hit pattern, that is one end-to-end setup."""

    cache_tier: str
    hit_pattern: str
    rounds: int = 1
    repeats: int = 3
    warmups: int = 1
    seed: int = 42

    def __post_init__(self):
        if self.cache_tier not in CACHE_TIERS:
            raise ValueError(f"unknown cache tier {self.cache_tier!r}")
        if self.hit_pattern not in HIT_PATTERNS:
            raise ValueError(f"unknown hit pattern {self.hit_pattern!r}")

    @property
    def config_id(self) -> str:
        return f"{self.cache_tier}.{self.hit_pattern}"

    def as_dict(self) -> dict:
        return {
            "config_id": self.config_id,
            "cache_tier": self.cache_tier,
            "cache_tier_description": CACHE_TIER_DESCRIPTION[self.cache_tier],
            "hit_pattern": self.hit_pattern,
            "rounds": self.rounds,
            "repeats": self.repeats,
            "warmups": self.warmups,
            "seed": self.seed,
        }


# Load scale for the full matrix; longer sequences are added with --input-lens
BASE_INPUT_LENS = (128, 512, 2048, 8192)
BASE_OUTPUT_LENS = (1, 128)


def build_workload_cases(
    input_lens,
    output_lens=(1,),
    num_requests=20,
    concurrencies=(1,),
) -> list:
    cases = []
    for input_len in input_lens:
        for output_len in output_lens:
            for concurrency in concurrencies:
                cases.append(
                    WorkloadCase(
                        input_len=input_len,
                        output_len=output_len,
                        num_requests=num_requests,
                        max_concurrency=concurrency,
                    )
                )
    return cases


def build_e2e_configs(
    tiers=CACHE_TIERS,
    patterns=HIT_PATTERNS,
    repeats=3,
    warmups=1,
    rounds=5,
    seed=42,
) -> list:
    configs = []
    for tier in tiers:
        for pattern in patterns:
            configs.append(
                E2EConfig(
                    cache_tier=tier,
                    hit_pattern=pattern,
                    rounds=rounds if pattern == "multiturn" else 1,
                    repeats=repeats,
                    warmups=warmups,
                    seed=seed,
                )
            )
    return configs


@dataclass
class BenchmarkPlan:
    """The full load matrix for one run; --dry-run prints this object."""

    quick: bool
    model_path: str
    backend: str
    tp_size: int
    page_size: int
    input_lens: tuple
    output_lens: tuple
    num_requests: int
    concurrencies: tuple
    repeats: int
    warmups: int
    rounds: int
    seed: int
    kernel: bool
    e2e: bool
    tiers: tuple
    patterns: tuple
    cases: list = field(default_factory=list)
    e2e_configs: list = field(default_factory=list)

    def finalize(self) -> "BenchmarkPlan":
        self.cases = build_workload_cases(
            input_lens=self.input_lens,
            output_lens=self.output_lens,
            num_requests=self.num_requests,
            concurrencies=self.concurrencies,
        )
        self.e2e_configs = build_e2e_configs(
            tiers=self.tiers,
            patterns=self.patterns,
            repeats=self.repeats,
            warmups=self.warmups,
            rounds=self.rounds,
            seed=self.seed,
        )
        return self

    @property
    def total_e2e_runs(self) -> int:
        return len(self.cases) * len(self.e2e_configs) * self.repeats

    def tp_size_display(self) -> str:
        # A real run resolves the tensor parallel size before the plan is built;
        # an unresolved value stays only for --dry-run, which starts no server and
        # therefore has no visible GPUs to count.
        if self.tp_size is None:
            return "auto (visible GPU count)"
        return str(self.tp_size)

    def as_dict(self) -> dict:
        return {
            "quick": self.quick,
            "model_path": self.model_path,
            "backend": self.backend,
            "tp_size": self.tp_size,
            "page_size": self.page_size,
            "input_lens": list(self.input_lens),
            "output_lens": list(self.output_lens),
            "num_requests": self.num_requests,
            "concurrencies": list(self.concurrencies),
            "repeats": self.repeats,
            "warmups": self.warmups,
            "rounds": self.rounds,
            "seed": self.seed,
            "run_kernel": self.kernel,
            "run_e2e": self.e2e,
            "tiers": list(self.tiers),
            "hit_patterns": list(self.patterns),
            "num_workload_cases": len(self.cases),
            "num_e2e_configs": len(self.e2e_configs),
            "total_e2e_runs": self.total_e2e_runs,
        }

    def describe(self) -> str:
        def row(label, value):
            return f"  {label:<17}{value}"

        lines = []
        lines.append("Load matrix")
        lines.append(row("model path", self.model_path))
        lines.append(row("backend", self.backend))
        lines.append(row("TP", self.tp_size_display()))
        lines.append(row("page/block size", self.page_size))
        lines.append(row("input lengths", ", ".join(str(v) for v in self.input_lens)))
        lines.append(row("output lengths", ", ".join(str(v) for v in self.output_lens)))
        lines.append(row("requests", self.num_requests))
        lines.append(row("concurrency", ", ".join(str(v) for v in self.concurrencies)))
        lines.append(row("repeats", f"{self.repeats} (formal results need at least 3)"))
        lines.append(row("warmups", self.warmups))
        lines.append(row("multiturn rounds", self.rounds))
        lines.append(row("seed", self.seed))
        lines.append(row("kernel phase", "enabled" if self.kernel else "skipped"))
        lines.append(row("e2e phase", "enabled" if self.e2e else "skipped"))
        lines.append("")
        lines.append(f"  cache tier x hit pattern ({len(self.e2e_configs)} setups)")
        for config in self.e2e_configs:
            lines.append(
                f"    {config.config_id:<24} rounds={config.rounds} "
                f"repeats={config.repeats} warmups={config.warmups}"
            )
        lines.append("")
        lines.append(f"  load points ({len(self.cases)})")
        for case in self.cases:
            lines.append(f"    {case.case_id:<28} {case.num_requests} requests")
        lines.append("")
        lines.append(
            f"  total e2e runs = {len(self.cases)} load points x "
            f"{len(self.e2e_configs)} setups x {self.repeats} repeats "
            f"= {self.total_e2e_runs}"
        )
        return "\n".join(lines)


def build_plan_from_args(args) -> BenchmarkPlan:
    """Expand the CLI arguments into the full load matrix."""
    if args.quick and args.full:
        raise ValueError("--quick and --full are mutually exclusive")
    if not args.quick and not args.full:
        raise ValueError("exactly one of --quick or --full is required")

    if args.quick:
        input_lens = (128, 512)
        output_lens = (1,)
        num_requests = 4
        concurrencies = (1,)
        repeats = 1
        warmups = 1
        rounds = 3
        tiers = ("gpu_only", "host")
        patterns = ("cold_miss", "full_hit", "partial_hit")
    else:
        input_lens = BASE_INPUT_LENS
        output_lens = BASE_OUTPUT_LENS
        num_requests = 20
        concurrencies = (1, 4)
        repeats = 3
        warmups = 1
        rounds = 5
        tiers = CACHE_TIERS
        patterns = HIT_PATTERNS

    # Test for None explicitly: a truthiness test would silently drop a supplied
    # 0 and fall back to the default, so the user's argument would do nothing
    if args.input_lens is not None:
        input_lens = tuple(args.input_lens)
    if args.output_len is not None:
        output_lens = (args.output_len,)
    if args.requests is not None:
        num_requests = args.requests
    if args.repeats is not None:
        repeats = args.repeats
    if args.rounds is not None:
        rounds = args.rounds
    if args.tiers is not None:
        tiers = tuple(args.tiers)
    if args.patterns is not None:
        patterns = tuple(args.patterns)
    if args.concurrency is not None:
        concurrencies = tuple(args.concurrency)

    # Validate every numeric argument up front, so a 0 or a negative value cannot
    # turn into a division by zero, an empty sample or a hard-to-place exception.
    # An empty --tp-size is not a number but the request to use the visible GPU
    # count, which only a run on hardware can resolve, so it is checked later.
    if args.tp_size is not None:
        check_positive("--tp-size", args.tp_size)
    check_positive("--page-size", args.page_size)
    check_positive("--requests", num_requests)
    check_positive("--repeats", repeats)
    check_positive("--rounds", rounds)
    for value in input_lens:
        check_positive("each --input-lens entry", value)
    for value in output_lens:
        check_positive("--output-len", value)
    for value in concurrencies:
        check_positive("each --concurrency entry", value)
    for name, values in (
        ("--input-lens", input_lens),
        ("--output-len", output_lens),
        ("--concurrency", concurrencies),
        ("--tiers", tiers),
        ("--patterns", patterns),
    ):
        if not values:
            raise ValueError(f"{name} must not be empty")

    plan = BenchmarkPlan(
        quick=bool(args.quick),
        model_path=args.model,
        backend=args.backend,
        tp_size=args.tp_size,
        page_size=args.page_size,
        input_lens=input_lens,
        output_lens=output_lens,
        num_requests=num_requests,
        concurrencies=concurrencies,
        repeats=repeats,
        warmups=warmups,
        rounds=rounds,
        seed=args.seed,
        kernel=not args.skip_kernel,
        e2e=not args.skip_e2e,
        tiers=tiers,
        patterns=patterns,
    )
    return plan.finalize()


@dataclass(frozen=True)
class PageTableSpec:
    """Metadata size paged attention needs; kernel and e2e share the conversion."""

    seq_len: int
    page_size: int

    @property
    def num_pages(self) -> int:
        return math.ceil(self.seq_len / self.page_size)

    @property
    def last_page_len(self) -> int:
        remainder = self.seq_len % self.page_size
        return self.page_size if remainder == 0 else remainder
