# The step matrix and the two things a step is made of: a shape, which is what the
# matrix varies, and the model's layer count, per-rank head counts and dtype bound
# to it.
#
# A step is a batch of sequences, each with a cached prefix (its history) and a
# number of tokens this step computes (its queries). The three modes are the three
# shapes that has in a server:
#
#   prefill  history 0,        queries = the whole sequence
#   extend   history = cached, queries = one chunk   (chunked prefill, or the
#                                                     suffix a reused prefix leaves)
#   decode   history = cached, queries = 1
#
# History length and query length are separate axes: an extend step sweeps its
# queries over --chunk-lens while its history comes from --input-lens.

from dataclasses import asdict, dataclass, field
import os

from .model_config import DTYPE_BYTES, canonical_torch_dtype, check_positive

MODES = ("prefill", "extend", "decode")

# Page layouts: consecutive physical pages, or the gaps of a pool that churned.
LAYOUTS = ("contiguous", "random")

# KV precisions the paged attention kernels this benchmark calls support.
KERNEL_DTYPES = ("bfloat16", "float16")

# The branches of FlashInferAttnBackend this benchmark replays. They live here,
# with the rest of the configuration vocabulary, so --help and --dry-run work on
# a machine with no GPU libraries installed.
BRANCH_RAGGED_NO_PREFIX = "ragged_no_prefix"
BRANCH_RAGGED_PREFIX_MERGE = "ragged_prefix_merge"
BRANCH_PAGED_EXTEND = "paged_extend"
BRANCH_PAGED_DECODE = "paged_decode"

# The two an extend step can take: the one a server runs while
# SGLANG_FLASHINFER_USE_PAGED is False (the default), and the one it runs with
# that variable set.
BRANCH_CHOICES = (BRANCH_RAGGED_PREFIX_MERGE, BRANCH_PAGED_EXTEND)

BRANCH_DETAIL = {
    BRANCH_RAGGED_NO_PREFIX: (
        "one ragged prefill call over the step's own K/V, then the KV write"
    ),
    BRANCH_RAGGED_PREFIX_MERGE: (
        "ragged suffix attention, paged history attention, merge_state, then the KV write"
    ),
    BRANCH_PAGED_EXTEND: "KV write, then one paged prefill call over the whole context",
    BRANCH_PAGED_DECODE: "KV write, then the paged decode call over the whole context",
}

# Which branches write the KV cache before they read it. The two ragged branches
# compute the attention first and save the cache afterwards.
BRANCHES_THAT_WRITE_FIRST = (BRANCH_PAGED_EXTEND, BRANCH_PAGED_DECODE)


def use_paged_env_value():
    """What SGLANG_FLASHINFER_USE_PAGED is set to, or None when it is not set.

    None and "0" mean different things: with the variable unset the matrix decides
    the extend branch, and with it set the environment already decided, so a
    conflicting choice is an error rather than a silent override.
    """
    raw = os.environ.get("SGLANG_FLASHINFER_USE_PAGED")
    if raw is None:
        return None
    return raw.lower() in ("1", "true", "yes")


def use_paged_default() -> bool:
    """What SGLANG_FLASHINFER_USE_PAGED is set to in this process, so a record
    says which server configuration it describes."""
    return use_paged_env_value() or False


def branch_for_paged(paged: bool) -> str:
    """The extend branch that variable selects."""
    return BRANCH_PAGED_EXTEND if paged else BRANCH_RAGGED_PREFIX_MERGE


def resolve_extend_branch(cli_choice):
    """The extend branch a run replays, from the one place that configures it.

    SGLANG_FLASHINFER_USE_PAGED is what a server sets and what the backend reads;
    --extend-branch is what a run passes. When the variable is set, it decides and
    a conflicting --extend-branch is refused, so a record can never state a branch
    the environment does not agree with.
    """
    env = use_paged_env_value()
    if cli_choice is not None and cli_choice not in BRANCH_CHOICES:
        raise ValueError(
            f"unknown extend branch {cli_choice!r}, expected one of {BRANCH_CHOICES}"
        )
    if env is None:
        return cli_choice or BRANCH_RAGGED_PREFIX_MERGE
    from_env = branch_for_paged(env)
    if cli_choice is not None and cli_choice != from_env:
        raise ValueError(
            f"--extend-branch {cli_choice} disagrees with "
            f"SGLANG_FLASHINFER_USE_PAGED={int(env)}, which selects {from_env}; "
            f"unset the variable to choose the branch on the command line"
        )
    return from_env


def branch_for_mode(mode, extend_branch):
    """Select the wrapper path for the configured prefill/extend lane."""
    if mode == "prefill" and extend_branch != BRANCH_PAGED_EXTEND:
        return BRANCH_RAGGED_NO_PREFIX
    if mode == "decode":
        return BRANCH_PAGED_DECODE
    return extend_branch


# Load scale for the --full matrix; longer sequences are added with --input-lens
BASE_INPUT_LENS = (128, 512, 2048, 8192)

# Query lengths of an extend step, over the history lengths above
BASE_CHUNK_LENS = (128, 512, 2048)

# Sequences per step. A serving step holds several sequences and they rarely end
# at the same length, so batch 4 is measured with one length and with lengths of
# 1, 1/2, 1/4 and 1/8 of it.
BASE_BATCH_SIZES = (1, 4)

# Tokens per page in the paged KV cache. 1 is what SGLang resolves to when
# --page-size is not given, and 16, 32, 64 and 128 are the values its own paged
# backends accept.
BASE_PAGE_SIZES = (1, 16, 32, 64, 128)

BASE_MODES = ("prefill", "extend", "decode")

# Iterations per step, and what a formal result needs. They live here so --help
# and --dry-run work on a machine with no GPU libraries installed.
DEFAULT_WARMUP = 10
DEFAULT_TIMED = 100
MINIMUM_WARMUP = 10
MINIMUM_TIMED = 100


def ragged_lengths(length, batch_size):
    """The lengths of one ragged batch, longest first."""
    return tuple(max(1, length >> index) for index in range(batch_size))


@dataclass(frozen=True)
class StepShape:
    """What the matrix varies: the step's history and query lengths, its page size
    and its layout."""

    mode: str
    prefix_lens: tuple
    new_lens: tuple
    page_size: int
    layout: str

    def __post_init__(self):
        if self.mode not in MODES:
            raise ValueError(f"unknown mode {self.mode!r}, expected one of {MODES}")
        if self.layout not in LAYOUTS:
            raise ValueError(f"unknown layout {self.layout!r}, expected {LAYOUTS}")
        if len(self.prefix_lens) != len(self.new_lens):
            raise ValueError(
                f"prefix_lens and new_lens must have one entry per sequence, got "
                f"{len(self.prefix_lens)} and {len(self.new_lens)}"
            )
        if not self.new_lens:
            raise ValueError("a step needs at least one sequence")
        if self.mode == "prefill" and any(self.prefix_lens):
            raise ValueError("a prefill step has no cached prefix")
        if self.mode == "decode" and any(length != 1 for length in self.new_lens):
            raise ValueError("a decode step computes exactly one token per sequence")

    @property
    def batch_size(self):
        return len(self.new_lens)

    @property
    def context_lens(self):
        return tuple(
            prefix + new for prefix, new in zip(self.prefix_lens, self.new_lens)
        )

    @property
    def new_tokens(self):
        return sum(self.new_lens)

    @property
    def context_tokens(self):
        return sum(self.context_lens)

    @property
    def pages_per_seq(self):
        return tuple(
            (length + self.page_size - 1) // self.page_size
            for length in self.context_lens
        )

    @property
    def pages(self):
        return sum(self.pages_per_seq)

    @property
    def padding_tokens(self):
        """Capacity the step's pages hold beyond its valid tokens, from a last page
        that is not full. It is unused allocation rather than something the kernel
        reads: the CSR stream names the valid tokens, one index each, at every page
        size."""
        return self.pages * self.page_size - self.context_tokens

    @property
    def attention_pairs(self):
        """Query-key pairs the step evaluates. A causal mask aligned to the end of
        the context leaves each query the history and everything computed before
        it."""
        return sum(
            new * prefix + new * (new + 1) // 2
            for prefix, new in zip(self.prefix_lens, self.new_lens)
        )

    @staticmethod
    def _length_tag(lengths):
        low, high = min(lengths), max(lengths)
        return str(low) if low == high else f"{low}-{high}"

    @property
    def label(self):
        """Unique across the matrix: mode, lengths, batch, page size, layout."""
        parts = [self.mode]
        if self.mode == "prefill":
            parts.append(f"len{self._length_tag(self.new_lens)}")
        elif self.mode == "extend":
            parts.append(f"pref{self._length_tag(self.prefix_lens)}")
            parts.append(f"chunk{self._length_tag(self.new_lens)}")
        else:
            parts.append(f"ctx{self._length_tag(self.context_lens)}")
        parts.append(f"bs{self.batch_size}")
        parts.append(f"ps{self.page_size}")
        if self.layout != "contiguous":
            parts.append(self.layout)
        return "_".join(parts)

    def as_dict(self):
        body = asdict(self)
        body["prefix_lens"] = list(self.prefix_lens)
        body["new_lens"] = list(self.new_lens)
        body.update(
            {
                "label": self.label,
                "batch_size": self.batch_size,
                "context_lens": list(self.context_lens),
                "new_tokens": self.new_tokens,
                "context_tokens": self.context_tokens,
                "pages_per_seq": list(self.pages_per_seq),
                "pages": self.pages,
                "padding_tokens": self.padding_tokens,
                "attention_pairs": self.attention_pairs,
            }
        )
        return body


@dataclass(frozen=True)
class KernelCase(StepShape):
    """A shape bound to the model: its layer count, per-rank head counts and
    dtype. Head counts are per rank, because that is what one GPU reads."""

    num_layers: int
    num_qo_heads: int
    num_kv_heads: int
    head_dim: int
    dtype: str

    def __post_init__(self):
        super().__post_init__()
        if self.num_kv_heads < 1:
            raise ValueError(
                f"a step needs at least one KV head, got {self.num_kv_heads}"
            )
        # Several query heads share one KV head, and the reference attention reads
        # that group with repeat_interleave. A head count that is not a whole
        # number of groups would silently truncate query heads, so the ratio is
        # checked here rather than left to the division.
        if self.num_qo_heads % self.num_kv_heads != 0:
            raise ValueError(
                f"num_qo_heads={self.num_qo_heads} is not a whole number of groups "
                f"of num_kv_heads={self.num_kv_heads}; the group ratio would "
                f"truncate query heads"
            )

    def group_ratio(self):
        """Query heads per KV head: how many queries one KV head serves, which is
        the number of times the reference attention reads it."""
        return self.num_qo_heads // self.num_kv_heads

    @property
    def dtype_bytes(self):
        if self.dtype not in DTYPE_BYTES:
            raise ValueError(
                f"no byte width for dtype {self.dtype!r}, extend DTYPE_BYTES"
            )
        return DTYPE_BYTES[self.dtype]

    def kv_bytes(self, tokens):
        """The KV of `tokens` tokens on one rank: K and V, every layer."""
        return (
            self.num_layers
            * 2
            * tokens
            * self.num_kv_heads
            * self.head_dim
            * self.dtype_bytes
        )

    def kv_bytes_written(self):
        """Bytes this step writes: only the tokens it computes."""
        return self.kv_bytes(self.new_tokens)

    def kv_page_capacity_bytes(self):
        """The capacity the step's pages occupy: whole pages, so a last page that
        is not full counts with the capacity it holds unused and `padding_tokens`
        states how many tokens that is. This is an allocation figure, not a read:
        the paged side reads the valid tokens, which `SglangStep.read_bytes` splits
        into the paged side and the ragged side."""
        return self.kv_bytes(self.pages * self.page_size)

    def attention_flops(self):
        """QK^T and PV are one multiply-add each, so a query-key pair costs 4
        floating point operations; every layer does that work."""
        return (
            self.num_layers
            * 4
            * self.attention_pairs
            * self.num_qo_heads
            * self.head_dim
        )

    def as_dict(self):
        body = super().as_dict()
        body.update(
            {
                "kv_bytes_written": self.kv_bytes_written(),
                "kv_page_capacity_bytes": self.kv_page_capacity_bytes(),
                "attention_flops": self.attention_flops(),
                "kv_layout": "NHD",
            }
        )
        return body


def assert_dense_sharding(model_config, tp_size):
    """Check the model is one this benchmark can describe and that the sharding
    divides evenly; return the per-rank head counts.

    Both head counts have to divide by tp_size: with num_attention_heads not
    dividing, integer division silently drops heads and the measurement covers a
    shape the model does not have.
    """
    if model_config.attention_layout != "dense":
        raise ValueError(
            f"the kernel benchmark measures models whose layers all keep a "
            f"full-length KV cache; this one is {model_config.attention_layout} "
            f"({model_config.attention_layout_evidence})"
        )
    if model_config.num_key_value_heads % tp_size != 0:
        raise ValueError(
            f"num_key_value_heads={model_config.num_key_value_heads} "
            f"is not divisible by tp_size={tp_size}"
        )
    if model_config.num_attention_heads % tp_size != 0:
        raise ValueError(
            f"num_attention_heads={model_config.num_attention_heads} "
            f"is not divisible by tp_size={tp_size}; integer division would "
            f"silently drop heads"
        )
    qo_heads = model_config.num_attention_heads // tp_size
    kv_heads = model_config.num_key_value_heads // tp_size
    # The same check the case makes, at the model level: the reference attention
    # reads each KV head num_qo_heads // num_kv_heads times, so the ratio has to be
    # a whole number of query heads per KV head.
    if kv_heads < 1 or qo_heads % kv_heads != 0:
        raise ValueError(
            f"this model gives {qo_heads} query heads per rank for {kv_heads} KV "
            f"heads per rank, which is not a whole group ratio"
        )
    return qo_heads, kv_heads


def _lens_for(mode, lengths, chunk_len):
    """The history and the queries of one step."""
    if mode == "prefill":
        return (0,) * len(lengths), tuple(lengths)
    if mode == "extend":
        return tuple(lengths), (chunk_len,) * len(lengths)
    return tuple(lengths), (1,) * len(lengths)


def _batch_shapes(length, batch_size):
    """One even-length batch, and for a batch above one a ragged one."""
    shapes = [(length,) * batch_size]
    if batch_size > 1:
        shapes.append(ragged_lengths(length, batch_size))
    return shapes


def build_shapes(
    modes, input_lens, chunk_lens, batch_sizes, page_sizes, layouts
) -> list:
    """Every step of the matrix: mode x history length x query length x batch shape
    x page size x layout. A decode step has one query length by definition, so
    --chunk-lens only multiplies the extend steps."""
    shapes = []
    for mode in modes:
        for length in input_lens:
            query_lens = chunk_lens if mode == "extend" else (None,)
            for chunk_len in query_lens:
                for batch_size in batch_sizes:
                    for lengths in _batch_shapes(length, batch_size):
                        prefix_lens, new_lens = _lens_for(mode, lengths, chunk_len)
                        for page_size in page_sizes:
                            for layout in layouts:
                                shapes.append(
                                    StepShape(
                                        mode=mode,
                                        prefix_lens=prefix_lens,
                                        new_lens=new_lens,
                                        page_size=page_size,
                                        layout=layout,
                                    )
                                )
    return shapes


def _shrink(lengths, cap):
    """Scale every length of one axis by the same factor, so the shape the
    reference runs keeps the batch's ratios.

    Clamping each length on its own would flatten a ragged batch — 8192, 4096,
    2048 and 1024 to 256 each — and the reference would then check a shape the
    step does not have. The factor is the smallest integer that brings the longest
    length inside the cap, so 8192, 4096, 2048 and 1024 become 256, 128, 64 and 32
    at a cap of 256.
    """
    longest = max(lengths)
    if longest <= cap:
        return tuple(lengths)
    factor = -(-longest // cap)
    return tuple(max(1, length // factor) for length in lengths)


def short_case(case, context_cap=256, chunk_cap=64) -> KernelCase:
    """The same step on short sequences, for the arithmetic reference: the check
    materialises a query by context score matrix, which a long context cannot fit,
    while the mapping and content checks stay on the full shape.

    Each axis is scaled by one factor, so the short step has the batch's lengths in
    the batch's proportions and its offsets are the scaled ones.
    """
    if case.mode == "prefill":
        prefix_lens = (0,) * case.batch_size
        new_lens = _shrink(case.new_lens, context_cap)
    elif case.mode == "extend":
        prefix_lens = _shrink(case.prefix_lens, context_cap)
        new_lens = _shrink(case.new_lens, chunk_cap)
    else:
        prefix_lens = _shrink(case.prefix_lens, context_cap)
        new_lens = (1,) * case.batch_size
    return KernelCase(
        mode=case.mode,
        prefix_lens=prefix_lens,
        new_lens=new_lens,
        page_size=case.page_size,
        layout=case.layout,
        num_layers=1,
        num_qo_heads=case.num_qo_heads,
        num_kv_heads=case.num_kv_heads,
        head_dim=case.head_dim,
        dtype=case.dtype,
    )


def build_cases(shapes, model_config, tp_size) -> list:
    """Bind every shape to the model's layer count, per-rank head counts and
    dtype."""
    qo_heads_per_rank, kv_heads_per_rank = assert_dense_sharding(model_config, tp_size)
    dtype = canonical_torch_dtype(model_config.torch_dtype)
    if dtype not in KERNEL_DTYPES:
        raise ValueError(
            f"the paged attention kernels this benchmark calls support "
            f"{KERNEL_DTYPES}; the model declares {dtype}"
        )
    return [
        KernelCase(
            mode=shape.mode,
            prefix_lens=shape.prefix_lens,
            new_lens=shape.new_lens,
            page_size=shape.page_size,
            layout=shape.layout,
            num_layers=model_config.num_layers,
            num_qo_heads=qo_heads_per_rank,
            num_kv_heads=kv_heads_per_rank,
            head_dim=model_config.head_dim,
            dtype=dtype,
        )
        for shape in shapes
    ]


@dataclass
class BenchmarkPlan:
    """The measured step matrix for one run; --dry-run prints this object."""

    quick: bool
    model_path: str
    tp_size: int
    modes: tuple
    input_lens: tuple
    chunk_lens: tuple
    batch_sizes: tuple
    page_sizes: tuple
    layouts: tuple
    seed: int
    shapes: list = field(default_factory=list)
    cases: list = field(default_factory=list)

    def finalize(self, model_config=None) -> "BenchmarkPlan":
        """Expand the matrix. Without a model config this stops at the shapes,
        which is what --dry-run prints on a machine with no GPU."""
        self.shapes = build_shapes(
            modes=self.modes,
            input_lens=self.input_lens,
            chunk_lens=self.chunk_lens,
            batch_sizes=self.batch_sizes,
            page_sizes=self.page_sizes,
            layouts=self.layouts,
        )
        if model_config is not None:
            self.cases = build_cases(self.shapes, model_config, self.tp_size)
        return self

    def tp_size_display(self) -> str:
        # A real run resolves the tensor parallel size before the plan is built;
        # an unresolved value stays only for --dry-run, which touches no GPU and
        # therefore has no visible GPUs to count.
        if self.tp_size is None:
            return "auto (visible GPU count)"
        return str(self.tp_size)

    def as_dict(self) -> dict:
        return {
            "quick": self.quick,
            "model_path": self.model_path,
            "tp_size": self.tp_size,
            "modes": list(self.modes),
            "input_lens": list(self.input_lens),
            "chunk_lens": list(self.chunk_lens),
            "batch_sizes": list(self.batch_sizes),
            "page_sizes": list(self.page_sizes),
            "layouts": list(self.layouts),
            "seed": self.seed,
            "num_steps": len(self.shapes),
        }

    def describe(self) -> str:
        def row(label, value):
            return f"  {label:<20}{value}"

        lines = []
        lines.append("Step matrix")
        lines.append(row("model path", self.model_path))
        lines.append(row("TP", self.tp_size_display()))
        lines.append(row("modes", ", ".join(self.modes)))
        lines.append(row("history lengths", ", ".join(str(v) for v in self.input_lens)))
        lines.append(row("query lengths", ", ".join(str(v) for v in self.chunk_lens)))
        lines.append(row("batch sizes", ", ".join(str(v) for v in self.batch_sizes)))
        lines.append(row("page sizes", ", ".join(str(v) for v in self.page_sizes)))
        lines.append(row("layouts", ", ".join(self.layouts)))
        lines.append(row("seed", self.seed))
        lines.append("")
        lines.append(f"  steps ({len(self.shapes)})")
        for shape in self.shapes:
            lines.append(
                f"    {shape.label:<48} history={list(shape.prefix_lens)} "
                f"queries={list(shape.new_lens)}"
            )
        return "\n".join(lines)


def build_plan_from_args(args) -> BenchmarkPlan:
    """Expand the CLI arguments into the step matrix."""
    if args.quick and args.full:
        raise ValueError("--quick and --full are mutually exclusive")
    if not args.quick and not args.full:
        raise ValueError("exactly one of --quick or --full is required")

    if args.quick:
        modes = ("prefill", "extend", "decode")
        input_lens = (128, 512)
        chunk_lens = (128,)
        batch_sizes = (1,)
        page_sizes = (1, 64)
        layouts = ("contiguous", "random")
    else:
        modes = BASE_MODES
        input_lens = BASE_INPUT_LENS
        chunk_lens = BASE_CHUNK_LENS
        batch_sizes = BASE_BATCH_SIZES
        page_sizes = BASE_PAGE_SIZES
        layouts = ("contiguous", "random")

    # Test for None explicitly: a truthiness test would silently drop a supplied
    # 0 and fall back to the default, so the user's argument would do nothing
    if args.modes is not None:
        modes = tuple(args.modes)
    if args.input_lens is not None:
        input_lens = tuple(args.input_lens)
    if args.chunk_lens is not None:
        chunk_lens = tuple(args.chunk_lens)
    if args.batch_sizes is not None:
        batch_sizes = tuple(args.batch_sizes)
    if args.page_sizes is not None:
        page_sizes = tuple(args.page_sizes)
    if args.layouts is not None:
        layouts = tuple(args.layouts)

    # Validate every numeric argument up front, so a 0 or a negative value cannot
    # turn into a division by zero, an empty sample or a hard-to-place exception.
    # An empty --tp-size is not a number but the request to use the visible GPU
    # count, which only a run on hardware can resolve, so it is checked later.
    if args.tp_size is not None:
        check_positive("--tp-size", args.tp_size)
    for value in input_lens:
        check_positive("each --input-lens entry", value)
    for value in chunk_lens:
        check_positive("each --chunk-lens entry", value)
    for value in batch_sizes:
        check_positive("each --batch-sizes entry", value)
    for value in page_sizes:
        check_positive("each --page-sizes entry", value)
    for name, values in (
        ("--modes", modes),
        ("--input-lens", input_lens),
        ("--chunk-lens", chunk_lens),
        ("--batch-sizes", batch_sizes),
        ("--page-sizes", page_sizes),
        ("--layouts", layouts),
    ):
        if not values:
            raise ValueError(f"{name} must not be empty")

    return BenchmarkPlan(
        quick=bool(args.quick),
        model_path=args.model,
        tp_size=args.tp_size,
        modes=modes,
        input_lens=input_lens,
        chunk_lens=chunk_lens,
        batch_sizes=batch_sizes,
        page_sizes=page_sizes,
        layouts=layouts,
        seed=args.seed,
    )
