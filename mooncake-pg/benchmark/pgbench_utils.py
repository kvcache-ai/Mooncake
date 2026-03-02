from __future__ import annotations

import datetime
import re
from typing import List, Optional, Tuple

import torch
import torch.distributed as dist


NCCL_DTYPE_ORDER = [
    "int8",
    "uint8",
    "int32",
    "uint32",
    "int64",
    "uint64",
    "half",
    "float",
    "double",
    "bfloat16",
    "f8e4m3",
    "f8e5m2",
]

_SIZE_RE = re.compile(r"^(\d+)([KkMmGgTt])?[Bb]?$")


def parse_size(value: object) -> int:
    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        raise ValueError(f"invalid size value: {value!r}")
    text = value.strip()
    if text.isdigit():
        return int(text)
    match = _SIZE_RE.match(text)
    if not match:
        raise ValueError(f"invalid size format: {value}")
    number = int(match.group(1))
    suffix = match.group(2)
    if suffix is None:
        return number
    scale = {
        "k": 1024,
        "m": 1024**2,
        "g": 1024**3,
        "t": 1024**4,
    }[suffix.lower()]
    return number * scale


def _get_attr(obj, name: str) -> Optional[object]:
    return getattr(obj, name, None)


def _resolve_fp8_dtype(name: str) -> Optional[torch.dtype]:
    if name == "f8e4m3":
        for candidate in ("float8_e4m3fn", "float8_e4m3fnuz"):
            dtype = _get_attr(torch, candidate)
            if dtype is not None:
                return dtype
    if name == "f8e5m2":
        for candidate in ("float8_e5m2", "float8_e5m2fnuz"):
            dtype = _get_attr(torch, candidate)
            if dtype is not None:
                return dtype
    return None


def _dtype_map() -> dict[str, Optional[torch.dtype]]:
    return {
        "int8": torch.int8,
        "uint8": torch.uint8,
        "int32": torch.int32,
        "uint32": _get_attr(torch, "uint32"),
        "int64": torch.int64,
        "uint64": _get_attr(torch, "uint64"),
        "half": torch.float16,
        "float": torch.float32,
        "double": torch.float64,
        "bfloat16": _get_attr(torch, "bfloat16"),
        "f8e4m3": _resolve_fp8_dtype("f8e4m3"),
        "f8e5m2": _resolve_fp8_dtype("f8e5m2"),
    }


def _can_allocate(dtype: torch.dtype, device: torch.device) -> bool:
    try:
        torch.empty(1, device=device, dtype=dtype)
        return True
    except Exception:
        return False


def list_supported_dtypes(device: torch.device) -> List[Tuple[str, torch.dtype]]:
    mapping = _dtype_map()
    supported: List[Tuple[str, torch.dtype]] = []
    for name in NCCL_DTYPE_ORDER:
        dtype = mapping.get(name)
        if dtype is None:
            continue
        if _can_allocate(dtype, device):
            supported.append((name, dtype))
    return supported


def resolve_dtype(name: str, device: torch.device) -> torch.dtype:
    mapping = _dtype_map()
    dtype = mapping.get(name)
    if dtype is None:
        raise ValueError(f"dtype '{name}' is not supported by this runtime")
    if not _can_allocate(dtype, device):
        raise ValueError(f"dtype '{name}' is not supported on device {device}")
    return dtype


def resolve_reduce_op(name: str) -> Tuple[dist.ReduceOp, Optional[float]]:
    if name == "sum":
        return dist.ReduceOp.SUM, None
    if name == "prod":
        return dist.ReduceOp.PRODUCT, None
    if name == "max":
        return dist.ReduceOp.MAX, None
    if name == "min":
        return dist.ReduceOp.MIN, None
    if name == "avg":
        # Use SUM and divide by world_size after collective.
        return dist.ReduceOp.SUM, 0.0
    raise ValueError(f"unsupported reduce op: {name}")


def _align_count_by_16(count: int, elt_size: int) -> int:
    if elt_size <= 0:
        return 0
    align = 16 // elt_size
    if align <= 0:
        return count
    return count & -align


def compute_counts(
    collective: str, size_bytes: int, elt_size: int, nranks: int
) -> Tuple[int, int, int, int, int]:
    size_elems = size_bytes // elt_size if elt_size else 0
    if collective in ("all_reduce", "broadcast", "sendrecv"):
        sendcount = size_elems
        recvcount = size_elems
        paramcount = sendcount
        return sendcount, recvcount, paramcount, 0, 0
    if collective == "all_gather":
        base = _align_count_by_16(size_elems // nranks if nranks else 0, elt_size)
        sendcount = base
        recvcount = base * nranks
        paramcount = base
        return sendcount, recvcount, paramcount, base, 0
    if collective == "reduce_scatter":
        base = _align_count_by_16(size_elems // nranks if nranks else 0, elt_size)
        sendcount = base * nranks
        recvcount = base
        paramcount = base
        return sendcount, recvcount, paramcount, 0, base
    if collective == "alltoall":
        base = _align_count_by_16(size_elems // nranks if nranks else 0, elt_size)
        sendcount = base * nranks
        recvcount = sendcount
        paramcount = base
        return sendcount, recvcount, paramcount, 0, 0
    raise ValueError(f"unsupported collective: {collective}")


def busbw_factor(collective: str, nranks: int) -> float:
    if collective == "all_reduce":
        return (2.0 * (nranks - 1)) / nranks if nranks else 0.0
    if collective in ("reduce_scatter", "all_gather", "alltoall"):
        return (nranks - 1) / nranks if nranks else 0.0
    if collective in ("broadcast", "sendrecv"):
        return 1.0
    return 1.0


def format_float(value: float, width: int) -> str:
    power = 0
    val = 1.0
    while value >= val:
        power += 1
        val *= 10.0
    if power < width - 2:
        return f"{value:>{width}.2f}"
    if power < width - 1:
        return f"{value:>{width}.1f}"
    if power < width + 1:
        return f"{value:>{width}.0f}"
    if width >= 7:
        return f"{value:>{width}.1e}"
    if width >= 8:
        return f"{value:>{width}.2e}"
    return f"{value:>{width}.0e}"


def humanize_number(value: int) -> str:
    suffixes = ["", "K", "M", "G", "T", "P"]
    val = float(value)
    idx = 0
    while idx < len(suffixes) - 1 and val >= 1024.0:
        val /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(val)}"
    if val < 10:
        return f"{val:.1f}{suffixes[idx]}"
    return f"{val:.0f}{suffixes[idx]}"


def format_header(report_cputime: bool, report_timestamps: bool) -> str:
    ts_lbl = "timestamp" if report_timestamps else ""
    ts_pad = 19 if report_timestamps else 0
    ts_fmt = "%Y-%m-%d %H:%M:%S" if report_timestamps else ""
    time_label = "cputime" if report_cputime else "time"
    lines = []
    lines.append("#")
    lines.append(
        f"# {'':10s}  {'':12s}  {'':8s}  {'':6s}  {'':6s}           out-of-place                       in-place          "
    )
    lines.append(
        f"# {'size':10s}  {'count':12s}  {'type':8s}  {'redop':6s}  {'root':6s}  {time_label:>7s}  {'algbw':6s}  {'busbw':6s}  {'#wrong':6s}  {time_label:>7s}  {'algbw':6s}  {'busbw':6s}  {'#wrong':6s} {ts_lbl:>{ts_pad}s}"
    )
    lines.append(
        f"# {'(B)':10s}  {'(elements)':12s}  {'':8s}  {'':6s}  {'':6s}  {'(us)':>7s}  {'(GB/s)':6s}  {'(GB/s)':6s}  {'':6s}  {'(us)':>7s}  {'(GB/s)':6s}  {'(GB/s)':6s}  {'':6s} {ts_fmt:>{ts_pad}s}"
    )
    return "\n".join(lines)


def format_result_line(
    size_bytes: int,
    count: int,
    dtype_name: str,
    op_name: str,
    root: int,
    oop: Optional[Tuple[float, float, float, int]],
    inp: Optional[Tuple[float, float, float, int]],
    report_timestamps: bool,
) -> str:
    size_str = humanize_number(size_bytes)
    count_str = humanize_number(count)
    line = (
        f"{size_str:>12s}  {count_str:>12s}  {dtype_name:8s}  {op_name:6s}  {root:6d}"
    )

    def _body(metrics: Optional[Tuple[float, float, float, int]]) -> str:
        if metrics is None:
            return f"  {'N/A':>7s}  {'N/A':>6s}  {'N/A':>6s}  {'N/A':>6s}"
        time_us, algbw, busbw, wrong = metrics
        time_str = format_float(time_us, 7)
        algbw_str = format_float(algbw, 6)
        busbw_str = format_float(busbw, 6)
        wrong_str = f"{wrong:6g}" if wrong >= 0 else f"{'N/A':>6s}"
        return f"  {time_str}  {algbw_str}  {busbw_str}  {wrong_str}"

    line += _body(oop)
    line += _body(inp)

    if report_timestamps:
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line += f" {ts:>19s}"

    return line
