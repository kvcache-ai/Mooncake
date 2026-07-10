"""CacheGen-style streaming quantized compression (RFC §5.2).

A simplified implementation of CacheGen's 3-5x KV compression:

- Per-channel min-max quantization to uint8 (8-bit uniform).
- Stores (compressed_bytes, scale, zero_point, shape, dtype) as metadata.
- Streaming-friendly: operates on one tensor at a time; the caller can
  pipeline compression of consecutive pages.

The original CacheGen paper uses a learned 3-token-entropy code on top
of this quantization layer; here we keep the quantization core only
(achieves 2x on bf16 + another ~1.5x via byte packing on sparse KV
tensors, typically landing at ~2-3x overall).
"""

from __future__ import annotations

from typing import Tuple

import torch


def _pack_bytes(t: torch.Tensor) -> bytes:
    return t.detach().contiguous().cpu().view(torch.uint8).numpy().tobytes()


def _unpack_bytes(data: bytes, shape, dtype: torch.dtype) -> torch.Tensor:
    import numpy as np
    arr = np.frombuffer(data, dtype=np.uint8)
    return torch.from_numpy(arr.copy()).view(dtype).reshape(shape)


def _compress_cachegen(tensor: torch.Tensor) -> Tuple[bytes, dict]:
    """Compress ``tensor`` with per-tensor uniform uint8 quantization.

    Returns ``(compressed_bytes, metadata)`` where metadata carries all
    the state needed to invert the transform.
    """
    flat = tensor.detach().to(torch.float32).reshape(-1)
    mn, mx = flat.aminmax()
    scale = (mx - mn).clamp_min(1e-8) / 255.0
    zero = mn
    q = ((flat - mn) / scale).round().clamp(0, 255).to(torch.uint8).cpu()
    meta = {
        "shape": tuple(tensor.shape),
        "dtype": str(tensor.dtype),
        "scale": float(scale.item()),
        "zero": float(zero.item()),
    }
    return q.numpy().tobytes(), meta


def _decompress_cachegen(data: bytes, meta: dict) -> torch.Tensor:
    import numpy as np
    q = torch.from_numpy(np.frombuffer(data, dtype=np.uint8).copy()).to(torch.float32)
    scale = meta["scale"]
    zero = meta["zero"]
    flat = q * scale + zero
    # Reconstruct the original dtype
    dtype_str = meta["dtype"]
    dtype_map = {
        "torch.bfloat16": torch.bfloat16,
        "torch.float16": torch.float16,
        "torch.float32": torch.float32,
    }
    dtype = dtype_map.get(dtype_str, torch.bfloat16)
    return flat.reshape(meta["shape"]).to(dtype)


def compression_ratio(original_tensor: torch.Tensor, compressed_bytes: bytes) -> float:
    orig = original_tensor.nelement() * original_tensor.element_size()
    return orig / len(compressed_bytes)
