# Opt-in FP8 KV storage

This module compresses complete CPU KV chunks before the existing Store `put`
and restores them after `get`. It has three responsibilities:

* `KVCodec` defines synchronous buffer-to-buffer encode/decode operations.
  `ScaledFp8Codec` implements FP16/BF16 to scaled E4M3FN on the CPU.
* The encoded record stores its descriptor, scales and payload together.
* `CodecStoreClient` connects the codec to the public Python Store API.

The master and Transfer Engine still handle opaque bytes. The implementation
does not depend on `mooncake-reshard` or change the attention cache dtype.
It is a CPU reference path, not a GPU offload or throughput optimization.

## Usage

Build Mooncake with `WITH_STORE=ON` using the usual CMake/Python installation
workflow. The build installs `mooncake._kv_codec` and `mooncake.codec_store`.
The tensor wrapper additionally requires PyTorch and NumPy at runtime.

```python
import torch
from mooncake.codec_store import CodecStoreClient

# store is an already initialized MooncakeDistributedStore.
cache = CodecStoreClient(store, group_size=128)
chunk = torch.randn(2, 16, 128, dtype=torch.bfloat16)
# Use your existing canonical cache key, including model, tokens, layout,
# original dtype and shard identity. The wrapper does not derive that identity.
key = "model-revision/layout-bf16/tp-rank/token-hash"
status = cache.put(key, chunk)
if status != 0:
    raise RuntimeError(f"Store put failed: {status}")

restored = torch.empty_like(chunk)
cache.get_into(key, restored)  # Lossy reconstruction in the original dtype.
store.remove(cache.storage_key(key))
```

Inputs must be nonempty, contiguous, strided CPU FP16/BF16 tensors with rank
1 through 8, finite values, and no autograd or lazy conjugate/negative view bits.
Groups are consecutive flattened elements, including a short final group;
there is no implicit per-head/per-token interpretation. The caller chooses a
shape and group size appropriate for its KV layout. Packed/FP8 inputs are
rejected rather than quantized again. Store a complete K/V chunk in one tensor,
or retain the application's existing protocol for multiple component keys.

The representation key is `__mc_codec__/fp8-e4m3fn-v1-g<group_size>/<key>`.
Reserve this prefix for the wrapper. Raw keys and different codec configurations
remain separate. To disable compression, use the original Store API. There is
no automatic fallback to a different representation. Use `storage_key()` for
existing Store remove/existence operations. `put` returns the underlying status
and preserves insert-only/existing-key and replica configuration semantics.

`get_into` reads the complete record once. It checks the version, checksum,
shape, dtype, configuration and side data before writing the destination.
Corruption raises `InvalidRecord`; incompatible descriptors raise `ValueError`.
An empty Store read raises `StoreReadError` because the bytes API cannot
distinguish a missing object from a failed read. These failures leave the
destination unchanged. Calls are synchronous: do not mutate participating
tensors concurrently. Python holds their exported buffers while C++ runs with
the GIL released. Range reads, batching, GPU streams and resharding are not
part of this initial interface.

## Wire format, version 1

All multibyte fields in the record are little-endian. Tensor buffers presented
to C++ use native FP16/BF16 byte order. There are no pointers or Python objects
in the record.

| Offset | Field |
| --- | --- |
| 0 | 8-byte magic `MCKVFP8\0` |
| 8 | uint16 version, currently 1 |
| 10 | uint8 source dtype: 1 = FP16, 2 = BF16 |
| 11 | uint8 rank |
| 12 | uint32 group size |
| 16 | uint64 element count |
| 24 | uint32 CRC32/IEEE of the entire record, with this field zeroed |
| 28 | uint32 reserved, must be zero |
| 32 | `rank` uint64 shape dimensions |
| after shape | `ceil(numel / group_size)` FP32 scales |
| after scales | `numel` E4M3FN bytes |

For each group, `scale = max(abs(x)) / 448` in FP32; an all-zero group uses 1.
Normalize in FP32, clamp to [-448, 448], and convert to E4M3FN with round to
nearest, ties to even. Decode multiplies the FP8 value by its persisted scale,
clamps to the source dtype's finite range, rounds to FP32, then converts to
the source dtype with round to nearest, ties to even. Signed zero is preserved.
Non-finite input, non-positive/non-finite scales and NaN payloads are rejected.
CRC detects accidental corruption; it does not authenticate data.

For `N` elements and rank `R`, encoded size is
`32 + 8*R + 4*ceil(N/group_size) + N` bytes, versus `2*N` raw bytes.
Small chunks or small groups can grow; no adaptive encoding is performed.
Quantization accuracy and end-to-end speed depend on the model/workload and
must be evaluated before using this lossy format in serving.

## Tests

The production C++ codec and Python binding can be built independently of
Store, CUDA and RDMA. From the repository root, with CMake, PyTorch, NumPy and
pytest available:

```sh
cmake -S mooncake-store/tests/codec -B /tmp/mooncake-codec-build \
  -DPython3_EXECUTABLE="$(command -v python3)" -DCMAKE_BUILD_TYPE=Debug
cmake --build /tmp/mooncake-codec-build -j 4
ctest --test-dir /tmp/mooncake-codec-build --output-on-failure
```

For C++ buffer checks with sanitizers and no Python dependencies:

```sh
cmake -S mooncake-store/tests/codec -B /tmp/mooncake-codec-asan \
  -DCODEC_TEST_PYTHON=OFF -DCMAKE_BUILD_TYPE=Debug \
  '-DCMAKE_CXX_FLAGS=-fsanitize=address,undefined -fno-omit-frame-pointer'
cmake --build /tmp/mooncake-codec-asan -j 4
ctest --test-dir /tmp/mooncake-codec-asan --output-on-failure
```

Tests compare scales, FP8 bytes and reconstructed values against PyTorch,
exercise all finite FP16/BF16 bit patterns, and cover malformed records,
descriptor mismatches, Store errors and representation isolation. The default
suite uses an in-memory implementation of the public bytes API. For the
optional real TCP Store test on Linux, install the full Mooncake package,
start an isolated master, and run:

```sh
MC_CODEC_INTEGRATION=1 MASTER_SERVER=127.0.0.1:50051 \
  python3 -m pytest python/tests/store/test_codec_store.py -q
```

The integration test creates a unique key and removes it on completion. It
does not cover RDMA, GPU execution, inference quality or serving performance.
