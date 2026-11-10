"""Plugin row-gather boundary; reuses only the engine's quantized ops and does
not replace hash/projection/collectives.
"""

import importlib
import types

import torch

_adapters = {}
_operation = None


def install_ops():
    global _operation
    if _operation is not None:
        return

    @torch.library.custom_op("mooncake::engram_plugin_lookup", mutates_args=("out",))
    def operation(adapter_id: int, ids: torch.Tensor, out: torch.Tensor) -> None:
        dispatch(_adapters[adapter_id], ids, out)

    @operation.register_fake
    def fake(adapter_id: int, ids: torch.Tensor, out: torch.Tensor) -> None:
        return None

    _operation = operation


def dispatch(adapter, ids, out):
    if not ids.is_cuda or not out.is_cuda or ids.device != out.device:
        raise ValueError(
            "Engram plugin supports only CUDA IDs/output on the same device"
        )
    if adapter.state.closed:
        raise RuntimeError("Engram plugin is closed")
    module = adapter.module
    weight = adapter.fetch("weight", ids)
    if adapter.kind == "sglang_deepseek":
        native = importlib.import_module("sglang.srt.layers.engram")
        scale = adapter.fetch("scale", ids)
        positions = torch.arange(ids.numel(), dtype=torch.int64, device=ids.device)
        native.engram_gather(
            weight.data_ptr(),
            scale.data_ptr(),
            positions,
            out.reshape(-1, module.dim),
            module.dim,
            native.FP8_BLOCK_SIZE,
            mxfp4=module.mxfp4,
        )
    elif adapter.kind == "sglang_qwen":
        values = weight.view(adapter.planes["weight"]["dtype"])
        out.copy_(values.to(torch.bfloat16))
    else:
        raise RuntimeError("Unknown Engram plugin adapter")


def _sglang_owned(module, ids):
    out = module.new_rows(ids)
    _operation(module._mooncake_engram_id, ids, out)
    return out


def _sglang_qwen(module, input_ids, out=None):
    shape = (*input_ids.shape, module.embedding_dim)
    if out is None:
        out = module.allocate_output(shape, input_ids.device)
    if (
        tuple(out.shape) != shape
        or out.dtype != torch.bfloat16
        or out.device != input_ids.device
    ):
        raise ValueError("SGLang PLE output layout mismatch")
    _operation(module._mooncake_engram_id, input_ids, out)
    return out


def bind(adapter):
    adapter.op_id = id(adapter)
    _adapters[adapter.op_id] = adapter
    module = adapter.module
    module._mooncake_engram_id = adapter.op_id
    if adapter.kind == "sglang_deepseek":
        module._owned_rows = types.MethodType(_sglang_owned, module)
    elif adapter.kind == "sglang_qwen":
        module.gather = types.MethodType(_sglang_qwen, module)
    else:
        raise RuntimeError("Unknown Engram plugin adapter")


def forget(adapters):
    for adapter in adapters:
        _adapters.pop(adapter.op_id, None)
