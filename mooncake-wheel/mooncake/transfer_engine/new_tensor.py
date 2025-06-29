import torch

dtype_map = {
        'bool': torch.bool,

        'float16': torch.float16,
        'float32': torch.float32,
        'float64': torch.float64,
        'half': torch.half,
        'float': torch.float,
        'double': torch.double,
        'bfloat16': torch.bfloat16,

        'int8': torch.int8,
        'int16': torch.int16,
        'int32': torch.int32,
        'int64': torch.int64,
        'uint8': torch.uint8,
        'uint16': torch.uint16,
        'uint32': torch.uint32,
        'uint64': torch.uint64,
        'long': torch.long,
        'int': torch.int,
        'short': torch.short,
        
        'complex64': torch.complex64,
        'complex128': torch.complex128,
        'complex': torch.complex
}

def shape_to_stride(shape: list)-> list:
    strides = []
    product = 1
    for dim in reversed(shape):
        strides.append(product)
        product *= dim
    return list(reversed(strides))

def get_child_tensor(tensor: torch.Tensor, shape: list, offset: int)-> torch.Tensor:
    stride = shape_to_stride(shape)
    return tensor.as_strided(shape, stride, storage_offset=offset)

def get_dtype(dtype: str)-> torch.dtype:
    return dtype_map[dtype]

def get_dtype_str(dtype: torch.dtype)-> str:
    for k, v in dtype_map.items():
        if v == dtype:
            return k

def create_tensor(shape: list, dtype: str, gpu_id=0):
    return torch.empty(shape, dtype=dtype_map[dtype], device=f"cuda:{gpu_id}")
