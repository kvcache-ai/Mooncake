from mooncake_vllm_adaptor_raw import mooncake_vllm_adaptor
from mooncake_vllm_adaptor_raw import MooncakeDistributedStore as MooncakeDistributedStoreBase

import torch
import numpy as np
from typing import List, Optional, Tuple
import struct
import ast

class MooncakeDistributedStore(MooncakeDistributedStoreBase):

    @staticmethod
    def __parse_header(data: bytes, header_len: int) -> dict:
        start = 4
        end = start + header_len
        if end > len(data):
            raise ValueError("header_len exceeds data length")
        header_bytes = data[start:end]
        try:
            header_str = header_bytes.decode('utf-8')
        except UnicodeDecodeError as e:
            raise ValueError("Invalid UTF-8 in header") from e
        try:
            header = ast.literal_eval(header_str)
        except (SyntaxError, ValueError) as e:
            raise ValueError("Malformed header data") from e
        if not isinstance(header, dict):
            raise ValueError("Header is not a dictionary")
        return header

    @staticmethod
    def __tensor_to_bytes(tensor: torch.Tensor) -> bytes:
        """Tensor to Bytes""" 
        # Get metadata
        dtype = str(tensor.dtype).split('.')[-1]
        shape = tensor.numpy().shape
        data = tensor.numpy().tobytes()
        
        # Concat metadata and data
        header = f'{{"dtype":"{dtype}","shape":{list(shape)}}}'.encode('utf-8')
        header_len = struct.pack('>I', len(header))
        return header_len + header + data

    @staticmethod
    def __bytes_to_tensor(data: bytes) -> torch.Tensor:
        """Bytes to Tensor"""
        # Get metadata and data
        header_len = struct.unpack('>I', data[:4])[0]
        header = MooncakeDistributedStore.__parse_header(data, header_len)
        buffer = data[4+header_len:]
        
        # construct NumPy
        np_array = np.frombuffer(buffer, dtype=header['dtype'])
        np_array = np_array.reshape(header['shape'])
        
        # Transfer PyTorch Tensor
        return torch.from_numpy(np_array.copy())

    """Add simple put API"""
    def put_tensor(self, key: str, value: torch.tensor):
        value_bytes = MooncakeDistributedStore.__tensor_to_bytes(value)
        super().put(key, value_bytes)
    
    """Add simple get API"""
    def get_tensor(self, key: str):
        value_bytes = super().get(key)
        return MooncakeDistributedStore.__bytes_to_tensor(value_bytes)