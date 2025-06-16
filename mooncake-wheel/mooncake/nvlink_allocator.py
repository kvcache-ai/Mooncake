import os
import torch
from torch.cuda.memory import CUDAPluggableAllocator

class NVLinkAllocator:
    _instances = {}
    
    @classmethod 
    def detect_hook(cls) -> bool:
        module_path = os.path.dirname(os.path.abspath(__file__))
        hook_so_path = os.path.join(module_path, 'hook.so')
        return os.path.exists(hook_so_path)
    
    @classmethod
    def get_allocator(cls, device):
        if device not in cls._instances:
            module_path = os.path.dirname(os.path.abspath(__file__))
            hook_so_path = os.path.join(module_path, 'hook.so')
            if os.path.exists(hook_so_path):
                allocator = CUDAPluggableAllocator(
                    hook_so_path,
                    "mc_nvlink_malloc",
                    "mc_nvlink_free"
                )
                cls._instances[device] = allocator
        return cls._instances[device]
