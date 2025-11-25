import os
import threading
from importlib import resources
from typing import Dict, Final, Optional

from torch import device as torch_device
from torch.cuda.memory import CUDAPluggableAllocator


class NVLinkAllocator:
    _instances: Dict[torch_device, CUDAPluggableAllocator] = {}
    _lock: Final = threading.Lock()

    @classmethod
    def _get_so_path(cls) -> str:
        """Dynamically locate nvlink_allocator.so in the mooncake package installation"""
        try:
            # Attempt to locate package resource
            with resources.path("mooncake", "nvlink_allocator.so") as so_path:
                if so_path.exists():
                    return str(so_path)
        except (ImportError, FileNotFoundError, TypeError):
            pass

        # Fallback strategy: check in package location via import metadata
        try:
            import mooncake

            base_path = os.path.dirname(os.path.abspath(mooncake.__file__))
            so_path = os.path.join(base_path, "nvlink_allocator.so")
            if os.path.exists(so_path):
                return so_path
        except (ImportError, FileNotFoundError, TypeError):
            raise ImportError(
                "SGLANG_MOONCAKE_CUSTOM_MEM_POOL require mooncake-transfer-engine >= 0.3.3.post2."
            )

    @classmethod
    def get_allocator(cls, device: torch_device) -> CUDAPluggableAllocator:
        with cls._lock:
            if device not in cls._instances:
                so_path = cls._get_so_path()
                cls._instances[device] = CUDAPluggableAllocator(
                    so_path, "mc_nvlink_malloc", "mc_nvlink_free"
                )
            return cls._instances[device]


class BarexAllocator:
    _instances: Dict[torch_device, CUDAPluggableAllocator] = {}
    _lock: Final = threading.Lock()

    @classmethod
    def _get_so_path(cls) -> str:
        """Dynamically locate libaccl_barex.so for barex memory allocation"""
        # Check common system paths for libaccl_barex.so
        possible_paths = [
            "/usr/lib/libaccl_barex.so",  # Ubuntu [deb]
            "/usr/lib64/libaccl_barex.so",  # AliOS [rpm]
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        # Try to locate in mooncake package installation
        try:
            # Attempt to locate package resource
            with resources.path("mooncake", "libaccl_barex.so") as so_path:
                if so_path.exists():
                    return str(so_path)
        except (ImportError, FileNotFoundError, TypeError):
            pass

        # Fallback strategy: check in package location via import metadata
        try:
            import mooncake

            base_path = os.path.dirname(os.path.abspath(mooncake.__file__))
            so_path = os.path.join(base_path, "libaccl_barex.so")
            if os.path.exists(so_path):
                return so_path
        except (ImportError, FileNotFoundError, TypeError):
            pass
        
        raise ImportError(
            "BarexAllocator requires libaccl_barex.so to be installed. "
            "Please install the barex library or ensure it's in the system path."
        )

    @classmethod
    def get_allocator(cls, device: torch_device) -> CUDAPluggableAllocator:
        with cls._lock:
            if device not in cls._instances:
                so_path = cls._get_so_path()
                cls._instances[device] = CUDAPluggableAllocator(
                    so_path, "u2mm_alloc_wrapper_with_stream", "u2mm_free_wrapper_with_stream"
                )
            return cls._instances[device]
