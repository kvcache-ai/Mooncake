import ctypes
import logging
import os
import threading
from importlib import resources
from typing import Dict, Final
from enum import IntEnum

from torch import device as torch_device
from torch.cuda.memory import CUDAPluggableAllocator

from .fabric_allocator_utils import get_mooncake_so_path, probe_allocator_backend

logger = logging.getLogger(__name__)


class MemoryBackend(IntEnum):
    USE_CUDAMALLOC = 0
    USE_CUMEMCREATE = 1
    UNKNOWN = -1
    UNSUPPORTED = -2


class NVLinkAllocator:
    _instances: Dict[torch_device, CUDAPluggableAllocator] = {}
    _lock: Final = threading.Lock()
    _supports_fabric: int = MemoryBackend.UNKNOWN
    _probe_done: bool = False

    @classmethod
    def _get_so_path(cls) -> str:
        return get_mooncake_so_path(
            "nvlink_allocator.so",
            "SGLANG_MOONCAKE_CUSTOM_MEM_POOL require mooncake-transfer-engine >= 0.3.3.post2.",
        )

    @classmethod
    def _probe_fabric_memory_support(cls, so_path: str) -> MemoryBackend:
        supported_type = probe_allocator_backend(
            so_path,
            "mc_allocator_probe",
            ctypes.c_int,
            int(MemoryBackend.UNSUPPORTED),
        )
        try:
            backend = MemoryBackend(supported_type)
        except ValueError:
            logger.info("Unknown Backend error")
            return MemoryBackend.UNKNOWN

        if backend == MemoryBackend.USE_CUDAMALLOC:
            logger.info("Use CudaMalloc fallback")
        elif backend == MemoryBackend.USE_CUMEMCREATE:
            logger.info("Supports Fabric Memory")
        elif backend == MemoryBackend.UNSUPPORTED:
            logger.info("Allocator backend probing is unsupported")
        else:
            logger.info("Unknown Backend error")
        return backend

    @classmethod
    def detect_mem_backend(cls) -> MemoryBackend:
        """Public API: check if fabric memory is supported."""
        if not cls._probe_done:
            with cls._lock:
                if cls._probe_done:
                    return cls._supports_fabric
                try:
                    cls._supports_fabric = cls._probe_fabric_memory_support(
                        cls._get_so_path()
                    )
                except Exception as e:
                    logger.error(
                        f"Critical error during fabric memory probe setup: {e}"
                    )
                    cls._supports_fabric = MemoryBackend.UNSUPPORTED

                cls._probe_done = True
        return cls._supports_fabric

    @classmethod
    def get_allocator(cls, device: torch_device) -> CUDAPluggableAllocator:
        with cls._lock:
            if device not in cls._instances:
                so_path = cls._get_so_path()
                cls._instances[device] = CUDAPluggableAllocator(
                    so_path, "mc_allocator_malloc", "mc_allocator_free"
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
                    so_path,
                    "u2mm_alloc_wrapper_with_stream",
                    "u2mm_free_wrapper_with_stream",
                )
            return cls._instances[device]
