import ctypes
import logging
import os
import threading
from importlib import resources
from typing import Dict, Final, Optional
from enum import IntEnum

from torch import device as torch_device
from torch.cuda.memory import CUDAPluggableAllocator

logger = logging.getLogger(__name__)


class MemoryBackend(IntEnum):
    USE_CUDAMALLOC  = 0
    USE_CUMEMCREATE = 1
    UNKNOWN         = -1
    UNSUPPORTED     = -2


class NVLinkAllocator:
    _instances: Dict[torch_device, CUDAPluggableAllocator] = {}
    _lock: Final = threading.Lock()
    _supports_fabric: int = MemoryBackend.UNKNOWN
    _probe_done: bool = False

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
    def _probe_fabric_memory_support(cls, so_path: str) -> MemoryBackend:
        """
        Probe whether the system supports fabric memory by calling a C++ function
        that attempts cuMemCreate with CU_MEM_HANDLE_TYPE_FABRIC.
        We assume the shared library exports a symbol like:
            extern "C" MemoryBackendType mc_probe_fabric_support(int device_id);
        """
        try:

            lib = ctypes.CDLL(so_path)

            # Try to get the probe function
            probe_func = lib.mc_probe_fabric_support
            probe_func.argtypes = [ctypes.c_int]
            probe_func.restype = ctypes.c_int

            # Use device 0 for probing
            dev_id = 0
            supported_type = probe_func(dev_id)
            if supported_type == MemoryBackend.USE_CUDAMALLOC:
                logger.info(f"Use CudaMalloc fallback")
            elif supported_type == MemoryBackend.USE_CUMEMCREATE:
                logger.info(f"Supports Fabric Memory")
            else:
                logger.info("Unknown Backend error")
            return supported_type

        except AttributeError:
            logger.warning(
                "Symbol 'mc_probe_fabric_support' not found in nvlink_allocator.so. "
                "Assuming fabric memory is NOT supported (you may need to update the library)."
            )
            return MemoryBackend.UNSUPPORTED
        except Exception as e:
            logger.warning(f"Failed to probe fabric memory support: {e}")
            return MemoryBackend.UNSUPPORTED

    @classmethod
    def detect_mem_backend(cls) -> MemoryBackend:
        """Public API: check if fabric memory is supported."""
        if not cls._probe_done:
            with cls._lock:
                if cls._probe_done:
                    return
                so_path = None
                try:
                    so_path = cls._get_so_path()
                    # First try dedicated probe function
                    cls._supports_fabric = cls._probe_fabric_memory_support(so_path)
                except Exception as e:
                    logger.error(f"Critical error during fabric memory probe setup: {e}")
                    cls._supports_fabric = MemoryBackend.UNSUPPORTED

                cls._probe_done = True
        return cls._supports_fabric

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
                    so_path,
                    "u2mm_alloc_wrapper_with_stream",
                    "u2mm_free_wrapper_with_stream",
                )
            return cls._instances[device]
