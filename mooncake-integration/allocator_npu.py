import ctypes
import logging
import os
import threading
from importlib import resources
from typing import Dict, Final, Optional
from enum import IntEnum
import torch_npu

from torch import device as torch_device
from torch_npu.npu.memory import NPUPluggableAllocator

logger = logging.getLogger(__name__)

class MemoryBackend(IntEnum):
    USE_ACLMALLOC  = 0
    USE_ACLMALLOCPHYSICAL = 1
    UNKNOWN         = -1
    UNSUPPORTED     = -2


class UBShmemAllocator:
    _instances: Dict[torch_device, NPUPluggableAllocator] = {}
    _lock: Final = threading.Lock()
    _supports_fabric: int = MemoryBackend.UNKNOWN
    _probe_done: bool = False

    @classmethod
    def _get_so_path(cls) -> str:
        """Dynamically locate ubshmem_fabric_allocator.so in the mooncake package installation"""
        try:
            # Attempt to locate package resource
            with resources.path("mooncake", "ubshmem_fabric_allocator.so") as so_path:
                if so_path.exists():
                    return str(so_path)
        except (ImportError, FileNotFoundError, TypeError):
            pass

        # Fallback strategy: check in package location via import metadata
        try:
            import mooncake

            base_path = os.path.dirname(os.path.abspath(mooncake.__file__))
            so_path = os.path.join(base_path, "ubshmem_fabric_allocator.so")
            if os.path.exists(so_path):
                return so_path
        except (ImportError, FileNotFoundError, TypeError):
            raise ImportError(
                "UBShmemAllocator require mooncake-transfer-engine with USE_UBSHMEM enabled."
            )

    @classmethod
    def _probe_fabric_memory_support(cls, so_path: str) -> MemoryBackend:
        """
        Probe whether the system supports fabric memory by calling a C++ function
        that attempts aclrtMallocPhysical with fabric memory support.
        The shared library exports a symbol like:
            extern "C" MemoryBackendType mc_probe_ub_fabric_support(int device_id);
        """
        try:

            lib = ctypes.CDLL(so_path)

            # Try to get the probe function
            probe_func = lib.mc_probe_ub_fabric_support
            probe_func.argtypes = [ctypes.c_int]
            probe_func.restype = ctypes.c_int

            # Use device 0 for probing
            dev_id = 0
            supported_type = probe_func(dev_id)
            if supported_type == MemoryBackend.USE_ACLMALLOC:
                logger.info(f"Use aclMalloc fallback")
            elif supported_type == MemoryBackend.USE_ACLMALLOCPHYSICAL:
                logger.info(f"Supports Fabric Memory with aclMallocPhysical")
            else:
                logger.info("Unknown Backend error")
            return supported_type

        except AttributeError:
            logger.warning(
                "Symbol 'mc_probe_ub_fabric_support' not found in ubshmem_fabric_allocator.so. "
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
                    return cls._supports_fabric
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
    def get_allocator(cls, device: torch_device) -> NPUPluggableAllocator:
        with cls._lock:
            if device not in cls._instances:
                so_path = cls._get_so_path()
                cls._instances[device] = NPUPluggableAllocator(
                    so_path, "mc_ub_fabric_malloc", "mc_ub_fabric_free"
                )
            return cls._instances[device]
