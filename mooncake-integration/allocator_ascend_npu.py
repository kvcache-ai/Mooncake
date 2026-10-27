import ctypes
import logging
import threading
from typing import Dict, Final

from torch import device as torch_device
from torch_npu.npu.memory import NPUPluggableAllocator

from .fabric_allocator_utils import get_mooncake_so_path, probe_allocator_backend

logger = logging.getLogger(__name__)


class UBShmemAllocator:
    _instances: Dict[torch_device, NPUPluggableAllocator] = {}
    _lock: Final = threading.Lock()
    _supports_fabric: bool = False
    _probe_done: bool = False

    @classmethod
    def _get_so_path(cls) -> str:
        return get_mooncake_so_path(
            "ubshmem_fabric_allocator.so",
            "UBShmemAllocator require mooncake-transfer-engine with USE_UBSHMEM enabled.",
        )

    @classmethod
    def _probe_fabric_memory_support(cls, so_path: str) -> bool:
        supported = bool(
            probe_allocator_backend(
                so_path,
                "mc_allocator_probe",
                ctypes.c_int,
                0,
            )
        )
        if supported:
            logger.info("Supports Fabric Memory with aclMallocPhysical")
        else:
            logger.info("Fabric memory not supported")
        return supported

    @classmethod
    def detect_mem_backend(cls) -> bool:
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
                    cls._supports_fabric = False

                cls._probe_done = True
        return cls._supports_fabric

    @classmethod
    def get_allocator(cls, device: torch_device) -> NPUPluggableAllocator:
        with cls._lock:
            if device not in cls._instances:
                so_path = cls._get_so_path()
                cls._instances[device] = NPUPluggableAllocator(
                    so_path, "mc_allocator_malloc", "mc_allocator_free"
                )
            return cls._instances[device]
