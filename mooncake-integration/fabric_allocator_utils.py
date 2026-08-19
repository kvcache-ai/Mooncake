import ctypes
import logging
import os
from importlib import resources
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

ProbeValue = TypeVar("ProbeValue")


def get_mooncake_so_path(so_name: str, import_error_message: str) -> str:
    try:
        with resources.path("mooncake", so_name) as so_path:
            if so_path.exists():
                return str(so_path)
    except (ImportError, FileNotFoundError, TypeError):
        pass

    try:
        import mooncake

        base_path = os.path.dirname(os.path.abspath(mooncake.__file__))
        so_path = os.path.join(base_path, so_name)
        if os.path.exists(so_path):
            return so_path
    except (ImportError, FileNotFoundError, TypeError):
        pass

    raise ImportError(import_error_message)


def probe_allocator_backend(
    so_path: str,
    probe_symbol: str,
    restype: Any,
    unsupported_value: ProbeValue,
) -> ProbeValue:
    try:
        lib = ctypes.CDLL(so_path)
        probe_func = getattr(lib, probe_symbol)
        probe_func.argtypes = [ctypes.c_int]
        probe_func.restype = restype
        return probe_func(0)
    except AttributeError:
        logger.warning(
            "Symbol '%s' not found in %s. Assuming allocator probing is unsupported.",
            probe_symbol,
            os.path.basename(so_path),
        )
        return unsupported_value
    except Exception as exc:
        logger.warning(
            "Failed to probe allocator backend from %s: %s",
            os.path.basename(so_path),
            exc,
        )
        return unsupported_value
