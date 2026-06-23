from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE_PACKAGE = ROOT / "mooncake"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import mooncake
except ModuleNotFoundError:
    pass
else:
    if (
        importlib.util.find_spec("mooncake.structured_object_store") is None
        or importlib.util.find_spec("mooncake.buffer_pool") is None
    ):
        mooncake_path = getattr(mooncake, "__path__", None)
        if mooncake_path is not None and str(SOURCE_PACKAGE) not in mooncake_path:
            mooncake_path.append(str(SOURCE_PACKAGE))
