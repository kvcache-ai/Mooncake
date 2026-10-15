from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = ROOT.parent / "python"
SOURCE_PACKAGE = SOURCE_ROOT / "mooncake"

try:
    import mooncake
except ModuleNotFoundError:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    if str(SOURCE_ROOT) not in sys.path:
        sys.path.append(str(SOURCE_ROOT))
else:
    mooncake_path = getattr(mooncake, "__path__", None)
    if mooncake_path is not None:
        for package_dir, module_name in (
            (SOURCE_PACKAGE, "mooncake.structured_object_store"),
            (ROOT / "mooncake", "mooncake._partial_read"),
        ):
            if (
                importlib.util.find_spec(module_name) is None
                and str(package_dir) not in mooncake_path
            ):
                mooncake_path.append(str(package_dir))
