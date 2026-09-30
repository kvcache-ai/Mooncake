from __future__ import annotations

import sys
from pathlib import Path

# `mooncake_store_rs` owns its own top-level name, so putting `python/` on the
# path is all the source tree needs -- no grafting onto an installed package.
ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
