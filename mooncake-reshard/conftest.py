from pathlib import Path

import mooncake


RESHARD_PACKAGE = str(Path(__file__).parent / "python" / "mooncake")
if RESHARD_PACKAGE not in mooncake.__path__:
    mooncake.__path__.append(RESHARD_PACKAGE)
