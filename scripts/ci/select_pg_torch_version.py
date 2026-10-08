# Copyright 2026 Mooncake contributors
# SPDX-License-Identifier: Apache-2.0

"""Select the latest PyTorch version with a PG extension in a wheel."""

import argparse
from collections.abc import Iterable
from pathlib import Path
import re
import zipfile


_PG_EXTENSION = re.compile(r"mooncake/pg_(\d+)_(\d+)_(\d+)(?:\.[^/]+)?\.so")


def select_torch_requirement(members: Iterable[str]) -> str:
    """Pin PG consumers to a bundled ABI; retain the non-PG install path."""
    versions = []
    for member in members:
        match = _PG_EXTENSION.fullmatch(member)
        if match is not None:
            versions.append(tuple(int(part) for part in match.groups()))
    if not versions:
        return "torch"
    return "torch==" + ".".join(str(part) for part in max(versions))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    args = parser.parse_args()
    with zipfile.ZipFile(args.wheel) as archive:
        print(select_torch_requirement(archive.namelist()))


if __name__ == "__main__":
    main()
