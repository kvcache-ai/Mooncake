#!/usr/bin/env python3
"""Use the baseline's tracked CMake recipes in disposable benchmark checkouts."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def build_files(root):
    tracked = (
        subprocess.check_output(["git", "-C", str(root), "ls-files", "-z"], timeout=30)
        .decode()
        .split("\0")
    )
    return {
        name
        for name in tracked
        if Path(name).name == "CMakeLists.txt" or name.endswith(".cmake")
    }


def checked_path(root, name):
    path = root / name
    if not path.resolve().is_relative_to(root) or any(
        item.is_symlink() for item in (path, *path.parents) if item != root
    ):
        raise ValueError(
            f"build configuration uses a symlink or escapes its checkout: {name}"
        )
    return path


def freeze(baseline, candidate):
    baseline, candidate = Path(baseline).resolve(), Path(candidate).resolve()
    if baseline == candidate:
        raise ValueError("baseline and candidate must be separate checkouts")
    names = build_files(baseline)
    if "CMakeLists.txt" not in names:
        raise ValueError("baseline has no tracked CMake project")
    sources = {
        name: checked_path(baseline, name).read_bytes() for name in sorted(names)
    }
    destinations = {
        name: checked_path(candidate, name) for name in names | build_files(candidate)
    }
    # Remove newly added recipes too: a frozen glob/include could otherwise load them.
    for name, path in destinations.items():
        if name not in sources:
            path.unlink(missing_ok=True)
    for name, content in sources.items():
        path = destinations[name]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    files = {
        name: hashlib.sha256(content).hexdigest() for name, content in sources.items()
    }
    return {
        "files": files,
        "sha256": hashlib.sha256(
            json.dumps(files, sort_keys=True).encode()
        ).hexdigest(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args()
    manifest = freeze(args.baseline, args.candidate)
    Path(args.manifest).write_text(json.dumps(manifest, indent=2) + "\n")
