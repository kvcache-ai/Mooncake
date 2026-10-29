#!/usr/bin/env python3
"""Select PR unit-test components; unknown inputs conservatively select all.

Store source ownership comes from its CMake source lists, not filename guesses
(e.g. master_client.cpp is client code). Public/shared interfaces and build
changes select all. Pushes, manual runs and nightly retain the full sweep.
"""

import argparse
import json
import os
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[2]


def cmake_list(text, name):
    match = re.search(r"set\(" + re.escape(name) + r"\s+([^)]*)\)", text)
    if match is None:
        raise ValueError(f"Missing CMake ownership list: {name}")
    return set(re.sub(r"#[^\n]*", "", match[1]).split())


def store_test_owners(root=ROOT):
    text = (root / "cmake/StoreTestComponents.cmake").read_text()
    return {
        name: f"store-{owner}"
        for owner in ("client", "master", "shared")
        for name in cmake_list(text, f"MOONCAKE_STORE_{owner.upper()}_TESTS")
    }


def store_source_owners(root=ROOT):
    text = (root / "mooncake-store/src/CMakeLists.txt").read_text()
    owners = {
        f"mooncake-store/src/{source}": f"store-{owner.lower()}"
        for owner in ("CLIENT", "MASTER", "SHARED")
        for source in cmake_list(text, f"MOONCAKE_STORE_{owner}_SOURCES")
    }
    owners["mooncake-store/src/master.cpp"] = "store-master"
    owners["mooncake-store/src/real_client_main.cpp"] = "store-client"
    return owners


def select_components(paths, *, full=False, root=ROOT):
    if full:
        return {"all"}
    sources = store_source_owners(root)
    tests = store_test_owners(root)
    # Associate nested test sources with their declared executable names too.
    for relative in ("mooncake-store/tests", "mooncake-store/tests/e2e"):
        text = (root / relative / "CMakeLists.txt").read_text()
        for name, source in re.findall(
            r"add_(?:store_test|ha_test|executable)\(\s*(\w+)\s+([\w/]+\.cpp)",
            text,
        ):
            if name in tests:
                sources[f"{relative}/{source}"] = tests[name]
    selected = {"common", "unclassified"}
    for path in paths:
        if path.endswith((".md", ".rst")):
            continue
        owner = sources.get(path)
        if (
            owner is None
            and path.startswith("mooncake-store/src/")
            and path.endswith(".h")
        ):
            owner = sources.get(path[:-2] + ".cpp")
        if owner in ("store-client", "store-master"):
            selected.update((owner, "store-shared", "client-master-contract"))
        elif owner == "store-shared":
            return {"all"}
        elif path.startswith(
            (
                "mooncake-transfer-engine/include/",
                "mooncake-transfer-engine/tent/include/",
            )
        ):
            return {"all"}
        elif path.startswith("mooncake-transfer-engine/") and Path(path).suffix in (
            ".cpp",
            ".h",
            ".hpp",
            ".c",
            ".cu",
            ".cuh",
            ".rs",
        ):
            # TENT is exercised by tent-ci; the classic job checks its facade
            # consumers without pulling master implementation tests into it.
            selected.update(("te", "store-client", "store-shared"))
        elif path.startswith(("mooncake-pg/", "mooncake-ep/")) and Path(
            path
        ).suffix in (".cpp", ".h", ".hpp", ".cu", ".cuh", ".py"):
            selected.add("ep")
        elif path.startswith("mooncake-conductor/") and Path(path).suffix in (
            ".cpp",
            ".h",
        ):
            selected.add("conductor")
        else:
            return {"all"}
    # Do not silently pass an empty/unrecognized source selection.
    return selected if len(selected) > 2 else {"all"}


def selection_outputs(components):
    full = "all" in components
    return {
        "unit_test_labels": ""
        if full
        else "^component-(" + "|".join(sorted(components)) + ")$",
        "unit_test_targets": ""
        if full
        else " ".join("mooncake-tests-" + c for c in sorted(components)),
        "te_tests": str(full or "te" in components).lower(),
        "pg_tests": str(full or "ep" in components).lower(),
        "store_client_tests": str(full or "store-client" in components).lower(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--files-json", default="[]")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()
    paths = json.loads(args.files_json)
    if not isinstance(paths, list) or any(not isinstance(p, str) for p in paths):
        parser.error("--files-json must be an array of paths")
    components = select_components(paths, full=args.full)
    output = "".join(
        f"{key}={value}\n" for key, value in selection_outputs(components).items()
    )
    print(output, end="")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
            stream.write(output)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
            stream.write(
                "### Unit-test components\n\n" + ", ".join(sorted(components)) + "\n"
            )


if __name__ == "__main__":
    main()
