# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Modified by Hygon Information Technology Co., Ltd., 2026.

"""Mooncake public Python package."""

from mooncake.buffer_pool import BufferPool, RegisteredBufferPool
from mooncake.version import __version__, __version_tuple__, __hcu_version__

import os
import re
from pathlib import Path


def _ib_sysfs_devices():
    path = Path("/sys/class/infiniband")
    if not path.exists():
        return []
    try:
        return sorted(p.name for p in path.iterdir())
    except Exception:
        return []


def _expects_shca_environment():
    if any(dev.startswith("shca") for dev in _ib_sysfs_devices()):
        return True
    return False


def _mooncake_distributions():
    try:
        from importlib import metadata as importlib_metadata
    except Exception:
        return []

    distributions = []
    seen = set()

    def add_distribution(name):
        key = name.lower()
        if key in seen:
            return
        try:
            dist = importlib_metadata.distribution(name)
            dist_name = dist.metadata.get("Name", name)
            distributions.append(
                {
                    "name": dist_name,
                    "summary": dist.metadata.get("Summary", ""),
                }
            )
            seen.add(dist_name.lower())
        except Exception:
            distributions.append({"name": name, "summary": ""})
            seen.add(key)

    try:
        package_map = importlib_metadata.packages_distributions()
        names = package_map.get("mooncake", [])
        for name in names:
            add_distribution(name)
    except Exception:
        pass

    for name in (
        "mooncake-transfer-engine-shca",
        "mooncake-transfer-engine",
    ):
        try:
            importlib_metadata.distribution(name)
        except Exception:
            continue
        add_distribution(name)

    return distributions


def _include_search_paths():
    paths = []
    for env_name in ("CPATH", "C_INCLUDE_PATH"):
        for item in os.environ.get(env_name, "").split(":"):
            if item:
                paths.append(Path(item))
    paths.extend((Path("/usr/local/include"), Path("/usr/include"), Path("/opt/hyhal/include")))

    unique = []
    seen = set()
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def _find_verbs_header():
    for base in _include_search_paths():
        candidate = base / "infiniband" / "verbs.h"
        if candidate.exists():
            return candidate
    return None


def _extract_struct_body(text, struct_name):
    match = re.search(r"\bstruct\s+" + re.escape(struct_name) + r"\s*\{", text)
    if not match:
        return ""

    start = match.end()
    depth = 1
    idx = start
    while idx < len(text):
        char = text[idx]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start:idx]
        idx += 1
    return ""


def _field_declaration(struct_body, field_name):
    for declaration in struct_body.split(";"):
        normalized = " ".join(declaration.split())
        if not normalized:
            continue
        if re.search(
            r"\b" + re.escape(field_name) + r"\b\s*(?:\[[^\]]*\])?\s*(?::\s*\d+)?$",
            normalized,
        ):
            return normalized + ";"
    return ""


def _dlid_decl_requires_shca(dlid_decl):
    decl = dlid_decl.lower()
    return "u17" in decl or "uint17" in decl or bool(re.search(r":\s*17\b", decl))


def _dlid_decl_is_standard(dlid_decl):
    decl = dlid_decl.lower()
    return "uint16_t" in decl or "uint16" in decl or bool(re.search(r":\s*16\b", decl))


def _shca_driver_layout_requirement():
    header = _find_verbs_header()
    if header is None:
        return "unknown"
    try:
        text = header.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return "unknown"

    ah_attr = _extract_struct_body(text, "ibv_ah_attr")
    dlid_decl = _field_declaration(ah_attr, "dlid")
    if not dlid_decl:
        return "unknown"
    if _dlid_decl_requires_shca(dlid_decl):
        return "shca"
    if _dlid_decl_is_standard(dlid_decl):
        return "standard"
    return "unknown"


def _check_import_compatibility():
    distributions = _mooncake_distributions()
    has_standard_dist = any(
        dist.get("name", "").lower() == "mooncake-transfer-engine"
        for dist in distributions
    )
    has_shca_dist = any(
        dist.get("name", "").lower() == "mooncake-transfer-engine-shca"
        for dist in distributions
    )
    if has_standard_dist and has_shca_dist:
        raise RuntimeError(
            "Mooncake package conflict: both mooncake-transfer-engine and "
            "mooncake-transfer-engine-shca appear to be installed. Uninstall both "
            "packages, then reinstall only the variant that matches this host."
        )

    if not _expects_shca_environment():
        return

    requirement = _shca_driver_layout_requirement()

    if requirement == "shca" and not has_shca_dist:
        raise RuntimeError(
            "Mooncake package mismatch: u17/17-bit SHCA driver layout detected, "
            "but installed package is not the SHCA variant. Install "
            "mooncake-transfer-engine-shca or rebuild with -DUSE_SHCA=ON."
        )

    if requirement == "standard" and has_shca_dist:
        raise RuntimeError(
            "Mooncake package mismatch: standard-aligned SHCA driver layout detected, "
            "but installed package is the SHCA variant. Install the standard "
            "mooncake-transfer-engine package without the -shca suffix."
        )


_check_import_compatibility()

__all__ = ["BufferPool", "RegisteredBufferPool"]
