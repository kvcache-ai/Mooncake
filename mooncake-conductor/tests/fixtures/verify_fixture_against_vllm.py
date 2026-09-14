#!/usr/bin/env python3
"""Verifies hash_golden_vectors_sha256.json against the checked-out vLLM.

Loads ``vllm/utils/hashing.py`` directly from a vLLM source checkout (without
importing the heavy ``vllm`` package) and uses its ``sha256`` function as the
normative oracle: SHA-256 over ``pickle.dumps(value, protocol=5)``.  The block
value shapes mirror ``vllm/v1/core/kv_cache_utils.py::hash_block_tokens``.

Usage:

    python3 verify_fixture_against_vllm.py <path-to-vllm-checkout>

Prints the Python/vLLM/protocol metadata it ran against and exits non-zero on
any mismatch.
"""

import importlib.util
import json
import pickle
import platform
import subprocess
import sys
import types
from pathlib import Path

FIXTURE = Path(__file__).with_name("hash_golden_vectors_sha256.json")


def load_vllm_hashing(checkout: Path):
    hashing_py = checkout / "vllm" / "utils" / "hashing.py"
    if not hashing_py.is_file():
        raise SystemExit(f"vLLM hashing.py not found at {hashing_py}")
    # hashing.py imports cbor2 at module scope; the sha256 recipe under test
    # never touches it, so a stub is sufficient when cbor2 is absent.
    if "cbor2" not in sys.modules:
        try:
            import cbor2  # noqa: F401
        except ImportError:
            sys.modules["cbor2"] = types.ModuleType("cbor2")
    spec = importlib.util.spec_from_file_location("vllm_hashing", hashing_py)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.sha256


def vllm_revision(checkout: Path) -> str:
    try:
        return subprocess.run(
            ["git", "-C", str(checkout), "describe", "--tags", "--always"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def expand_tokens(case: dict):
    if "token_ids_repeat" in case:
        spec = case["token_ids_repeat"]
        return [spec["value"]] * spec["count"]
    return list(case["token_ids"])


def expand_lora(case: dict) -> str:
    if "lora_name_repeat" in case:
        spec = case["lora_name_repeat"]
        return spec["value"] * spec["count"]
    return case["lora_name"]


def extras_for(lora_name: str, cache_salt, block_index: int):
    keys = []
    if lora_name:
        keys.append(lora_name)
    if block_index == 0 and cache_salt:
        keys.append(cache_salt)
    return tuple(keys) if keys else None


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {Path(__file__).name} <path-to-vllm-checkout>")
    checkout = Path(sys.argv[1])
    vllm_sha256 = load_vllm_hashing(checkout)
    # The fixture carries non-ASCII cache salts, so the encoding is explicit
    # rather than platform-dependent.
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))

    print(f"python: {platform.python_version()}")
    print(f"pickle protocol: {pickle.HIGHEST_PROTOCOL}")
    print(f"vLLM checkout: {checkout} ({vllm_revision(checkout)})")

    failures = 0

    def check(label: str, actual: str, expected: str) -> None:
        nonlocal failures
        if actual != expected:
            failures += 1
            print(f"MISMATCH {label}:\n  actual   {actual}\n  expected {expected}")

    for vector in fixture["seed_root_vectors"]:
        seed = vector["python_hash_seed"]
        check(f"seed root {seed!r}", vllm_sha256(seed).hex(), vector["root_digest"])
        check(
            f"seed pickle bytes {seed!r}",
            pickle.dumps(seed, protocol=5).hex(),
            vector["pickle_hex"],
        )

    root = bytes.fromhex(fixture["profile"]["root_digest"])
    for case in fixture["cases"]:
        tokens = expand_tokens(case)
        block_size = case["block_size"]
        lora_name = expand_lora(case)
        cache_salt = case["cache_salt"]
        parent = root
        for index, entry in enumerate(case["expected"]):
            block_tokens = tuple(tokens[index * block_size : (index + 1) * block_size])
            value = (
                parent,
                block_tokens,
                extras_for(lora_name, cache_salt, index),
            )
            serialized = pickle.dumps(value, protocol=5)
            digest = vllm_sha256(value).hex()
            check(f"{case['name']} block {index}", digest, entry["digest"])
            if "pickle_hex" in entry:
                check(
                    f"{case['name']} block {index} pickle bytes",
                    serialized.hex(),
                    entry["pickle_hex"],
                )
            else:
                check(
                    f"{case['name']} block {index} pickle length",
                    str(len(serialized)),
                    str(entry["pickle_len"]),
                )
            parent = bytes.fromhex(digest)

    if failures:
        print(f"{failures} mismatch(es)")
        return 1
    print("all seed roots, block digests, and serialized bytes match vLLM")
    return 0


if __name__ == "__main__":
    sys.exit(main())
