#!/usr/bin/env python3
"""Generates hash_golden_vectors_sha256.json from the vLLM `sha256` recipe.

The `sha256` vLLM prefix-cache hash is SHA-256 over
``pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)``; for the supported
Python range (3.10-3.14) HIGHEST_PROTOCOL is protocol 5.  This script uses the
real CPython pickler as the normative oracle and replicates
vLLM's ``hash_block_tokens`` value shapes:

    root  = sha256(pickle(seed_string))
    block = sha256(pickle((parent_digest_bytes, tuple(block_token_ids),
                           extra_keys)))

with ``extra_keys`` equal to None, or the tuple ``(lora_name,)`` /
``(lora_name, cache_salt)`` (salt only on the first block), matching
vLLM's ``lora + mm + cache_salt + prompt_embeds`` ordering for the subset the
Conductor query contract can express.

The checked-in fixture pins the generator environment below.  Regenerate with:

    python3 generate_hash_golden_vectors.py

from this directory and diff the result; only regenerate intentionally.
"""

import hashlib
import json
import pickle
import platform
import sys
from pathlib import Path

PROTOCOL = pickle.HIGHEST_PROTOCOL
assert PROTOCOL == 5, f"expected Pickle protocol 5, got {PROTOCOL}"

# Pinned reference: vLLM v1 kv_cache_utils.hash_block_tokens +
# utils/hashing.py sha256, verified against checkout
# v0.22.1rc0-459-g462ef83d5 (commit 462ef83d58e6fadeb6e216dc583554a6980a0af9).
VLLM_REFERENCE = (
    "vLLM v0.22.1rc0-459-g462ef83d5 hash_block_tokens value shapes with "
    "utils/hashing.py sha256 = SHA256(pickle.dumps(value, protocol=5))"
)

# Serialized block bytes are recorded in full for every case small enough to
# keep the fixture reviewable.  Cases that must cross CPython's 64 KiB frame
# target (frame boundary and large-payload paths) record pickle_len instead;
# their digest is still the SHA-256 of the exact serialized bytes, so a digest
# match certifies byte equality.
PICKLE_HEX_SIZE_LIMIT = 8192

SEEDS = ["0", "00", "random", "4294967295"]


def sha256_pickle(value) -> bytes:
    return hashlib.sha256(pickle.dumps(value, protocol=PROTOCOL)).digest()


def extras_for(lora_name: str, cache_salt, block_index: int):
    keys = []
    if lora_name:
        keys.append(lora_name)
    if block_index == 0 and cache_salt:
        keys.append(cache_salt)
    return tuple(keys) if keys else None


def block_entry(pickle_bytes: bytes) -> dict:
    digest = hashlib.sha256(pickle_bytes).digest()
    entry = {
        "digest": digest.hex(),
        "projected_hex": digest[-8:].hex(),
        "projected_decimal": str(int.from_bytes(digest[-8:], "big")),
    }
    if len(pickle_bytes) <= PICKLE_HEX_SIZE_LIMIT:
        entry["pickle_hex"] = pickle_bytes.hex()
    else:
        entry["pickle_len"] = len(pickle_bytes)
    return entry


def expand_tokens(case: dict):
    if "token_ids_repeat" in case:
        spec = case["token_ids_repeat"]
        return [spec["value"]] * spec["count"]
    return list(case["token_ids"])


CASES = [
    {
        "name": "spec_unsalted",
        "block_size": 4,
        "lora_name": "",
        "cache_salt": None,
        "token_ids": [1, 2, 3, 4, 5, 6, 7, 8],
    },
    {
        "name": "single_token_tuple",
        "block_size": 1,
        "lora_name": "",
        "cache_salt": None,
        "token_ids": [7],
    },
    {
        "name": "two_token_tuple",
        "block_size": 2,
        "lora_name": "",
        "cache_salt": None,
        "token_ids": [7, 8],
    },
    {
        "name": "three_token_tuple",
        "block_size": 3,
        "lora_name": "",
        "cache_salt": None,
        "token_ids": [7, 8, 9],
    },
    {
        "name": "empty_extras_encode_as_none",
        "block_size": 4,
        "lora_name": "",
        "cache_salt": None,
        "token_ids": [10, 20, 30, 40],
    },
    {
        "name": "signed_integer_boundaries",
        "block_size": 8,
        "lora_name": "",
        "cache_salt": None,
        "token_ids": [-2147483648, -1, 0, 255, 256, 65535, 65536, 2147483647],
    },
    {
        "name": "utf8_multibyte",
        "block_size": 4,
        "lora_name": "模型-适配器",
        "cache_salt": "盐值-🚀",
        "token_ids": [1, 2, 3, 4],
    },
    {
        "name": "lora_length_255",
        "block_size": 4,
        "lora_name": "a" * 255,
        "cache_salt": None,
        "token_ids": [1, 2, 3, 4],
    },
    {
        "name": "lora_length_256",
        "block_size": 4,
        "lora_name": "a" * 256,
        "cache_salt": None,
        "token_ids": [1, 2, 3, 4],
    },
    {
        "name": "salt_length_255",
        "block_size": 4,
        "lora_name": "",
        "cache_salt": "s" * 255,
        "token_ids": [1, 2, 3, 4],
    },
    {
        "name": "salt_length_256",
        "block_size": 4,
        "lora_name": "",
        "cache_salt": "s" * 256,
        "token_ids": [1, 2, 3, 4],
    },
    {
        "name": "lora_every_block",
        "block_size": 4,
        "lora_name": "adapter-A",
        "cache_salt": None,
        "token_ids": [1, 2, 3, 4, 5, 6, 7, 8],
    },
    {
        "name": "salt_first_block_only",
        "block_size": 4,
        "lora_name": "",
        "cache_salt": "tenant-salt",
        "token_ids": [1, 2, 3, 4, 5, 6, 7, 8],
    },
    {
        "name": "lora_then_salt",
        "block_size": 4,
        "lora_name": "adapter-A",
        "cache_salt": "tenant-salt",
        "token_ids": [1, 2, 3, 4, 5, 6, 7, 8],
    },
    {
        "name": "multi_block_chain",
        "block_size": 4,
        "lora_name": "",
        "cache_salt": None,
        "token_ids": list(range(1, 17)),
    },
    {
        "name": "incomplete_tail",
        "block_size": 4,
        "lora_name": "",
        "cache_salt": None,
        "token_ids": [1, 2, 3, 4, 5, 6],
    },
    {
        "name": "no_complete_block",
        "block_size": 4,
        "lora_name": "",
        "cache_salt": None,
        "token_ids": [1, 2, 3],
    },
    {
        # 22000 BININT2 tokens serialize to ~66 KiB inside the token tuple,
        # forcing CPython's _Framer to commit a frame mid-value.
        "name": "frame_boundary_token_stream",
        "block_size": 22000,
        "lora_name": "",
        "cache_salt": None,
        "token_ids_repeat": {"value": 256, "count": 22000},
    },
    {
        # A >= 64 KiB extra-key string takes CPython's write_large_bytes path:
        # the pending frame is force-committed and the string is written
        # without a frame opcode.
        "name": "large_lora_large_payload",
        "block_size": 4,
        "lora_name": "a" * 70000,
        "cache_salt": None,
        "token_ids": [1, 2, 3, 4],
    },
]


def main() -> None:
    seed_root_vectors = []
    for seed in SEEDS:
        serialized = pickle.dumps(seed, protocol=PROTOCOL)
        seed_root_vectors.append(
            {
                "python_hash_seed": seed,
                "pickle_hex": serialized.hex(),
                "root_digest": hashlib.sha256(serialized).hexdigest(),
            }
        )

    root_digest = sha256_pickle("0")
    cases = []
    for case in CASES:
        tokens = expand_tokens(case)
        block_size = case["block_size"]
        lora_name = case["lora_name"]
        cache_salt = case["cache_salt"]

        expected = []
        parent = root_digest
        for block_index in range(len(tokens) // block_size):
            block_tokens = tokens[
                block_index * block_size : (block_index + 1) * block_size
            ]
            value = (
                parent,
                tuple(block_tokens),
                extras_for(lora_name, cache_salt, block_index),
            )
            serialized = pickle.dumps(value, protocol=PROTOCOL)
            expected.append(block_entry(serialized))
            parent = hashlib.sha256(serialized).digest()

        entry = {
            "name": case["name"],
            "block_size": block_size,
            "lora_name": lora_name if len(lora_name) <= 512 else "",
            "cache_salt": cache_salt,
            "expected": expected,
        }
        if len(lora_name) > 512:
            entry["lora_name_repeat"] = {"value": "a", "count": len(lora_name)}
        if "token_ids_repeat" in case:
            entry["token_ids_repeat"] = case["token_ids_repeat"]
        else:
            entry["token_ids"] = tokens

        if case["name"] == "spec_unsalted":
            # Digest of the second block when the low-64 projection is
            # incorrectly reused as the parent instead of the full digest.
            first_digest = bytes.fromhex(expected[0]["digest"])
            wrong_value = (
                first_digest[-8:],
                tuple(tokens[4:8]),
                None,
            )
            entry["incorrect_low64_parent_digest"] = hashlib.sha256(
                pickle.dumps(wrong_value, protocol=PROTOCOL)
            ).hexdigest()

        cases.append(entry)

    fixture = {
        "description": (
            "Golden vectors for the supported vLLM v1 sha256 (CPython Pickle "
            "protocol 5) seed-derived root and block-hash profile.  pickle_hex "
            "records the exact serialized bytes hashed for small values; large "
            "frame-boundary values record pickle_len and are certified by "
            "their digest."
        ),
        "generator": VLLM_REFERENCE,
        "generator_metadata": {
            "python_version": platform.python_version(),
            "pickle_protocol": PROTOCOL,
            "pickle_highest_protocol": pickle.HIGHEST_PROTOCOL,
        },
        "profile": {
            "strategy": "vllm_v1",
            "algorithm": "sha256",
            "python_hash_seed": "0",
            "root_digest": root_digest.hex(),
            "index_projection": "low64_be",
        },
        "seed_root_vectors": seed_root_vectors,
        "cases": cases,
    }

    out = Path(__file__).with_name("hash_golden_vectors_sha256.json")
    out.write_text(json.dumps(fixture, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    sys.exit(main())
