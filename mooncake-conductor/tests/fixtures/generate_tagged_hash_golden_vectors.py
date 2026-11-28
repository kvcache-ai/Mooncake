#!/usr/bin/env python3
"""Generates hash_golden_vectors_vllm_tagged.json for the vllm_v1_tagged
strategy.

vLLM v0.31 (PR #51899) changed the extra keys that enter
``hash_block_tokens`` from bare strings to tagged tuples.  For the subset the
Conductor query contract can express (cache salt, no LoRA), a block value is

    (parent_digest_bytes, tuple(block_token_ids), extra_keys)

with ``extra_keys`` equal to ``(("cache_salt", salt),)`` on the first block of
a request with a non-empty salt and None otherwise.  The digest is
``sha256(pickle.dumps(value, protocol=5))`` for ``sha256`` and
``sha256(cbor2.dumps(value, canonical=True))`` for ``sha256_cbor``; the root
is the same function applied to the seed string.  The real CPython pickler and
cbor2 are the normative serializers here.

Each suite pins one (algorithm, python_hash_seed) profile.  ``vllm-none-hash``
is vLLM's default seed for cryptographic hashes when PYTHONHASHSEED is unset
(vLLM v0.29+).

Regenerate with (cbor2 required):

    python3 generate_tagged_hash_golden_vectors.py

from this directory and diff the result; only regenerate intentionally.
"""

import hashlib
import json
import pickle
import platform
import sys
from importlib.metadata import version
from pathlib import Path

import cbor2

PROTOCOL = pickle.HIGHEST_PROTOCOL
assert PROTOCOL == 5, f"expected Pickle protocol 5, got {PROTOCOL}"

VLLM_REFERENCE = (
    "vLLM main 70b67cf805 (v0.31.1rc0+) kv_cache_utils."
    "generate_block_hash_extra_keys + hash_block_tokens with utils/hashing.py "
    "sha256 (pickle protocol 5) and sha256_cbor (cbor2 canonical=True)"
)

SERIALIZED_HEX_SIZE_LIMIT = 8192

SUITES = [
    ("sha256", "0"),
    ("sha256_cbor", "0"),
    ("sha256", "vllm-none-hash"),
    ("sha256_cbor", "vllm-none-hash"),
]

CASES = [
    {
        "name": "unsalted_matches_untagged_shape",
        "block_size": 4,
        "cache_salt": None,
        "token_ids": [1, 2, 3, 4, 5, 6, 7, 8],
    },
    {
        "name": "empty_salt_has_no_extra_key",
        "block_size": 4,
        "cache_salt": "",
        "token_ids": [1, 2, 3, 4, 5, 6, 7, 8],
    },
    {
        "name": "salt_first_block_only",
        "block_size": 4,
        "cache_salt": "tenant-salt",
        "token_ids": [1, 2, 3, 4, 5, 6, 7, 8],
    },
    {
        "name": "utf8_multibyte_salt",
        "block_size": 4,
        "cache_salt": "盐值-🚀",
        "token_ids": [1, 2, 3, 4],
    },
    {
        "name": "salt_length_255",
        "block_size": 4,
        "cache_salt": "s" * 255,
        "token_ids": [1, 2, 3, 4],
    },
    {
        "name": "salt_length_256",
        "block_size": 4,
        "cache_salt": "s" * 256,
        "token_ids": [1, 2, 3, 4],
    },
    {
        # A >= 64 KiB salt takes CPython's write_large_bytes path inside the
        # nested tagged tuple.
        "name": "large_salt_large_payload",
        "block_size": 4,
        "cache_salt": "s" * 70000,
        "token_ids": [1, 2, 3, 4],
    },
]


def serializer(algorithm: str):
    if algorithm == "sha256":
        return lambda value: pickle.dumps(value, protocol=PROTOCOL)
    if algorithm == "sha256_cbor":
        return lambda value: cbor2.dumps(value, canonical=True)
    raise ValueError(algorithm)


def extras_for(cache_salt, block_index: int):
    # vLLM: lora_extra_keys + mm_extra_keys + cache_salt_keys + embeds_keys,
    # with the salt key only when start_token_idx == 0 and the salt is truthy.
    if block_index == 0 and cache_salt:
        return (("cache_salt", cache_salt),)
    return None


def block_entry(serialized: bytes) -> dict:
    digest = hashlib.sha256(serialized).digest()
    entry = {
        "digest": digest.hex(),
        "projected_hex": digest[-8:].hex(),
        "projected_decimal": str(int.from_bytes(digest[-8:], "big")),
    }
    if len(serialized) <= SERIALIZED_HEX_SIZE_LIMIT:
        entry["serialized_hex"] = serialized.hex()
    else:
        entry["serialized_len"] = len(serialized)
    return entry


def build_suite(algorithm: str, seed: str) -> dict:
    dumps = serializer(algorithm)
    root = hashlib.sha256(dumps(seed)).digest()
    cases = []
    for case in CASES:
        tokens = case["token_ids"]
        block_size = case["block_size"]
        salt = case["cache_salt"]
        expected = []
        parent = root
        for block_index in range(len(tokens) // block_size):
            value = (
                parent,
                tuple(
                    tokens[block_index * block_size : (block_index + 1) * block_size]
                ),
                extras_for(salt, block_index),
            )
            serialized = dumps(value)
            expected.append(block_entry(serialized))
            parent = hashlib.sha256(serialized).digest()
        entry = {
            "name": case["name"],
            "block_size": block_size,
            "token_ids": tokens,
            "expected": expected,
        }
        if salt is not None and len(salt) > 512:
            entry["cache_salt"] = ""
            entry["cache_salt_repeat"] = {"value": salt[0], "count": len(salt)}
        else:
            entry["cache_salt"] = salt
        cases.append(entry)
    return {
        "profile": {
            "strategy": "vllm_v1_tagged",
            "algorithm": algorithm,
            "python_hash_seed": seed,
            "root_digest": root.hex(),
            "index_projection": "low64_be",
        },
        "seed_serialized_hex": dumps(seed).hex(),
        "cases": cases,
    }


def main() -> None:
    fixture = {
        "description": (
            "Golden vectors for the vllm_v1_tagged strategy: vLLM >= v0.31 "
            "tagged cache-salt extra keys under sha256 (Pickle protocol 5) "
            "and sha256_cbor (canonical CBOR).  serialized_hex records the "
            "exact bytes hashed for small values; large values record "
            "serialized_len and are certified by their digest."
        ),
        "generator": VLLM_REFERENCE,
        "generator_metadata": {
            "python_version": platform.python_version(),
            "pickle_protocol": PROTOCOL,
            "cbor2_version": version("cbor2"),
        },
        "suites": [build_suite(algorithm, seed) for algorithm, seed in SUITES],
    }
    out = Path(__file__).with_name("hash_golden_vectors_vllm_tagged.json")
    out.write_text(
        json.dumps(fixture, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"wrote {out}")


if __name__ == "__main__":
    sys.exit(main())
