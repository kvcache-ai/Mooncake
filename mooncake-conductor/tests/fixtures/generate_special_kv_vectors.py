"""Generate receiver fixtures from pinned, unmodified producer definitions.

Run outside the inference repositories; importing their runtime is unnecessary.
Only event schemas and pure namespace helpers are loaded from their source AST.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import struct
import subprocess
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List, Optional, Union

import msgspec


def matcher_vectors(vllm: Path) -> list[dict]:
    """Run actual vLLM matcher methods, replacing only BlockPool storage."""

    class Spec(SimpleNamespace):
        pass

    class Pool:
        hash_block_size = 16
        null_block = object()

        def __init__(self, present):
            self.present = present

        def get_cached_block(self, block_hash, group_ids):
            if block_hash not in self.present:
                return None
            return tuple(SimpleNamespace(block_id=block_hash) for _ in group_ids)

    scope = {
        "SlidingWindowSpec": Spec,
        "MambaSpec": Spec,
        "Sequence": Sequence,
        "cdiv": lambda a, b: (a + b - 1) // b,
    }
    util_path = vllm / "vllm/v1/core/kv_cache_utils.py"
    # Equal physical/hash sizes exercise the real resolver's identity branch.
    resolver = next(
        n
        for n in ast.parse(util_path.read_text(encoding="utf-8")).body
        if isinstance(n, ast.FunctionDef) and n.name == "resolve_block_hashes"
    )
    source_path = vllm / "vllm/v1/core/single_type_kv_cache_manager.py"
    source = ast.parse(source_path.read_text(encoding="utf-8"))
    selected = [
        ast.ImportFrom(module="__future__", names=[ast.alias("annotations")], level=0),
        resolver,
    ]
    for name in ("SlidingWindowManager", "MambaManager"):
        original = next(
            n for n in source.body if isinstance(n, ast.ClassDef) and n.name == name
        )
        methods = [
            n
            for n in original.body
            if isinstance(n, ast.FunctionDef)
            and n.name in {"find_longest_cache_hit", "_contiguous_blocks_for_hit"}
        ]
        selected.append(
            ast.ClassDef(
                name=name, bases=[], keywords=[], body=methods, decorator_list=[]
            )
        )
    module = ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[]))
    exec(compile(module, str(source_path), "exec"), scope)
    cases = []
    for policy, name in (
        ("checkpoint", "MambaManager"),
        ("window", "SlidingWindowManager"),
    ):
        manager = scope[name]
        manager.supports_fine_grained_hash_lookup = False
        spec = Spec(block_size=16, sliding_window=33)
        for mask in range(0, 256, 7):
            present = [i + 1 for i in range(8) if mask & (1 << i)]
            expected = manager.find_longest_cache_hit(
                list(range(1, 9)), 128, [0], Pool(set(present)), spec, False, 16
            )[1]
            cases.append(
                {
                    "policy": policy,
                    "present": [16 * x for x in present],
                    "expected": expected,
                }
            )
    return cases


def definitions(path: Path, names: set[str], scope: dict) -> dict:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = []
    found = set()
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names:
            nodes.append(node)
            found.add(node.name)
    if found != names:
        raise ValueError(f"Missing producer definitions: {names - found}")
    module = ast.Module(body=nodes, type_ignores=[])
    exec(compile(module, str(path), "exec"), scope)
    return scope


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sglang", type=Path, required=True)
    parser.add_argument("--vllm", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).with_name("special_kv_vectors.json"),
    )
    args = parser.parse_args()
    common = {
        "msgspec": msgspec,
        "Any": Any,
        "Optional": Optional,
        "Union": Union,
        "List": List,
        "hashlib": hashlib,
    }
    sg = definitions(
        args.sglang / "python/sglang/srt/disaggregation/kv_events.py",
        {
            "EventBatch",
            "KVCacheEvent",
            "BlockStored",
            "BlockRemoved",
            "AllBlocksCleared",
            "KVEventBatch",
            "kv_event_namespace_seed",
            "namespaced_block_hash",
        },
        {**common, "_NAMESPACE_SEED_TAG": b"sglang-kv-event-namespace-v1"},
    )
    utils = definitions(
        args.sglang / "python/sglang/srt/mem_cache/utils.py",
        {"storage_namespace_seed"},
        dict(common),
    )
    vl = definitions(
        args.vllm / "vllm/distributed/kv_events.py",
        {
            "EventBatch",
            "KVCacheEvent",
            "BlockStored",
            "BlockRemoved",
            "AllBlocksCleared",
        },
        {**common, "ExternalBlockHash": Union[int, bytes]},
    )
    tokens = [1, 2, 3, 4, 5]
    cases = []
    for bigram in (False, True):
        logical = list(zip(tokens, tokens[1:])) if bigram else tokens
        for extra, salt in (
            (None, None),
            (None, ""),
            (None, "tenant-A"),
            ("adapter-load-id", "tenant-A"),
        ):
            for domain in ("sglang_storage", "sglang_event", "sglang_dynamo"):
                if domain == "sglang_storage":
                    root = utils["storage_namespace_seed"](extra, salt)
                    parent = bytes.fromhex(root) if root is not None else b""
                    seed = None
                else:
                    parent = (
                        hashlib.sha256(
                            b"sglang-cache-salt-v1\0" + salt.encode()
                        ).digest()
                        if salt is not None
                        else b""
                    )
                    seed = sg["kv_event_namespace_seed"](
                        extra_key=extra,
                        cache_salt=salt if domain == "sglang_dynamo" else None,
                    )
                hashes = []
                for start in range(0, len(logical), 2):
                    page = logical[start : start + 2]
                    flat = [x for pair in page for x in pair] if bigram else page
                    parent = hashlib.sha256(
                        parent + struct.pack("<" + "I" * len(flat), *flat)
                    ).digest()
                    raw = int.from_bytes(parent[:8], "big", signed=True)
                    value = sg["namespaced_block_hash"](raw, namespace_seed=seed)
                    hashes.append(str(value & ((1 << 64) - 1)))
                cases.append(
                    {
                        "bigram": bigram,
                        "domain": domain,
                        "salt": salt,
                        "extra_key": extra,
                        "hashes": hashes,
                    }
                )
    # Producer serializers, including the valid empty-token Mamba checkpoint.
    stored = vl["BlockStored"](
        block_hashes=[123],
        parent_block_hash=None,
        token_ids=[],
        block_size=64,
        lora_id=None,
        medium="GPU",
        lora_name=None,
        group_idx=1,
        kv_cache_spec_kind="mamba",
    )
    sg_stored = sg["BlockStored"](
        block_hashes=[-1],
        parent_block_hash=None,
        token_ids=[1, 2],
        block_size=2,
        lora_id=None,
        medium="gpu",
        cache_salt="tenant-A",
    )
    fixtures = {
        "sglang_commit": subprocess.check_output(
            ["git", "-C", str(args.sglang), "rev-parse", "HEAD"], text=True
        ).strip(),
        "vllm_commit": subprocess.check_output(
            ["git", "-C", str(args.vllm), "rev-parse", "HEAD"], text=True
        ).strip(),
        "tokens": tokens,
        "block_size": 2,
        "hash_cases": cases,
        "matcher_cases": matcher_vectors(args.vllm),
        "vllm_checkpoint_msgpack": msgspec.msgpack.encode(
            vl["EventBatch"](1.0, [stored], 0)
        ).hex(),
        "sglang_page_msgpack": msgspec.msgpack.encode(
            sg["KVEventBatch"](1.0, [sg_stored], 0)
        ).hex(),
    }
    args.output.write_text(json.dumps(fixtures, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
