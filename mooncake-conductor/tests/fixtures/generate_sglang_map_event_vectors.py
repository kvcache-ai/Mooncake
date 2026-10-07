#!/usr/bin/env python3
"""Generates sglang_map_event_vectors.json from upstream SGLang definitions.

The payloads are encoded by the real ``msgspec.msgpack.Encoder`` against the
``EventBatch``/``KVCacheEvent``/``BlockStored``/``BlockRemoved``/
``AllBlocksCleared`` definitions in a local SGLang checkout, so the fixture is
the wire format the publisher actually emits rather than a hand-written guess.

``EventBatch`` stays a positional ``[ts, events, attn_dp_rank]`` array while
each event is a tagged map (``tag=True``, no ``array_like``) whose defaulted
fields are omitted (``omit_defaults=True``).

The checked-in fixture pins the generator environment below.  Regenerate with:

    python3 generate_sglang_map_event_vectors.py --sglang-root /path/to/sglang

from this directory and diff the result; only regenerate intentionally.  The
C++ tests consume the committed hex, so neither an SGLang checkout nor a Python
runtime is needed to run them.
"""

import argparse
import ast
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional, Union

try:
    import msgspec
except ImportError:  # pragma: no cover - environment guard
    sys.exit("msgspec is required: pip install msgspec")

# Path of the KV-event definitions inside an SGLang checkout.
KV_EVENTS_RELATIVE = Path("python/sglang/srt/disaggregation/kv_events.py")

# Definitions loaded from that module.  Importing the package pulls in GPU-only
# dependencies, so the exact class statements are extracted and executed on
# their own; the selection is asserted below so an upstream rename fails loudly
# instead of silently producing a stale fixture.
REQUIRED_DEFINITIONS = (
    "EventBatch",
    "KVCacheEvent",
    "BlockStored",
    "BlockRemoved",
    "AllBlocksCleared",
    "KVEventBatch",
)


def load_definitions(kv_events_path: Path) -> dict:
    """Executes only the required class statements from kv_events.py."""
    source = kv_events_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    selected = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name in REQUIRED_DEFINITIONS
    }
    missing = [name for name in REQUIRED_DEFINITIONS if name not in selected]
    if missing:
        sys.exit(
            "upstream definitions not found in "
            f"{kv_events_path}: {', '.join(missing)}. The schema changed; "
            "update this generator deliberately."
        )
    # Keep upstream's declaration order so base classes precede subclasses.
    ordered = [
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name in REQUIRED_DEFINITIONS
    ]
    namespace: dict = {
        "msgspec": msgspec,
        "Any": Any,
        "Optional": Optional,
        "Union": Union,
    }
    module = ast.Module(body=ordered, type_ignores=[])
    exec(compile(module, str(kv_events_path), "exec"), namespace)  # noqa: S102
    return namespace


def git_commit(root: Path) -> str:
    try:
        return subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def build_cases(ns: dict) -> list:
    """Payloads covering the field and type combinations the decoder must
    handle.  Every expectation mirrors what the encoder produced, so the
    fixture cannot disagree with upstream by construction."""
    stored = ns["BlockStored"]
    removed = ns["BlockRemoved"]
    cleared = ns["AllBlocksCleared"]
    batch = ns["KVEventBatch"]

    return [
        {
            "name": "stored_removed_cleared",
            "comment": "One batch carrying each event type in order.",
            "batch": batch(
                ts=12.5,
                events=[
                    stored(
                        block_hashes=[-1, 2],
                        parent_block_hash=None,
                        token_ids=[1, 2, 3, 4],
                        block_size=2,
                        lora_id=None,
                        medium="GPU",
                    ),
                    removed(block_hashes=[-1], medium="GPU"),
                    cleared(),
                ],
                attn_dp_rank=4,
            ),
        },
        {
            "name": "stored_omitted_optionals",
            "comment": "medium/cache_salt/session_id omitted by omit_defaults.",
            "batch": batch(
                ts=1.0,
                events=[
                    stored(
                        block_hashes=[7],
                        parent_block_hash=None,
                        token_ids=[11, 12],
                        block_size=2,
                        lora_id=None,
                    )
                ],
                attn_dp_rank=0,
            ),
        },
        {
            "name": "stored_salt_session_and_parent",
            "comment": "All optionals present alongside a signed parent hash.",
            "batch": batch(
                ts=2.5,
                events=[
                    stored(
                        block_hashes=[9],
                        parent_block_hash=-2,
                        token_ids=[5],
                        block_size=4,
                        lora_id=3,
                        medium="CPU_PINNED",
                        cache_salt="salt-a",
                        session_id="session-a",
                    )
                ],
                attn_dp_rank=1,
            ),
        },
        {
            "name": "stored_signed_hash_limits",
            "comment": "INT64_MIN/INT64_MAX hashes must keep their bit pattern.",
            "batch": batch(
                ts=3.0,
                events=[
                    stored(
                        block_hashes=[-(2**63), 2**63 - 1, -1],
                        parent_block_hash=-(2**63),
                        token_ids=[],
                        block_size=1,
                        lora_id=None,
                    )
                ],
                attn_dp_rank=None,
            ),
        },
        {
            "name": "removed_omitted_medium",
            "comment": "BlockRemoved with medium left at its default.",
            "batch": batch(
                ts=4.0,
                events=[removed(block_hashes=[-3])],
                attn_dp_rank=2,
            ),
        },
        {
            "name": "empty_batch",
            "comment": "An empty event list is a valid batch.",
            "batch": batch(ts=5.0, events=[], attn_dp_rank=0),
        },
    ]


# Hash fields span the full signed 64-bit range, so they are recorded as
# decimal strings: JSON numbers above 2^53 do not survive a double round-trip.
# Matches the uint64 convention in the other fixtures.
HASH_FIELDS = frozenset({"block_hashes", "parent_block_hash"})


def describe(event) -> dict:
    """Field values as the encoder saw them, for the C++ expectations.

    Only present fields are listed, so the expectations also record which keys
    ``omit_defaults`` dropped.
    """
    fields = {"type": type(event).__name__}
    for name in type(event).__struct_fields__:
        value = getattr(event, name)
        if value is None and name not in ("parent_block_hash", "lora_id", "token_ids"):
            # Omitted on the wire; leave it out of the expectations too.
            continue
        if name in HASH_FIELDS and value is not None:
            value = (
                [str(item) for item in value] if isinstance(value, list) else str(value)
            )
        fields[name] = value
    return fields


# Token sequences whose block hashes come from upstream's own hash recipe, so
# the ingest-to-query chain is checked against SGLang rather than against
# Conductor's reimplementation of the same hash.
INTEGRATION_BLOCK_SIZE = 4
INTEGRATION_TOKENS = [11, 12, 13, 14, 21, 22, 23, 24, 31, 32, 33, 34, 41, 42, 43, 44]


def upstream_block_hashes(tokens, page_size, hash_oracle):
    """Signed int64 block hashes exactly as the publisher would emit them."""
    from array import array

    hex_digests = hash_oracle.get_hash(
        array("I", tokens), len(tokens), 1, False, None, page_size
    )
    if isinstance(hex_digests, str):
        hex_digests = [hex_digests]
    signed = []
    for digest in hex_digests:
        # Upstream hash_str_to_int64: first 16 hex chars as a signed int64.
        value = int(digest[:16], 16)
        signed.append(value - 2**64 if value >= 2**63 else value)
    return hex_digests, signed


def build_integration(ns, hash_oracle):
    """Per-tier BlockStored payloads plus the query the C++ test must issue.

    Growing native GPU/Host/disk prefixes produce cumulative 4/8/8/12 counts.
    Shared ownership is supplied by Mooncake, not by a synthetic SGLang medium.
    """
    stored = ns["BlockStored"]
    removed = ns["BlockRemoved"]
    cleared = ns["AllBlocksCleared"]
    batch = ns["KVEventBatch"]

    page = INTEGRATION_BLOCK_SIZE
    tokens = INTEGRATION_TOKENS
    hex_digests, signed = upstream_block_hashes(tokens, page, hash_oracle)

    # Tiers an SGLang publisher actually drives in Conductor: an absent or GPU
    # medium is the device cache, CPU_PINNED is the engine's own host cache and
    # DISK its local disk tier. A pure SGLang engine never registers as a shared
    # owner, but the query tiers are nested reachability rather than exclusive
    # ownership, so its own host blocks still count toward cpu_share.
    # Upstream's fourth value, EXTERNAL, is not driven here: the handler's
    # existing medium policy ignores it and this change does not expand media.
    tiers = [("GPU", 1), ("CPU_PINNED", 2), ("DISK", 3)]
    publishes = []
    for medium, block_count in tiers:
        publishes.append(
            {
                "medium": medium,
                "block_count": block_count,
                "payload_hex": msgspec.msgpack.Encoder()
                .encode(
                    batch(
                        ts=100.0 + block_count,
                        events=[
                            stored(
                                block_hashes=signed[:block_count],
                                parent_block_hash=None,
                                token_ids=tokens[: block_count * page],
                                block_size=page,
                                lora_id=None,
                                medium=medium,
                            )
                        ],
                        attn_dp_rank=0,
                    )
                )
                .hex(),
            }
        )

    remove_first = msgspec.msgpack.Encoder().encode(
        batch(
            ts=200.0,
            events=[removed(block_hashes=signed[:1], medium="GPU")],
            attn_dp_rank=0,
        )
    )
    clear_all = msgspec.msgpack.Encoder().encode(
        batch(ts=201.0, events=[cleared()], attn_dp_rank=0)
    )

    return {
        "comment": (
            "Ingest-to-query chain data. block_hashes come from upstream's "
            "own hash extension, not from Conductor, so a query match proves "
            "the two agree."
        ),
        "block_size": page,
        "token_ids": tokens,
        "upstream_hex_digests": hex_digests,
        "block_hashes_signed": [str(v) for v in signed],
        "tier_publishes": publishes,
        "remove_first_block_hex": remove_first.hex(),
        "clear_all_hex": clear_all.hex(),
        "expected_cumulative_tokens": {
            "npu": page * 1,
            "cpu_local": page * 2,
            "disk": page * 3,
            # No shared owner exists here, so the shared tier adds nothing of
            # its own and collapses onto cpu_local. Separation shows up in the
            # owner sets, not in this counter.
            "cpu_share": page * 2,
        },
    }


def load_hash_oracle(sglang_root: Path):
    """Builds and imports upstream's native hash extension, or returns None.

    Requires little-endian Linux, pybind11 and libcrypto. Failure is fatal to
    full-fixture generation; callers must not overwrite a complete fixture.
    """
    import importlib.util
    import sysconfig
    import tempfile

    binding = sglang_root / "python/sglang/srt/mem_cache/cpp_utils" / "hash_binding.cpp"
    if not binding.is_file():
        return None, f"not found: {binding}"
    if sys.byteorder != "little" or not sys.platform.startswith("linux"):
        return None, "upstream hash extension requires little-endian Linux"
    try:
        import pybind11
    except ImportError:
        return None, "pybind11 is required to build the upstream hash oracle"
    out_dir = Path(tempfile.mkdtemp(prefix="sglang_hash_"))
    suffix = sysconfig.get_config_var("EXT_SUFFIX")
    module_path = out_dir / ("hicache_hash_cpp" + suffix)
    cmd = [
        "g++",
        "-O3",
        "-std=c++17",
        "-DNDEBUG",
        "-shared",
        "-fPIC",
        "-DTORCH_EXTENSION_NAME=hicache_hash_cpp",
        "-I" + sysconfig.get_paths()["include"],
        "-I" + pybind11.get_include(),
        str(binding),
        "-lcrypto",
        "-o",
        str(module_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None, "building upstream hash oracle failed: " + result.stderr[-400:]
    spec = importlib.util.spec_from_file_location("hicache_hash_cpp", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sglang-root",
        required=True,
        type=Path,
        help="Path to an SGLang checkout (no drive path is committed).",
    )
    parser.add_argument(
        "--sglang-revision",
        help="Source commit for a verified archive without Git metadata.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).with_name("sglang_map_event_vectors.json"),
    )
    args = parser.parse_args()

    kv_events_path = args.sglang_root / KV_EVENTS_RELATIVE
    if not kv_events_path.is_file():
        sys.exit(f"not found: {kv_events_path}")

    namespace = load_definitions(kv_events_path)
    encoder = msgspec.msgpack.Encoder()

    # Assert the wire shape this fixture and the C++ decoder are built around,
    # so a change upstream fails here instead of in a confusing decoder test.
    batch_config = namespace["EventBatch"].__struct_config__
    event_config = namespace["KVCacheEvent"].__struct_config__
    assert batch_config.array_like, "EventBatch must stay array_like"
    assert not event_config.array_like, "KVCacheEvent must be a tagged map"
    assert event_config.tag is not None, "KVCacheEvent must be tagged"
    assert event_config.omit_defaults, "KVCacheEvent must omit defaults"

    cases = []
    for case in build_cases(namespace):
        payload = encoder.encode(case["batch"])
        cases.append(
            {
                "name": case["name"],
                "comment": case["comment"],
                "payload_hex": payload.hex(),
                "expected": {
                    "ts": case["batch"].ts,
                    "attn_dp_rank": case["batch"].attn_dp_rank,
                    "events": [describe(e) for e in case["batch"].events],
                },
            }
        )

    hash_oracle, oracle_error = load_hash_oracle(args.sglang_root)
    if hash_oracle is None:
        sys.exit("cannot generate complete fixture: " + oracle_error)
    integration = build_integration(namespace, hash_oracle)

    document = {
        "description": (
            "Golden SGLang KV-event payloads encoded by upstream msgspec "
            "definitions. EventBatch is a positional [ts, events, "
            "attn_dp_rank] array; each event is a tagged map with defaulted "
            "fields omitted."
        ),
        "provenance": {
            "sglang_commit": args.sglang_revision or git_commit(args.sglang_root),
            "kv_events_relative_path": KV_EVENTS_RELATIVE.as_posix(),
            "kv_events_sha256": hashlib.sha256(kv_events_path.read_bytes()).hexdigest(),
            "msgspec_version": msgspec.__version__,
            "python_version": sys.version.split()[0],
            "generation_command": (
                "python3 generate_sglang_map_event_vectors.py "
                "--sglang-root <sglang checkout>"
            ),
            "note": (
                "Serialization harness only: this encodes real upstream "
                "struct definitions and is not a running SGLang engine."
            ),
        },
        "cases": cases,
    }
    binding = (
        args.sglang_root / "python/sglang/srt/mem_cache/cpp_utils/hash_binding.cpp"
    )
    document["provenance"]["hash_oracle_sha256"] = hashlib.sha256(
        binding.read_bytes()
    ).hexdigest()
    document["provenance"]["hash_oracle"] = (
        "upstream sglang/srt/mem_cache/cpp_utils/hash_binding.cpp built "
        "locally; block hashes are upstream values, not Conductor's"
    )
    document["integration"] = integration

    args.output.write_text(
        json.dumps(document, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    print(f"wrote {args.output} ({len(cases)} cases)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
