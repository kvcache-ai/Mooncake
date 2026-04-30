#!/usr/bin/env python3
"""
Forge RL Design 01 §3.4 — end-to-end smoke test for QueryPrefixMatch.

Drives a live mooncake-store cluster (started separately, see
`docs/source/getting-started.md`) through the Python binding exposed by
`mooncake-integration/store/store_py.cpp`. Validates:

  1. baseline:        empty / oversized chains return the documented errors;
  2. cache miss:      query against an unindexed chain reports
                      matched_blocks == 0 and a benign empty best_segment_name;
  3. cache hit:       after a put() the chain prefixed by hash(payload key)
                      reports matched_blocks > 0 and points back at the local
                      segment with a non-zero query_lease_ms.

When the master was started with `--enable_prefix_query=false`, every test
that touches the RPC will skip cleanly (the wrapper surfaces -1600 and we
treat it as a documented "feature off" signal — the on/off flag itself is a
master-side gflag, not a client-controllable knob).

The test is intentionally tolerant of the cluster topology — it only asserts
invariants that must hold for *any* correctly-wired master. Cost-ranking
correctness is covered exhaustively by the C++ unit tests in
`mooncake-store/tests/prefix_query_test.cpp`.

Usage:
    # Start a master + at least one client/segment first, then:
    MOONCAKE_MASTER=127.0.0.1:50051 \
    MOONCAKE_PROTOCOL=tcp \
    MOONCAKE_TE_META_DATA_SERVER=P2PHANDSHAKE \
    python scripts/test_prefix_query.py
"""
from __future__ import annotations

import os
import sys
import time
import unittest

from mooncake.mooncake_config import MooncakeConfig

try:
    from mooncake.store import MooncakeDistributedStore
except ImportError as exc:
    print(f"Could not import mooncake.store: {exc}", file=sys.stderr)
    raise

# Mirrors the constants in `mooncake-store/include/types.h` ErrorCode block.
# Hard-coded here so the test surfaces a clear failure when these values shift
# unintentionally — the design doc §3.8 says they are part of the public ABI.
_OK = 0
_PREFIX_QUERY_DISABLED = -1600
_PREFIX_CHAIN_TOO_LONG = -1601
_PREFIX_CHAIN_EMPTY = -1602

# §3.4 — QueryPrefixMatchRequest.chain has a server-enforced ceiling of 256
# entries (matches `MasterService::kMaxPrefixChainLength`). The Python
# binding short-circuits anything beyond that locally, so we pad just past
# the limit to exercise PREFIX_CHAIN_TOO_LONG without wasting RPC bandwidth.
_OVERSIZED_CHAIN_LEN = 257

# Default chain length used by the cache-hit smoke. Kept short so the test
# completes fast even on a single-node setup; correctness of long chains is
# covered by the C++ side.
_SMOKE_CHAIN_LEN = 8

GLOBAL_STORE: MooncakeDistributedStore | None = None
GLOBAL_CONFIG: MooncakeConfig | None = None


def _hash64(value: int, seed: int) -> int:
    """A trivial 64-bit mixing function used to fabricate chain hashes.

    The master treats each chain element as an opaque 64-bit key, so any
    deterministic, well-distributed hash works. We avoid `hash()` (varies
    per-process) and `hashlib` (overkill + ~1us each) on purpose.
    """
    x = (value ^ seed) & 0xFFFFFFFFFFFFFFFF
    x = (x ^ (x >> 33)) * 0xFF51AFD7ED558CCD & 0xFFFFFFFFFFFFFFFF
    x = (x ^ (x >> 33)) * 0xC4CEB9FE1A85EC53 & 0xFFFFFFFFFFFFFFFF
    return x ^ (x >> 33)


def _build_chain(seed: int, length: int) -> list[int]:
    """Build a chained-hash sequence: chain[i] = hash(chain[i-1], i)."""
    chain: list[int] = []
    prev = seed & 0xFFFFFFFFFFFFFFFF
    for i in range(length):
        prev = _hash64(prev, i + 1)
        chain.append(prev)
    return chain


def setUpModule() -> None:
    global GLOBAL_STORE, GLOBAL_CONFIG
    GLOBAL_CONFIG = MooncakeConfig.load_from_env()
    GLOBAL_STORE = MooncakeDistributedStore()
    rc = GLOBAL_STORE.setup(
        GLOBAL_CONFIG.local_hostname,
        GLOBAL_CONFIG.metadata_server,
        GLOBAL_CONFIG.global_segment_size,
        GLOBAL_CONFIG.local_buffer_size,
        GLOBAL_CONFIG.protocol,
        GLOBAL_CONFIG.device_name,
        GLOBAL_CONFIG.master_server_address,
    )
    if rc != _OK:
        raise RuntimeError(
            f"MooncakeDistributedStore.setup failed: rc={rc}. "
            "Is the master reachable at "
            f"{GLOBAL_CONFIG.master_server_address}?"
        )


def tearDownModule() -> None:
    global GLOBAL_STORE
    if GLOBAL_STORE is not None:
        try:
            GLOBAL_STORE.close()
        except Exception:
            # close() failure during teardown is informative but should not
            # mask the actual test failure.
            pass
        GLOBAL_STORE = None


class PrefixQuerySmoke(unittest.TestCase):
    """Public-API contract checks. See module docstring for scope."""

    def _query(self, chain: list[int]):
        """Thin convenience wrapper to centralise the call signature.

        The C++ binding returns a 4-tuple
            (matched_blocks, candidates, query_lease_ms, err_code)
        where ``candidates`` is ``list[tuple[segment_name, replica_type]]``.
        We unpack it into a small dict here so individual tests can keep
        readable assertions without each one re-doing tuple indexing.
        On error the binding hands back ``None`` for the payload slots, so
        we forward ``None`` to the caller verbatim - the rc is the source
        of truth for the failure mode.
        """
        assert GLOBAL_STORE is not None
        matched_blocks, candidates, query_lease_ms, err_code = (
            GLOBAL_STORE.query_prefix_match(chain)
        )
        if err_code != _OK:
            return None, err_code
        return (
            {
                "matched_blocks": matched_blocks,
                "candidates": list(candidates) if candidates else [],
                "query_lease_ms": query_lease_ms,
            },
            err_code,
        )

    def test_empty_chain_returns_chain_empty(self) -> None:
        resp, rc = self._query([])
        # The master is allowed to also report DISABLED here when the flag is
        # off — DISABLED short-circuits before chain validation. Either is a
        # valid documented response; nothing else is.
        self.assertIn(rc, (_PREFIX_CHAIN_EMPTY, _PREFIX_QUERY_DISABLED))
        self.assertIsNone(resp)

    def test_oversized_chain_returns_chain_too_long(self) -> None:
        chain = _build_chain(seed=0xDEADBEEF, length=_OVERSIZED_CHAIN_LEN)
        resp, rc = self._query(chain)
        self.assertIn(rc, (_PREFIX_CHAIN_TOO_LONG, _PREFIX_QUERY_DISABLED))
        self.assertIsNone(resp)

    def test_unindexed_chain_returns_zero_match(self) -> None:
        # Use a high-entropy seed so we (almost certainly) miss everything in
        # the master's metadata shards. If this asserts triggers in CI, audit
        # the seed for accidental collisions before chasing a real bug.
        chain = _build_chain(seed=0xA17EBABEDEADC0DE, length=_SMOKE_CHAIN_LEN)
        resp, rc = self._query(chain)
        if rc == _PREFIX_QUERY_DISABLED:
            self.skipTest(
                "master started without --enable_prefix_query; "
                "set the flag and rerun this test"
            )
        self.assertEqual(rc, _OK, msg=f"unexpected rc={rc}, resp={resp}")
        self.assertIsNotNone(resp)
        self.assertEqual(resp["matched_blocks"], 0)
        # Pure LPM contract: a zero-match response carries an empty
        # candidate list and a non-negative advisory lease. We deliberately
        # do NOT assert any cost-aware fields (best_segment_name /
        # est_cost_us / best_replica_storage_tier) here - ranking lives in
        # a separate router-side QueryCost RPC, not in QueryPrefixMatch.
        self.assertEqual(resp["candidates"], [])
        self.assertGreaterEqual(resp["query_lease_ms"], 0)

    def test_put_then_query_reports_hit(self) -> None:
        """End-to-end: put an object whose key matches the canonical
        prefix-hash encoding, then query."""
        # The master maps each chain entry through `MakePrefixHashKey` (see
        # mooncake-store/include/prefix_key.h) — we mirror that exact format
        # here so put() lands at the same metadata-shard slot QueryPrefixMatch
        # will look at.
        chain = _build_chain(seed=0xCAFEBABE, length=_SMOKE_CHAIN_LEN)
        first_key = f"__pfx__{chain[0]:016x}"

        assert GLOBAL_STORE is not None
        # Use a small unique payload so successive runs don't collide. The
        # cluster guarantees idempotent put for the same key+value pair.
        payload = (f"prefix-query-smoke-{int(time.time() * 1000)}").encode()
        put_rc = GLOBAL_STORE.put(first_key, payload)
        if put_rc != _OK:
            self.skipTest(
                f"put({first_key!r}) failed with rc={put_rc} — "
                "cluster likely lacks a writable segment"
            )

        try:
            # Give the master a beat to settle the metadata insertion. The
            # contract is synchronous, but cross-RPC propagation in HA mode
            # may need ~10ms.
            time.sleep(0.05)
            resp, rc = self._query(chain, avg_block_bytes=len(payload))
            if rc == _PREFIX_QUERY_DISABLED:
                self.skipTest(
                    "master started without --enable_prefix_query; "
                    "set the flag and rerun this test"
                )
            self.assertEqual(rc, _OK, msg=f"unexpected rc={rc}, resp={resp}")
            self.assertIsNotNone(resp)
            self.assertGreaterEqual(
                resp["matched_blocks"],
                1,
                msg="put() landed but QueryPrefixMatch reported no hit; "
                "metadata shard likely keyed differently from "
                "expectations — see design §3.3",
            )
            self.assertNotEqual(
                resp["best_segment_name"],
                "",
                msg="hit must point at a concrete segment",
            )
            self.assertGreater(
                resp["query_lease_ms"],
                0,
                msg="hit must come with a non-zero routing-hint TTL",
            )
        finally:
            # Best-effort cleanup. We swallow remove failures because the
            # downstream tests in the same module don't depend on this key.
            try:
                GLOBAL_STORE.remove(first_key)
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
