#!/usr/bin/env python3
"""RL data-plane load driver for capturing Mooncake Store allocation traces.

Each process plays one data-parallel rank of an RL trainer and reproduces the
per-step data flow of a GRPO/PPO pipeline:

1. rollout   : ``input_ids``/``attention_mask``/``position_ids``/``responses``/
               ``response_mask`` plus non-tensor ``uid``/``data_source``/
               ``reward_model`` rows for a variable-size batch.
2. old_log_prob, ref_log_prob, reward, advantage stages: appended as separate
               objects, mirroring the actor/ref/critic workers.
3. optional  : one large whole object per step (``--big_field_mib``), like a
               merged blob written with a single put.
4. trainer   : reads micro-batches back.
5. cleanup   : steps older than ``--keep_steps`` are removed explicitly, unless
               ``--no_cleanup`` leaves reclamation to master eviction.

Two write paths are available (``--api``): ``dataproto`` uses the structured
object API (``MooncakeBundleTransfer``, chunked, ~30 keys per batch);
``put_parts`` stores every field as one whole object with ``put_parts``, reads
with ``get_buffer`` and frees with a forced ``remove``, for data planes that
use only those calls.

Object sizes and lifetimes are exactly what the master sees for this batch
geometry; only the tensor contents are random. Run ``mooncake_master --v=1``
and extract the trace with ``mooncake-store/benchmarks/extract_alloc_trace.py``.

Example (8 ranks, 16 x 4 GiB segments, stop after 3x capacity written):

    python rl_dataproto_trace_driver.py --ranks 8 --extra_segments 8 \
        --segment_gib 4 --target_multiple 3
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import random
import sys
import time
from dataclasses import dataclass

import numpy as np
from mooncake.store import MooncakeDistributedStore
from mooncake.structured_object_store import (
    BundleTransferPolicy,
    MooncakeBundleTransfer,
)

GiB = 1024**3
MiB = 1024**2

# Stages appended after the rollout, with the float fields each one carries.
APPEND_STAGES = (
    ("old_log_prob", ("old_log_probs", "rollout_log_probs")),
    ("ref_log_prob", ("ref_log_prob",)),
    ("reward", ("token_level_scores", "token_level_rewards")),
    ("advantage", ("advantages", "returns")),
)
# Fields the trainer reads back for each micro-batch.
READ_FIELDS = ("input_ids", "responses", "old_log_probs", "advantages", "uid")
# Errors the Mooncake client raises for failed puts/gets/removes; anything else
# is a bug in this script and should propagate.
STORE_ERRORS = (RuntimeError, ValueError, OSError)


@dataclass
class StepGeometry:
    rows: int
    prompt_len: int
    response_len: int


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument(
        "--ranks", type=int, default=8, help="data-parallel ranks (client processes)"
    )
    p.add_argument(
        "--extra_segments",
        type=int,
        default=0,
        help="storage-only clients that mount a segment but write nothing",
    )
    p.add_argument(
        "--segment_gib", type=float, default=4.0, help="segment size per client, GiB"
    )
    p.add_argument(
        "--local_buffer_mib",
        type=int,
        default=1024,
        help="registered local buffer per client",
    )
    p.add_argument(
        "--steps", type=int, default=0, help="max steps (0 = until --target_multiple)"
    )
    p.add_argument(
        "--target_multiple",
        type=float,
        default=3.0,
        help="stop when cumulative bytes written reach this multiple of pool capacity",
    )
    p.add_argument(
        "--rows_min", type=int, default=128, help="min rows per rank per step"
    )
    p.add_argument(
        "--rows_max", type=int, default=512, help="max rows per rank per step"
    )
    p.add_argument(
        "--prompt_len", type=int, default=2048, help="max prompt length (tokens)"
    )
    p.add_argument(
        "--response_len", type=int, default=8192, help="max response length (tokens)"
    )
    p.add_argument(
        "--min_len_frac",
        type=float,
        default=0.25,
        help="per-step padded length is uniform in [frac, 1] x max length",
    )
    p.add_argument(
        "--keep_steps", type=int, default=2, help="steps to keep alive before cleanup"
    )
    p.add_argument(
        "--no_cleanup",
        action="store_true",
        help="never remove; rely on master eviction",
    )
    p.add_argument(
        "--chunk_mib",
        type=int,
        default=64,
        help="dataproto: structured object chunk size; large values give whole-field objects",
    )
    p.add_argument(
        "--micro_batches", type=int, default=4, help="trainer reads per step per rank"
    )
    p.add_argument(
        "--big_field_mib",
        type=int,
        default=0,
        help="also write one whole object of this many MiB per rank per step; 0 = off",
    )
    p.add_argument(
        "--big_field_jitter",
        type=float,
        default=0.0,
        help="per-step size jitter for the big object: uniform in [1-j, 1+j] x MiB",
    )
    p.add_argument(
        "--api",
        choices=["dataproto", "put_parts"],
        default="dataproto",
        help="write path: structured objects, or one put_parts object per field",
    )
    p.add_argument(
        "--parts_per_object",
        type=int,
        default=4,
        help="put_parts: split each field into this many byte parts",
    )
    p.add_argument("--master", default="127.0.0.1:50051")
    p.add_argument("--metadata", default="P2PHANDSHAKE")
    p.add_argument("--protocol", default="tcp")
    p.add_argument("--device", default="")
    p.add_argument("--base_port", type=int, default=17100)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--stats_file", default="", help="write per-rank byte counters here")
    return p.parse_args(argv)


def make_store(args, rank: int, segment_bytes: int) -> MooncakeDistributedStore:
    store = MooncakeDistributedStore()
    rc = store.setup(
        f"127.0.0.1:{args.base_port + rank}",
        args.metadata,
        int(segment_bytes),
        int(args.local_buffer_mib) * MiB,
        args.protocol,
        args.device,
        args.master,
    )
    if rc != 0:
        raise RuntimeError(f"rank {rank}: store.setup failed rc={rc}")
    return store


# ----------------------------------------------------------------- workload --


def step_geometry(rng: random.Random, args) -> StepGeometry:
    frac = rng.uniform(args.min_len_frac, 1.0)
    return StepGeometry(
        rows=rng.randint(args.rows_min, args.rows_max),
        prompt_len=max(16, int(args.prompt_len * frac)),
        response_len=max(16, int(args.response_len * frac)),
    )


def rollout_batch(g: StepGeometry, step: int, rank: int, rng: np.random.Generator) -> dict:
    total = g.prompt_len + g.response_len
    ids = rng.integers(0, 150000, size=(g.rows, total), dtype=np.int64)
    return {
        "batch": {
            "input_ids": ids,
            "attention_mask": np.ones((g.rows, total), dtype=np.int64),
            "position_ids": np.tile(np.arange(total, dtype=np.int64), (g.rows, 1)),
            "responses": ids[:, g.prompt_len :].copy(),
            "response_mask": np.ones((g.rows, g.response_len), dtype=np.int64),
        },
        "non_tensor_batch": {
            "uid": np.array(
                [f"s{step}-r{rank}-{i}" for i in range(g.rows)], dtype=object
            ),
            "data_source": np.array(["math"] * g.rows, dtype=object),
            "reward_model": np.array(
                [{"style": "rule", "ground_truth": str(i)} for i in range(g.rows)],
                dtype=object,
            ),
        },
        "meta_info": {"step": step, "rank": rank, "global_token_num": [total] * g.rows},
    }


def float_stage(g: StepGeometry, rng: np.random.Generator, *names: str) -> dict:
    return {
        "batch": {
            n: rng.standard_normal((g.rows, g.response_len), dtype=np.float32)
            for n in names
        }
    }


def big_blob(args, rng: random.Random) -> dict:
    """One float32 array of --big_field_mib MiB (with jitter), batch size 1."""
    mib = int(
        args.big_field_mib
        * rng.uniform(1.0 - args.big_field_jitter, 1.0 + args.big_field_jitter)
    )
    return {
        "batch": {
            "rollout_blob": np.empty((1, max(1, mib) * MiB // 4), dtype=np.float32)
        }
    }


def tensor_bytes(data: dict) -> int:
    return sum(a.nbytes for a in data["batch"].values())


# ------------------------------------------------------------- write paths --


class DataProtoStep:
    """One RL step stored through the structured object API."""

    def __init__(
        self, transfer: MooncakeBundleTransfer, rank: int, step: int, args
    ) -> None:
        self.transfer = transfer
        self.namespace = f"rank{rank}"
        self.partition = f"step-{step}"
        self.policy = BundleTransferPolicy(copy_mode="copy")
        self.blob_chunk = max(2 * args.big_field_mib, args.chunk_mib) * MiB
        self.ref = None
        self.blob_ref = None

    def put_stage(self, stage: str, data: dict) -> None:
        if self.ref is None:
            self.ref = self.transfer.put_dataproto(
                data,
                namespace=self.namespace,
                partition=self.partition,
                stage=stage,
                policy=self.policy,
            )
        else:
            self.ref = self.transfer.append_dataproto_fields(
                self.ref, data, stage=stage, policy=self.policy
            )

    def put_blob(self, blob: dict) -> None:
        # Separate DataProto (batch size 1) so the blob stays one whole object.
        self.blob_ref = self.transfer.put_dataproto(
            blob,
            namespace=self.namespace,
            partition=f"{self.partition}-blob",
            stage="blob",
            policy=self.policy,
            chunk_bytes=self.blob_chunk,
        )

    def read(self, rows: slice) -> None:
        self.transfer.get_dataproto(self.ref, fields=list(READ_FIELDS), rows=rows)

    def cleanup(self) -> None:
        for ref in (self.ref, self.blob_ref):
            if ref is not None:
                self.transfer.cleanup_dataproto(ref)
        self.ref = self.blob_ref = None


class PutPartsStep:
    """One RL step stored as whole objects: one key per field via put_parts."""

    def __init__(
        self, store: MooncakeDistributedStore, rank: int, step: int, args
    ) -> None:
        self.store = store
        self.prefix = f"rl/rank{rank}/step-{step}"
        self.parts = max(1, args.parts_per_object)
        self.keys: list[str] = []

    def _put(self, key: str, parts: list) -> None:
        rc = self.store.put_parts(key, *parts)
        if rc != 0:
            raise RuntimeError(f"put_parts failed for {key}: {rc}")
        self.keys.append(key)

    def put_stage(self, stage: str, data: dict) -> None:
        for name, array in data["batch"].items():
            flat = np.ascontiguousarray(array).view(np.uint8).reshape(-1)
            parts = np.array_split(flat, min(self.parts, max(1, flat.size)))
            self._put(f"{self.prefix}/{stage}/{name}", parts)
        for name, values in data.get("non_tensor_batch", {}).items():
            self._put(
                f"{self.prefix}/{stage}/{name}", ["\n".join(map(str, values)).encode()]
            )

    def put_blob(self, blob: dict) -> None:
        self.put_stage("blob", blob)

    def read(self, rows: slice) -> None:
        # get_buffer returns whole objects; row selection happens client-side.
        for key in self.keys:
            if (
                key.rsplit("/", 1)[1] in READ_FIELDS
                and self.store.get_buffer(key) is None
            ):
                raise RuntimeError(f"get_buffer miss for {key}")

    def cleanup(self) -> None:
        # force=True: objects read moments ago still carry a lease, exactly
        # like the structured store's cleanup path.
        for key in self.keys:
            self.store.remove(key, True)
        self.keys.clear()


# --------------------------------------------------------------- processes --


def run_rank(rank: int, args, capacity_bytes: int, barrier, counters, stop_flag):
    rng = random.Random(args.seed * 1000 + rank)
    np_rng = np.random.default_rng(args.seed * 1000 + rank)
    store = make_store(args, rank, args.segment_gib * GiB)
    transfer = MooncakeBundleTransfer(
        store, key_prefix="rl", default_chunk_bytes=max(1, args.chunk_mib) * MiB
    )

    def new_step(step: int):
        if args.api == "put_parts":
            return PutPartsStep(store, rank, step, args)
        return DataProtoStep(transfer, rank, step, args)

    def quiet_cleanup(obj) -> None:
        try:
            obj.cleanup()
        except STORE_ERRORS as exc:
            print(f"[rank {rank}] cleanup failed: {str(exc)[:120]}", file=sys.stderr)

    live: list[tuple[int, object]] = []  # (step, step object), oldest first
    written = failures = step = 0
    target = args.target_multiple * capacity_bytes
    try:
        while not stop_flag.value and not (args.steps and step >= args.steps):
            g = step_geometry(rng, args)
            current = new_step(step)
            try:
                data = rollout_batch(g, step, rank, np_rng)
                written += tensor_bytes(data)
                current.put_stage("rollout", data)
                for stage, names in APPEND_STAGES:
                    stage_data = float_stage(g, np_rng, *names)
                    written += tensor_bytes(stage_data)
                    current.put_stage(stage, stage_data)
                if args.big_field_mib > 0:
                    blob = big_blob(args, rng)
                    written += tensor_bytes(blob)
                    current.put_blob(blob)
                mb = max(1, g.rows // args.micro_batches)
                for lo in range(0, g.rows, mb):
                    current.read(slice(lo, min(g.rows, lo + mb)))
                live.append((step, current))
            except STORE_ERRORS as exc:  # an allocation failure surfaces here
                failures += 1
                quiet_cleanup(current)
                if failures <= 5 or failures % 100 == 0:
                    print(
                        f"[rank {rank}] step {step} failed: {type(exc).__name__}: {str(exc)[:160]}",
                        file=sys.stderr,
                        flush=True,
                    )
            if not args.no_cleanup:
                while live and live[0][0] <= step - args.keep_steps:
                    quiet_cleanup(live.pop(0)[1])
            counters[rank] = written
            step += 1
            barrier.wait()
            if rank == 0:
                total = sum(counters)
                if step % 5 == 0 or total >= target:
                    print(
                        f"[step {step}] rows={g.rows} prompt={g.prompt_len} resp={g.response_len} "
                        f"written={total / GiB:.1f} GiB ({total / capacity_bytes:.2f}x capacity) "
                        f"live_steps={len(live)} failures={failures}",
                        flush=True,
                    )
                if total >= target:
                    stop_flag.value = 1
            barrier.wait()
    finally:
        if not args.no_cleanup:
            for _, obj in live:
                quiet_cleanup(obj)
        store.close()
    print(
        f"[rank {rank}] done: steps={step} written={written / GiB:.2f} GiB failures={failures}",
        flush=True,
    )


def run_storage_only(rank: int, args, stop_flag):
    store = make_store(args, rank, args.segment_gib * GiB)
    while not stop_flag.value:
        time.sleep(0.5)
    store.close()


def main(argv=None):
    args = parse_args(argv)
    num_segments = args.ranks + args.extra_segments
    capacity = int(num_segments * args.segment_gib * GiB)
    print(
        f"pool: {num_segments} segments x {args.segment_gib} GiB = {capacity / GiB:.0f} GiB, "
        f"target {args.target_multiple}x = {args.target_multiple * capacity / GiB:.0f} GiB written",
        flush=True,
    )
    ctx = mp.get_context("spawn")
    barrier = ctx.Barrier(args.ranks)
    counters = ctx.Array("q", args.ranks)
    stop_flag = ctx.Value("i", 0)
    procs = [
        ctx.Process(target=run_storage_only, args=(args.ranks + i, args, stop_flag))
        for i in range(args.extra_segments)
    ]
    procs += [
        ctx.Process(
            target=run_rank, args=(rank, args, capacity, barrier, counters, stop_flag)
        )
        for rank in range(args.ranks)
    ]
    for p in procs:
        p.start()
        time.sleep(0.2)
    try:
        for p in procs[args.extra_segments :]:
            p.join()
    finally:
        stop_flag.value = 1
        for p in procs:
            p.join(timeout=30)
            if p.is_alive():
                p.terminate()
    if args.stats_file:
        with open(args.stats_file, "w") as handle:
            handle.write("\n".join(str(c) for c in counters) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
