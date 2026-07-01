"""Proxy-aware KV/Hidden eviction benchmark for Mooncake Store.

This benchmark keeps one shared multimodal workload and replays it with
different proxy access rules. It is intentionally object-centric: the core
unit is a KV or Hidden store object, not an end-to-end inference request.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import math
import random
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


MiB = 1024 * 1024


@dataclass(frozen=True)
class ObjectSpec:
    key: str
    group: str
    size: int
    data_type: str
    modality: str
    recompute_cost_ms: float
    video_frames: int = 0
    with_soft_pin: bool = False


@dataclass(frozen=True)
class ImageSpec:
    image_id: str
    modality: str
    size_bucket: str
    hidden_key: str
    hidden_size: int
    public: bool
    recompute_cost_ms: float
    video_frames: int = 0


@dataclass(frozen=True)
class SessionSpec:
    session_id: str
    kind: str
    prompt_bucket: str
    rounds: int
    images: tuple[ImageSpec, ...]
    kv_keys: tuple[str, ...]
    round_kv_block_counts: tuple[int, ...]


@dataclass(frozen=True)
class SharedWorkload:
    sessions: tuple[SessionSpec, ...]
    kv_objects: tuple[ObjectSpec, ...]
    hidden_objects: tuple[ObjectSpec, ...]
    pressure_objects: tuple[ObjectSpec, ...]
    fingerprint: str


@dataclass(frozen=True)
class AccessPlan:
    proxy_mode: str
    dataset_fingerprint: str
    kv_get_keys: tuple[str, ...]
    hidden_get_keys: tuple[str, ...]
    get_events: tuple[tuple[str, str], ...]

    @property
    def kv_get_objects(self) -> int:
        return len(self.kv_get_keys)

    @property
    def hidden_get_objects(self) -> int:
        return len(self.hidden_get_keys)


@dataclass(frozen=True)
class RequestEvent:
    timestamp_ms: float
    session_id: str
    round_id: int
    hidden_keys: tuple[str, ...]
    kv_keys: tuple[str, ...]
    is_rebuild: bool = False

    @property
    def get_events(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            [("hidden", key) for key in self.hidden_keys]
            + [("kv", key) for key in self.kv_keys]
        )


@dataclass(frozen=True)
class RequestReplayPlan:
    proxy_mode: str
    dataset_fingerprint: str
    requests: tuple[RequestEvent, ...]

    @property
    def get_events(self) -> tuple[tuple[str, str], ...]:
        return tuple(event for request in self.requests for event in request.get_events)

    @property
    def kv_get_keys(self) -> tuple[str, ...]:
        return tuple(key for request in self.requests for key in request.kv_keys)

    @property
    def hidden_get_keys(self) -> tuple[str, ...]:
        return tuple(key for request in self.requests for key in request.hidden_keys)

    @property
    def kv_get_objects(self) -> int:
        return len(self.kv_get_keys)

    @property
    def hidden_get_objects(self) -> int:
        return len(self.hidden_get_keys)


@dataclass(frozen=True)
class MixedEvent:
    operation: str
    object_type: str
    key: str


@dataclass(frozen=True)
class MixedWorkloadPlan:
    write_workload: SharedWorkload
    write_objects: tuple[ObjectSpec, ...]
    events: tuple[MixedEvent, ...]


@dataclass(frozen=True)
class StrategyCase:
    name: str
    master_flags: str
    bench_flags: str = ""


@dataclass
class PhaseStats:
    name: str
    latencies: list[float] = field(default_factory=list)
    objects: int = 0
    successes: int = 0
    failures: int = 0
    misses: int = 0
    verify_failures: int = 0
    bytes_processed: int = 0
    recompute_ms_saved: float = 0.0
    error_counts: Counter = field(default_factory=Counter)
    start_time: float = 0.0
    end_time: float = 0.0

    @property
    def duration_sec(self) -> float:
        return max(0.0, self.end_time - self.start_time)


@dataclass(frozen=True)
class ProbeGroupStats:
    total_objects: int
    hit_objects: int
    total_bytes: int
    hit_bytes: int
    total_recompute_ms: float = 0.0
    hit_recompute_ms: float = 0.0

    @property
    def miss_objects(self) -> int:
        return max(0, self.total_objects - self.hit_objects)

    @property
    def miss_bytes(self) -> int:
        return max(0, self.total_bytes - self.hit_bytes)

    @property
    def miss_recompute_ms(self) -> float:
        return max(0.0, self.total_recompute_ms - self.hit_recompute_ms)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Proxy-aware Hidden/KV eviction benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--proxy-mode", choices=["full_history", "session"], required=True)
    parser.add_argument("--key-prefix", default="proxyhiddenkv")
    parser.add_argument("--rand-seed", type=int, default=1)
    parser.add_argument("--sessions", type=int, default=200)
    parser.add_argument("--scale", type=float, default=0.001)
    parser.add_argument("--hidden-size-multiplier", type=float, default=1.0)
    parser.add_argument("--min-object-size", type=int, default=512)
    parser.add_argument("--private-image-ratio", type=float, default=0.8)
    parser.add_argument("--private-video-ratio", type=float, default=0.8)
    parser.add_argument("--session-hidden-edge-rate", type=float, default=0.17)
    parser.add_argument(
        "--kv-prefix-share-ratio",
        type=float,
        default=0.30,
        help="Target fraction of each session KV chain backed by cross-session prefix-family blocks.",
    )
    parser.add_argument(
        "--kv-prefix-family-count",
        type=int,
        default=8,
        help="Number of shared system/template KV prefix families.",
    )
    parser.add_argument(
        "--public-visual-kv-share-rate",
        type=float,
        default=0.50,
        help="Probability that a public visual session uses a visual-specific shared KV prefix family.",
    )
    parser.add_argument("--public-cross-session-hit-rate", type=float, default=0.762)
    parser.add_argument("--private-cross-session-hit-rate", type=float, default=0.037)
    parser.add_argument("--pressure-multiplier", type=float, default=0.5)
    parser.add_argument(
        "--execution-mode",
        choices=["phase", "mixed", "request", "request_eviction"],
        default="phase",
    )
    parser.add_argument("--kv-block-tokens", type=int, default=16)
    parser.add_argument("--session-arrival-mean-ms", type=float, default=50.0)
    parser.add_argument("--round-gap-median-ms", type=float, default=500.0)
    parser.add_argument("--round-gap-sigma", type=float, default=0.7)
    parser.add_argument("--session-rebuild-rate", type=float, default=0.10)
    parser.add_argument(
        "--request-timing-scale",
        type=float,
        default=1.0,
        help="Scale factor applied to request replay sleeps. Use 0 for a snapshot-style replay.",
    )
    parser.add_argument("--mixed-events", type=int, default=1000)
    parser.add_argument("--mixed-write-sessions", type=int, default=200)
    parser.add_argument("--mixed-event-gap-ms", type=float, default=0.0)
    parser.add_argument("--rwmixread", type=int, default=70)
    parser.add_argument("--cooldown-sec", type=float, default=2.0)
    parser.add_argument("--post-pressure-sec", type=float, default=1.0)
    parser.add_argument("--soft-pin-video-hidden", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-json", default="")

    parser.add_argument("--local-hostname", default="127.0.0.1:50071")
    parser.add_argument("--metadata-server", default="http://127.0.0.1:8080/metadata")
    parser.add_argument("--master-server", default="127.0.0.1:50051")
    parser.add_argument("--protocol", default="tcp")
    parser.add_argument("--device-name", default="")
    parser.add_argument("--global-segment-size", type=int, default=64 * MiB)
    parser.add_argument("--local-buffer-size", type=int, default=32 * MiB)
    parser.add_argument("--memory-replica-num", type=int, default=1)
    parser.add_argument("--nof-replica-num", type=int, default=0)
    return parser


def align_size(size: float, min_size: int) -> int:
    value = max(min_size, int(math.ceil(size / 512.0) * 512))
    return value


def weighted_choice(rng: random.Random, items: list[tuple[str, float]]) -> str:
    total = sum(weight for _, weight in items)
    pick = rng.random() * total
    acc = 0.0
    for value, weight in items:
        acc += weight
        if pick <= acc:
            return value
    return items[-1][0]


def scaled_mib(mib: float, args: argparse.Namespace) -> int:
    return align_size(mib * MiB * args.scale, args.min_object_size)


def hidden_mib_for_bucket(bucket: str) -> float:
    return {
        "small": 2.2,
        "standard": 4.2,
        "large": 9.9,
        "xlarge": 13.5,
    }[bucket]


def hidden_size_for_bucket(bucket: str, args: argparse.Namespace) -> int:
    base_mib = hidden_mib_for_bucket(bucket)
    return align_size(
        base_mib * MiB * args.scale * args.hidden_size_multiplier,
        args.min_object_size,
    )


def image_recompute_cost_ms(bucket: str) -> float:
    return {
        "small": 80.0,
        "standard": 175.0,
        "large": 410.0,
        "xlarge": 640.0,
    }[bucket]


def video_recompute_cost_ms(bucket: str, frames: int) -> float:
    # Qwen-VL style video Hidden is produced by per-frame visual encoding
    # followed by temporal fusion; final Store bytes can be similar to one image.
    return image_recompute_cost_ms(bucket) * frames + 20.0 * frames


def kv_recompute_cost_ms(prompt_bucket: str, rounds: int) -> float:
    tokens = {
        "tiny": 128,
        "short": 512,
        "long": 2048,
        "xlong": 8192,
    }[prompt_bucket]
    return tokens * (1.0 + max(0, rounds - 1) * 0.18) * 0.08


def kv_size_for_prompt(prompt_bucket: str, rounds: int, args: argparse.Namespace) -> int:
    base_mib = {
        "tiny": 41.0,
        "short": 96.0,
        "long": 247.0,
        "xlong": 896.0,
    }[prompt_bucket]
    round_factor = 1.0 + max(0, rounds - 1) * 0.18
    return scaled_mib(base_mib * round_factor, args)


def kv_mib_per_token() -> float:
    # Qwen-VL 7B bf16: 2(K/V) * 28 layers * 4 kv heads * 128 head dim * 2B.
    return 57344 / MiB


def text_tokens_for_prompt_bucket(prompt_bucket: str, *, followup: bool) -> int:
    if followup:
        return {
            "tiny": 50,
            "short": 100,
            "long": 256,
            "xlong": 512,
        }[prompt_bucket]
    return {
        "tiny": 50,
        "short": 128,
        "long": 384,
        "xlong": 1024,
    }[prompt_bucket]


def kv_block_size(args: argparse.Namespace) -> int:
    # Qwen-VL 7B scale estimate:
    # layers(28) * K/V(2) * kv_heads(4) * head_dim(128) * fp16(2B) * block_tokens(16)
    # = 917,504 bytes ~= 0.875 MiB before benchmark scaling.
    base_mib = 0.875 * (args.kv_block_tokens / 16.0)
    return scaled_mib(base_mib, args)


def kv_block_count_for_prompt(prompt_bucket: str, rounds: int, args: argparse.Namespace) -> int:
    return max(1, math.ceil(kv_size_for_prompt(prompt_bucket, rounds, args) / kv_block_size(args)))


def kv_round_block_counts_for_session(
    *,
    kind: str,
    prompt_bucket: str,
    rounds: int,
    image_buckets: tuple[str, ...],
    args: argparse.Namespace,
) -> tuple[int, ...]:
    del kind
    block_size = kv_block_size(args)
    visual_kv_mib = sum(hidden_mib_for_bucket(bucket) * 8.0 for bucket in image_buckets)
    first_text_mib = text_tokens_for_prompt_bucket(
        prompt_bucket,
        followup=False,
    ) * kv_mib_per_token()
    followup_mib = text_tokens_for_prompt_bucket(
        prompt_bucket,
        followup=True,
    ) * kv_mib_per_token()

    counts: list[int] = []
    cumulative_mib = visual_kv_mib + first_text_mib
    for round_id in range(rounds):
        if round_id > 0:
            cumulative_mib += followup_mib
        scaled_bytes = scaled_mib(cumulative_mib, args)
        counts.append(max(1, math.ceil(scaled_bytes / block_size)))
    return tuple(counts)


def kv_blocks_for_round(session: SessionSpec, round_id: int) -> int:
    if not session.kv_keys:
        return 0
    if session.round_kv_block_counts:
        return min(len(session.kv_keys), session.round_kv_block_counts[round_id])
    progress = (round_id + 1) / max(1, session.rounds)
    return max(1, math.ceil(len(session.kv_keys) * progress))


def session_shape(index: int, rng: random.Random) -> tuple[str, int]:
    del index
    kind = weighted_choice(
        rng,
        [
            ("text", 0.15),
            ("single_image_single_round", 0.45),
            ("single_image_multi_round", 0.20),
            ("multi_image_compare", 0.08),
            ("batch_large_image", 0.02),
            ("video_short_single_round", 0.04),
            ("video_short_multi_round", 0.01),
            ("video_medium_single_round", 0.04),
            ("video_medium_multi_round", 0.01),
        ],
    )
    if kind == "text":
        image_count = 0
    elif kind.startswith("video_"):
        image_count = 1
    elif kind in {"single_image_single_round", "single_image_multi_round"}:
        image_count = 1
    elif kind == "multi_image_compare":
        image_count = rng.randint(2, 4)
    else:
        image_count = rng.randint(5, 8)
    return kind, image_count


def rounds_for_kind(kind: str, rng: random.Random) -> int:
    if kind in {"single_image_single_round", "video_short_single_round", "video_medium_single_round"}:
        return 1
    if kind == "batch_large_image":
        return rng.randint(10, 16)
    if kind in {
        "single_image_multi_round",
        "video_short_multi_round",
        "video_medium_multi_round",
        "multi_image_compare",
    }:
        bucket = weighted_choice(
            rng,
            [
                ("few", 0.78),
                ("multi", 0.22),
            ],
        )
        if bucket == "few":
            return rng.randint(2, 4)
        return rng.randint(5, 10)
    bucket = weighted_choice(
        rng,
        [
            ("single", 0.62),
            ("few", 0.28),
            ("multi", 0.08),
            ("long", 0.02),
        ],
    )
    if bucket == "single":
        return 1
    if bucket == "few":
        return rng.randint(2, 4)
    if bucket == "multi":
        return rng.randint(5, 10)
    return rng.randint(11, 16)


def prompt_bucket_for_kind(kind: str, rng: random.Random) -> str:
    if kind == "batch_large_image":
        return "xlong"
    if kind == "video_medium_multi_round":
        return weighted_choice(rng, [("long", 0.80), ("xlong", 0.20)])
    if kind.startswith("video_"):
        return weighted_choice(rng, [("tiny", 0.35), ("short", 0.45), ("long", 0.20)])
    return weighted_choice(
        rng,
        [
            ("tiny", 0.55),
            ("short", 0.30),
            ("long", 0.13),
            ("xlong", 0.02),
        ],
    )


def image_bucket_for_kind(kind: str, rng: random.Random) -> str:
    if kind.startswith("video_"):
        return "large"
    if kind == "batch_large_image":
        return weighted_choice(rng, [("large", 0.80), ("xlarge", 0.20)])
    if kind == "multi_image_compare":
        return weighted_choice(rng, [("standard", 0.55), ("large", 0.40), ("xlarge", 0.05)])
    return weighted_choice(
        rng,
        [
            ("small", 0.60),
            ("standard", 0.28),
            ("large", 0.10),
            ("xlarge", 0.02),
        ],
    )


def modality_for_kind(kind: str) -> str:
    return "video" if kind.startswith("video_") else "image"


def video_frames_for_kind(kind: str) -> int:
    if kind.startswith("video_short_"):
        return 4
    if kind.startswith("video_medium_"):
        return 8
    return 0


def hidden_key(prefix: str, session_id: str, image_id: str, modality: str = "image") -> str:
    return f"hidden@model:qwen-vl@mm_encoder:{modality}@parallel:p@layout:l@{prefix}:{session_id}:{image_id}"


def kv_key(prefix: str, session_id: str, round_id: int) -> str:
    return f"{prefix}:kv:{session_id}:r{round_id}"


def kv_prefix_key(prefix: str, family_id: str, block_id: int) -> str:
    return f"{prefix}:kvprefix:{family_id}:b{block_id}"


def pressure_key(prefix: str, index: int) -> str:
    return f"{prefix}:pressure:{index}"


def public_hidden_key(
    prefix: str,
    *,
    modality: str,
    bucket: str,
    pool_slot: int,
) -> str:
    image_id = f"public_{modality}_{bucket}_{pool_slot:03d}"
    return hidden_key(prefix, "public_pool", image_id, modality)


def public_pool_slot_count(args: argparse.Namespace, modality: str) -> int:
    hit_rate = (
        args.public_cross_session_hit_rate
        if modality == "image"
        else max(args.public_cross_session_hit_rate * 0.7, args.private_cross_session_hit_rate)
    )
    # A higher cross-session hit rate means a smaller public material pool and
    # therefore more repeated Hidden keys across sessions.
    return max(1, int(args.sessions * max(0.02, 1.0 - hit_rate) * 0.25))


def build_shared_workload(args: argparse.Namespace) -> SharedWorkload:
    rng = random.Random(args.rand_seed)
    sessions: list[SessionSpec] = []
    kv_objects_by_key: dict[str, ObjectSpec] = {}
    hidden_objects_by_key: dict[str, ObjectSpec] = {}
    kv_family_count = max(1, getattr(args, "kv_prefix_family_count", 8))
    kv_prefix_share_ratio = min(1.0, max(0.0, getattr(args, "kv_prefix_share_ratio", 0.30)))
    public_visual_kv_share_rate = min(
        1.0,
        max(0.0, getattr(args, "public_visual_kv_share_rate", 0.50)),
    )

    for index in range(args.sessions):
        session_id = f"s{index:05d}"
        kind, image_count = session_shape(index, rng)
        rounds = rounds_for_kind(kind, rng)
        prompt_bucket = prompt_bucket_for_kind(kind, rng)
        modality = modality_for_kind(kind)
        video_frames = video_frames_for_kind(kind)

        images: list[ImageSpec] = []
        for image_index in range(image_count):
            bucket = image_bucket_for_kind(kind, rng)
            private_ratio = args.private_video_ratio if modality == "video" else args.private_image_ratio
            public = rng.random() >= private_ratio
            size = hidden_size_for_bucket(bucket, args)
            recompute_cost = (
                video_recompute_cost_ms(bucket, video_frames)
                if modality == "video"
                else image_recompute_cost_ms(bucket)
            )
            if public:
                pool_slot = rng.randrange(public_pool_slot_count(args, modality))
                key = public_hidden_key(
                    args.key_prefix,
                    modality=modality,
                    bucket=bucket,
                    pool_slot=pool_slot,
                )
                image_id = key.rsplit("@", 1)[-1]
            else:
                image_id = f"vid{image_index}" if modality == "video" else f"img{image_index}"
                key = hidden_key(args.key_prefix, session_id, image_id, modality)
            images.append(
                ImageSpec(
                    image_id=image_id,
                    modality=modality,
                    size_bucket=bucket,
                    hidden_key=key,
                    hidden_size=size,
                    public=public,
                    recompute_cost_ms=recompute_cost,
                    video_frames=video_frames,
                )
            )
            if modality == "video":
                group = "video_hidden"
            elif public:
                group = "public_hidden"
            else:
                group = "active_hidden"
            if modality == "image" and not public and rounds == 1:
                group = "cold_private_hidden"
            hidden_objects_by_key.setdefault(
                key,
                ObjectSpec(
                    key=key,
                    group=group,
                    size=size,
                    data_type="HIDDEN_STATE",
                    modality=modality,
                    video_frames=video_frames,
                    recompute_cost_ms=recompute_cost,
                    with_soft_pin=modality == "video" and args.soft_pin_video_hidden,
                )
            )

        kv_keys: list[str] = []
        round_kv_block_counts = kv_round_block_counts_for_session(
            kind=kind,
            prompt_bucket=prompt_bucket,
            rounds=rounds,
            image_buckets=tuple(image.size_bucket for image in images),
            args=args,
        )
        block_count = round_kv_block_counts[-1]
        block_size = kv_block_size(args)
        total_kv_mib = block_count * block_size / max(args.scale, 1e-12) / MiB
        block_recompute_cost = (total_kv_mib / kv_mib_per_token() * 0.08) / block_count
        shared_block_count = int(round(block_count * kv_prefix_share_ratio))
        if block_count > 1 and kv_prefix_share_ratio > 0.0:
            shared_block_count = max(1, min(block_count - 1, shared_block_count))
        else:
            shared_block_count = min(block_count, shared_block_count)

        public_visual_keys = [image.hidden_key for image in images if image.public]
        use_visual_prefix = bool(public_visual_keys) and rng.random() < public_visual_kv_share_rate
        if use_visual_prefix:
            visual_digest = hashlib.sha256(
                "|".join(sorted(public_visual_keys)).encode("utf-8")
            ).hexdigest()[:10]
            family_id = f"visual:{prompt_bucket}:{visual_digest}"
        else:
            family_id = f"system:{prompt_bucket}:{rng.randrange(kv_family_count):02d}"

        for block_id in range(block_count):
            if block_id < shared_block_count:
                key = kv_prefix_key(args.key_prefix, family_id, block_id)
            else:
                key = kv_key(args.key_prefix, session_id, block_id - shared_block_count)
            kv_keys.append(key)
            kv_objects_by_key.setdefault(
                key,
                ObjectSpec(
                    key=key,
                    group="kv_reuse",
                    size=block_size,
                    data_type="KVCACHE",
                    modality="kv",
                    recompute_cost_ms=block_recompute_cost,
                )
            )

        sessions.append(
            SessionSpec(
                session_id=session_id,
                kind=kind,
                prompt_bucket=prompt_bucket,
                rounds=rounds,
                images=tuple(images),
                kv_keys=tuple(kv_keys),
                round_kv_block_counts=round_kv_block_counts,
            )
        )

    kv_objects = tuple(kv_objects_by_key.values())
    hidden_objects = tuple(hidden_objects_by_key.values())

    pressure_count = max(1, int((len(kv_objects) + len(hidden_objects)) * args.pressure_multiplier))
    median_kv_size = sorted(obj.size for obj in kv_objects)[len(kv_objects) // 2] if kv_objects else args.min_object_size
    pressure_objects = tuple(
        ObjectSpec(
            key=pressure_key(args.key_prefix, index),
            group="pressure",
            size=median_kv_size,
            data_type="KVCACHE",
            modality="kv",
            recompute_cost_ms=0.0,
        )
        for index in range(pressure_count)
    )

    fingerprint_payload = json.dumps(
        [
            (
                s.kind,
                s.prompt_bucket,
                s.rounds,
                [(i.modality, i.size_bucket, i.video_frames, i.public, i.hidden_key) for i in s.images],
                s.kv_keys,
            )
            for s in sessions
        ],
        sort_keys=True,
    ).encode("utf-8")
    fingerprint = hashlib.sha256(fingerprint_payload).hexdigest()[:16]

    return SharedWorkload(
        sessions=tuple(sessions),
        kv_objects=kv_objects,
        hidden_objects=hidden_objects,
        pressure_objects=pressure_objects,
        fingerprint=fingerprint,
    )


def build_proxy_access_plan(
    workload: SharedWorkload,
    *,
    proxy_mode: str,
    args: argparse.Namespace,
) -> AccessPlan:
    request_plan = build_request_replay_plan(workload, proxy_mode=proxy_mode, args=args)
    get_events = list(request_plan.get_events)
    kv_get_keys = [key for object_type, key in get_events if object_type == "kv"]
    hidden_get_keys = [key for object_type, key in get_events if object_type == "hidden"]

    return AccessPlan(
        proxy_mode=proxy_mode,
        dataset_fingerprint=workload.fingerprint,
        kv_get_keys=tuple(kv_get_keys),
        hidden_get_keys=tuple(hidden_get_keys),
        get_events=tuple(get_events),
    )


def build_request_replay_plan(
    workload: SharedWorkload,
    *,
    proxy_mode: str,
    args: argparse.Namespace,
) -> RequestReplayPlan:
    if proxy_mode not in {"full_history", "session"}:
        raise ValueError(f"unknown proxy_mode={proxy_mode!r}")

    rng = random.Random(args.rand_seed + (17 if proxy_mode == "session" else 31))
    requests: list[RequestEvent] = []
    timeline_ms = 0.0

    for session in workload.sessions:
        timeline_ms += rng.expovariate(1.0 / max(args.session_arrival_mean_ms, 0.001))
        request_time_ms = timeline_ms
        rebuild_round = -1
        if (
            proxy_mode == "session"
            and session.images
            and session.rounds > 1
            and rng.random() < args.session_rebuild_rate
        ):
            rebuild_round = max(1, session.rounds // 2)

        for round_id in range(session.rounds):
            if round_id > 0:
                request_time_ms += rng.lognormvariate(
                    math.log(max(args.round_gap_median_ms, 0.001)),
                    max(args.round_gap_sigma, 0.001),
                )

            hidden_keys: tuple[str, ...]
            is_rebuild = False
            if proxy_mode == "full_history":
                hidden_keys = tuple(image.hidden_key for image in session.images)
            elif round_id == 0:
                hidden_keys = tuple(image.hidden_key for image in session.images)
            elif round_id == rebuild_round:
                hidden_keys = tuple(image.hidden_key for image in session.images)
                is_rebuild = bool(hidden_keys)
            else:
                hidden_keys = ()

            block_count = kv_blocks_for_round(session, round_id)
            kv_keys = tuple(session.kv_keys[:block_count])
            requests.append(
                RequestEvent(
                    timestamp_ms=request_time_ms,
                    session_id=session.session_id,
                    round_id=round_id,
                    hidden_keys=hidden_keys,
                    kv_keys=kv_keys,
                    is_rebuild=is_rebuild,
                )
            )

    requests.sort(key=lambda request: (request.timestamp_ms, request.session_id, request.round_id))
    return RequestReplayPlan(
        proxy_mode=proxy_mode,
        dataset_fingerprint=workload.fingerprint,
        requests=tuple(requests),
    )


def select_session_rebuild_hidden_keys(
    workload: SharedWorkload,
    args: argparse.Namespace,
) -> tuple[str, ...]:
    """Select session-proxy boundary rebuilds.

    Session proxy ordinary follow-ups do not touch historical image Hidden.
    The boundary rate is applied per multimodal multi-round session; selected
    sessions rebuild the full visual history once around the middle of the
    conversation.
    """

    rng = random.Random(args.rand_seed + 17)
    keys: list[str] = []
    for session in workload.sessions:
        if not session.images or session.rounds <= 1:
            continue
        if rng.random() < getattr(args, "session_rebuild_rate", args.session_hidden_edge_rate):
            keys.extend(image.hidden_key for image in session.images)
    return tuple(keys)


def unique_in_order(keys: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for key in keys:
        if key in seen:
            continue
        seen.add(key)
        result.append(key)
    return tuple(result)


def accessed_probe_keys(access_plan: AccessPlan) -> tuple[tuple[str, ...], tuple[str, ...]]:
    return (
        unique_in_order(access_plan.kv_get_keys),
        unique_in_order(access_plan.hidden_get_keys),
    )


def accessed_probe_event_keys(access_plan: AccessPlan) -> tuple[tuple[str, ...], tuple[str, ...]]:
    return access_plan.kv_get_keys, access_plan.hidden_get_keys


def object_type_for_spec(spec: ObjectSpec) -> str:
    return "kv" if spec.modality == "kv" else "hidden"


def args_for_write_workload(args: argparse.Namespace) -> argparse.Namespace:
    write_args = argparse.Namespace(**vars(args))
    write_args.key_prefix = f"{args.key_prefix}:write"
    write_args.rand_seed = args.rand_seed + 1009
    write_args.sessions = args.mixed_write_sessions
    return write_args


def build_pressure_session_workload(args: argparse.Namespace) -> SharedWorkload:
    return build_shared_workload(args_for_write_workload(args))


def build_mixed_workload_plan(
    prepared: SharedWorkload,
    access_plan: AccessPlan,
    args: argparse.Namespace,
) -> MixedWorkloadPlan:
    rng = random.Random(args.rand_seed + 4099)
    write_workload = build_shared_workload(args_for_write_workload(args))
    write_objects = session_ordered_objects(write_workload)
    write_index = 0
    read_source = tuple(MixedEvent("read", object_type, key) for object_type, key in access_plan.get_events)
    write_source = tuple(
        MixedEvent("write", object_type_for_spec(spec), spec.key)
        for spec in write_objects
    )

    events: list[MixedEvent] = []
    for _ in range(args.mixed_events):
        can_read = bool(read_source)
        can_write = write_index < len(write_source)
        if not can_read and not can_write:
            break
        do_read = can_read and (not can_write or rng.randrange(100) < args.rwmixread)
        if do_read:
            events.append(read_source[rng.randrange(len(read_source))])
        else:
            events.append(write_source[write_index])
            write_index += 1

    return MixedWorkloadPlan(
        write_workload=write_workload,
        write_objects=write_objects,
        events=tuple(events),
    )


def build_strategy_matrix(
    *,
    proxy_mode: str,
    ttl_values: list[str],
    hidden_budget_ratio: float,
    default_kv_ttl: str,
    hidden_default_ttl: str,
) -> list[StrategyCase]:
    del proxy_mode, hidden_budget_ratio, default_kv_ttl, hidden_default_ttl

    def safe(value: str) -> str:
        return "".join(ch if ch.isalnum() else "_" for ch in value)

    cases: list[StrategyCase] = []
    for ttl in ttl_values:
        cases.append(
            StrategyCase(
                name=f"original_baseline_{safe(ttl)}",
                master_flags=(
                    "--enable_hidden_type_aware_eviction=false "
                    f"--default_kv_lease_ttl={ttl} "
                    f"--default_kv_soft_pin_ttl={ttl} "
                    "--allow_hidden_in_global_eviction=false "
                    "--allow_evict_soft_pinned_objects=false"
                ),
            )
        )

    def add_hidden_budget_case(budget: float, hidden_ttl: str, kv_ttl: str) -> None:
        budget_name = f"{budget:.2f}".replace(".", "p")
        cases.append(
            StrategyCase(
                name=f"hidden_budget_{budget_name}_h{safe(hidden_ttl)}_kv{safe(kv_ttl)}",
                master_flags=(
                    "--enable_hidden_type_aware_eviction=true "
                    f"--hidden_memory_budget_ratio={budget:.2f} "
                    f"--default_hidden_lease_ttl={hidden_ttl} "
                    f"--soft_pinned_hidden_lease_ttl={hidden_ttl} "
                    f"--default_kv_lease_ttl={kv_ttl} "
                    "--default_kv_soft_pin_ttl=60s "
                    "--allow_hidden_in_global_eviction=false "
                    "--allow_evict_soft_pinned_objects=false"
                ),
            )
        )

    for budget, hidden_ttl, kv_ttl in (
        (0.06, "1s", "10s"),
        (0.08, "10s", "10s"),
        (0.08, "5s", "10s"),
        (0.08, "500ms", "5s"),
        (0.08, "500ms", "1s"),
        (0.10, "10s", "10s"),
        (0.10, "5s", "10s"),
        (0.10, "1s", "10s"),
        (0.10, "500ms", "5s"),
        (0.10, "500ms", "1s"),
        (0.12, "10s", "10s"),
    ):
        add_hidden_budget_case(budget, hidden_ttl, kv_ttl)

    def add_soft_pin_case(
        budget: float, hidden_ttl: str, hidden_soft_pin_ttl: str, kv_ttl: str
    ) -> None:
        budget_name = f"{budget:.2f}".replace(".", "p")
        cases.append(
            StrategyCase(
                name=(
                    f"soft_pin_budget_{budget_name}_h{safe(hidden_ttl)}_"
                    f"soft{safe(hidden_soft_pin_ttl)}_kv{safe(kv_ttl)}"
                ),
                master_flags=(
                    "--enable_hidden_type_aware_eviction=true "
                    f"--hidden_memory_budget_ratio={budget:.2f} "
                    f"--default_hidden_lease_ttl={hidden_ttl} "
                    f"--soft_pinned_hidden_lease_ttl={hidden_soft_pin_ttl} "
                    f"--default_kv_lease_ttl={kv_ttl} "
                    "--default_kv_soft_pin_ttl=60s "
                    "--allow_hidden_in_global_eviction=false "
                    "--allow_evict_soft_pinned_objects=false"
                ),
                bench_flags="--soft-pin-video-hidden",
            )
        )

    for budget, hidden_ttl, hidden_soft_pin_ttl, kv_ttl in (
        (0.06, "1s", "2s", "10s"),
        (0.08, "10s", "20s", "10s"),
        (0.08, "5s", "10s", "10s"),
        (0.08, "500ms", "1s", "5s"),
        (0.08, "500ms", "1s", "1s"),
    ):
        add_soft_pin_case(budget, hidden_ttl, hidden_soft_pin_ttl, kv_ttl)

    return cases


def summarize_group_metrics(groups: dict[str, ProbeGroupStats]) -> dict[str, float]:
    reusable = [
        groups.get("kv_reuse"),
        groups.get("active_hidden"),
        groups.get("public_hidden"),
        groups.get("video_hidden"),
    ]
    reusable_hit_bytes = sum(item.hit_bytes for item in reusable if item)
    reusable_total_bytes = sum(item.total_bytes for item in reusable if item)
    reusable_hit_recompute = sum(item.hit_recompute_ms for item in reusable if item)
    reusable_total_recompute = sum(item.total_recompute_ms for item in reusable if item)
    cold = groups.get("cold_private_hidden")
    pressure = groups.get("pressure")

    cold_release_bytes = cold.miss_bytes / cold.total_bytes if cold and cold.total_bytes else 0.0
    cold_release_objects = cold.miss_objects / cold.total_objects if cold and cold.total_objects else 0.0
    effective_conversion = (
        pressure.hit_bytes / cold.miss_bytes
        if pressure and cold and cold.miss_bytes
        else 0.0
    )

    return {
        "reusable_object_survival_bytes": reusable_hit_bytes / reusable_total_bytes
        if reusable_total_bytes
        else 0.0,
        "cold_hidden_release_bytes": cold_release_bytes,
        "effective_space_conversion_bytes": effective_conversion,
        "cold_private_hidden_release_objects": cold_release_objects,
        "saved_compute_ms": reusable_hit_recompute,
        "lost_compute_ms": reusable_total_recompute - reusable_hit_recompute,
    }


def summarize_accessed_metrics(
    accessed_kv: PhaseStats,
    accessed_hidden: PhaseStats,
) -> dict[str, float | int]:
    return {
        "accessed_kv_hit_objects": accessed_kv.successes,
        "accessed_kv_total_objects": accessed_kv.objects,
        "accessed_hidden_hit_objects": accessed_hidden.successes,
        "accessed_hidden_total_objects": accessed_hidden.objects,
        "accessed_kv_hit_rate": (
            accessed_kv.successes / accessed_kv.objects if accessed_kv.objects else 0.0
        ),
        "accessed_hidden_hit_rate": (
            accessed_hidden.successes / accessed_hidden.objects
            if accessed_hidden.objects
            else 0.0
        ),
        "accessed_saved_compute_ms": (
            accessed_kv.recompute_ms_saved + accessed_hidden.recompute_ms_saved
        ),
    }


def summarize_accessed_metrics_from_keys(
    *,
    kv_keys: Iterable[str],
    hidden_keys: Iterable[str],
    kv_hit_keys: set[str],
    hidden_hit_keys: set[str],
    specs_by_key: dict[str, ObjectSpec],
) -> dict[str, float | int]:
    kv_total = 0
    kv_hit = 0
    hidden_total = 0
    hidden_hit = 0
    kv_saved_compute = 0.0
    image_hidden_total = 0
    image_hidden_hit = 0
    video_hidden_total = 0
    video_hidden_hit = 0
    image_hidden_saved_compute = 0.0
    video_hidden_saved_compute = 0.0

    for key in kv_keys:
        kv_total += 1
        if key in kv_hit_keys:
            kv_hit += 1
            kv_saved_compute += specs_by_key[key].recompute_cost_ms

    for key in hidden_keys:
        spec = specs_by_key[key]
        hidden_total += 1
        if spec.modality == "video":
            video_hidden_total += 1
        else:
            image_hidden_total += 1
        if key not in hidden_hit_keys:
            continue
        hidden_hit += 1
        if spec.modality == "video":
            video_hidden_hit += 1
            video_hidden_saved_compute += spec.recompute_cost_ms
        else:
            image_hidden_hit += 1
            image_hidden_saved_compute += spec.recompute_cost_ms

    hidden_saved_compute = image_hidden_saved_compute + video_hidden_saved_compute
    return {
        "accessed_kv_hit_objects": kv_hit,
        "accessed_kv_total_objects": kv_total,
        "accessed_hidden_hit_objects": hidden_hit,
        "accessed_hidden_total_objects": hidden_total,
        "accessed_kv_hit_rate": kv_hit / kv_total if kv_total else 0.0,
        "accessed_hidden_hit_rate": hidden_hit / hidden_total if hidden_total else 0.0,
        "accessed_saved_compute_ms": kv_saved_compute + hidden_saved_compute,
        "accessed_kv_saved_compute_ms": kv_saved_compute,
        "accessed_image_hidden_total_objects": image_hidden_total,
        "accessed_image_hidden_hit_objects": image_hidden_hit,
        "accessed_image_hidden_hit_rate": (
            image_hidden_hit / image_hidden_total if image_hidden_total else 0.0
        ),
        "accessed_image_hidden_saved_compute_ms": image_hidden_saved_compute,
        "accessed_video_hidden_total_objects": video_hidden_total,
        "accessed_video_hidden_hit_objects": video_hidden_hit,
        "accessed_video_hidden_hit_rate": (
            video_hidden_hit / video_hidden_total if video_hidden_total else 0.0
        ),
        "accessed_video_hidden_saved_compute_ms": video_hidden_saved_compute,
    }


def payload_for(key: str, size: int) -> bytes:
    seed = sum(key.encode("utf-8")) & 0xFF
    return bytes([seed]) * size


def phase_to_dict(stats: PhaseStats) -> dict[str, Any]:
    duration = stats.duration_sec
    return {
        "name": stats.name,
        "objects": stats.objects,
        "successful_objects": stats.successes,
        "failed_objects": stats.failures,
        "misses": stats.misses,
        "verify_failures": stats.verify_failures,
        "bytes": stats.bytes_processed,
        "recompute_ms_saved": stats.recompute_ms_saved,
        "duration_sec": duration,
        "object_per_sec": stats.objects / duration if duration else 0.0,
        "MiB_per_sec": stats.bytes_processed / duration / MiB if duration else 0.0,
        "lat_mean_ms": statistics.mean(stats.latencies) * 1000 if stats.latencies else 0.0,
        "lat_p50_ms": percentile(stats.latencies, 0.50) * 1000,
        "lat_p95_ms": percentile(stats.latencies, 0.95) * 1000,
        "lat_p99_ms": percentile(stats.latencies, 0.99) * 1000,
        "error_counts": dict(stats.error_counts),
    }


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = (len(ordered) - 1) * q
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def workload_summary(
    workload: SharedWorkload,
    access_plan: AccessPlan,
    args: argparse.Namespace,
) -> dict[str, Any]:
    visuals = [visual for session in workload.sessions for visual in session.images]
    image_count = sum(visual.modality == "image" for visual in visuals)
    video_count = sum(visual.modality == "video" for visual in visuals)
    public_images = sum(visual.public for visual in visuals if visual.modality == "image")
    public_videos = sum(visual.public for visual in visuals if visual.modality == "video")
    by_kind = Counter(session.kind for session in workload.sessions)
    return {
        "dataset_fingerprint": workload.fingerprint,
        "sessions": len(workload.sessions),
        "scale": args.scale,
        "hidden_size_multiplier": args.hidden_size_multiplier,
        "session_kinds": dict(by_kind),
        "images": image_count,
        "videos": video_count,
        "public_images": public_images,
        "private_images": image_count - public_images,
        "public_videos": public_videos,
        "private_videos": video_count - public_videos,
        "kv_objects": len(workload.kv_objects),
        "hidden_objects": len(workload.hidden_objects),
        "soft_pinned_hidden_objects": sum(obj.with_soft_pin for obj in workload.hidden_objects),
        "pressure_objects": len(workload.pressure_objects),
        "kv_bytes": sum(obj.size for obj in workload.kv_objects),
        "hidden_bytes": sum(obj.size for obj in workload.hidden_objects),
        "pressure_bytes": sum(obj.size for obj in workload.pressure_objects),
        "kv_recompute_ms": sum(obj.recompute_cost_ms for obj in workload.kv_objects),
        "hidden_recompute_ms": sum(obj.recompute_cost_ms for obj in workload.hidden_objects),
        "proxy_mode": access_plan.proxy_mode,
        "kv_get_objects": access_plan.kv_get_objects,
        "hidden_get_objects": access_plan.hidden_get_objects,
    }


def object_by_key(workload: SharedWorkload) -> dict[str, ObjectSpec]:
    return {obj.key: obj for obj in workload.kv_objects + workload.hidden_objects + workload.pressure_objects}


def session_ordered_objects(workload: SharedWorkload) -> tuple[ObjectSpec, ...]:
    specs_by_key = object_by_key(workload)
    ordered: list[ObjectSpec] = []
    seen: set[str] = set()
    for session in workload.sessions:
        for image in session.images:
            if image.hidden_key not in seen:
                ordered.append(specs_by_key[image.hidden_key])
                seen.add(image.hidden_key)
        for key in session.kv_keys:
            if key not in seen:
                ordered.append(specs_by_key[key])
                seen.add(key)
    return tuple(ordered)


def group_specs(specs: Iterable[ObjectSpec]) -> dict[str, list[ObjectSpec]]:
    groups: dict[str, list[ObjectSpec]] = defaultdict(list)
    for spec in specs:
        groups[spec.group].append(spec)
    return dict(groups)


def dry_run_result(args: argparse.Namespace) -> dict[str, Any]:
    workload = build_shared_workload(args)
    access_plan = build_proxy_access_plan(workload, proxy_mode=args.proxy_mode, args=args)
    matrix = build_strategy_matrix(
        proxy_mode=args.proxy_mode,
        ttl_values=["500ms", "1s", "2s", "5s", "10s"],
        hidden_budget_ratio=0.15 if args.proxy_mode == "session" else 0.35,
        default_kv_ttl="10s",
        hidden_default_ttl="500ms",
    )
    return {
        "mode": "dry_run",
        "workload": workload_summary(workload, access_plan, args),
        "strategy_matrix": [case.__dict__ for case in matrix],
    }


class StoreHarness:
    def __init__(self, args: argparse.Namespace):
        store_module = importlib.import_module("mooncake.store")
        self.store_module = store_module
        self.store = store_module.MooncakeDistributedStore()
        ret = self.store.setup(
            args.local_hostname,
            args.metadata_server,
            args.global_segment_size,
            args.local_buffer_size,
            args.protocol,
            args.device_name,
            args.master_server,
        )
        if ret != 0:
            raise RuntimeError(f"store setup failed: {ret}")
        self._config_cache: dict[str, Any] = {}
        self.args = args

    def close(self) -> None:
        if hasattr(self.store, "close"):
            self.store.close()
        elif hasattr(self.store, "tearDownAll"):
            self.store.tearDownAll()

    def config_for(self, spec: ObjectSpec):
        key = (spec.data_type, spec.with_soft_pin)
        if key in self._config_cache:
            return self._config_cache[key]
        config = self.store_module.ReplicateConfig()
        config.replica_num = self.args.memory_replica_num
        config.nof_replica_num = self.args.nof_replica_num
        config.data_type = getattr(self.store_module.ObjectDataType, spec.data_type)
        if hasattr(config, "with_soft_pin"):
            config.with_soft_pin = spec.with_soft_pin
        self._config_cache[key] = config
        return config

    def put(self, spec: ObjectSpec) -> int:
        return self.store.put(spec.key, payload_for(spec.key, spec.size), self.config_for(spec))

    def get(self, spec: ObjectSpec) -> bool:
        payload = self.store.get(spec.key)
        return payload not in (None, b"")


def run_put_phase(harness: StoreHarness, name: str, specs: Iterable[ObjectSpec]) -> PhaseStats:
    stats = PhaseStats(name=name, start_time=time.time())
    for spec in specs:
        stats.objects += 1
        start = time.perf_counter()
        ret = harness.put(spec)
        stats.latencies.append(time.perf_counter() - start)
        if ret == 0:
            stats.successes += 1
            stats.bytes_processed += spec.size
        else:
            stats.failures += 1
            stats.error_counts[ret] += 1
    stats.end_time = time.time()
    return stats


def run_get_phase(
    harness: StoreHarness,
    name: str,
    keys: Iterable[str],
    specs_by_key: dict[str, ObjectSpec],
) -> PhaseStats:
    stats, _ = run_get_phase_with_hit_keys(harness, name, keys, specs_by_key)
    return stats


def run_get_phase_with_hit_keys(
    harness: StoreHarness,
    name: str,
    keys: Iterable[str],
    specs_by_key: dict[str, ObjectSpec],
) -> tuple[PhaseStats, set[str]]:
    stats = PhaseStats(name=name, start_time=time.time())
    hit_keys: set[str] = set()
    for key in keys:
        spec = specs_by_key[key]
        stats.objects += 1
        start = time.perf_counter()
        hit = harness.get(spec)
        stats.latencies.append(time.perf_counter() - start)
        if hit:
            stats.successes += 1
            hit_keys.add(key)
            stats.bytes_processed += spec.size
            stats.recompute_ms_saved += spec.recompute_cost_ms
        else:
            stats.failures += 1
            stats.misses += 1
            stats.error_counts["MISS"] += 1
    stats.end_time = time.time()
    return stats, hit_keys


def expand_probe_events_from_hit_keys(
    *,
    name: str,
    keys: Iterable[str],
    specs_by_key: dict[str, ObjectSpec],
    hit_keys: set[str],
) -> PhaseStats:
    stats = PhaseStats(name=name, start_time=time.time())
    for key in keys:
        spec = specs_by_key[key]
        stats.objects += 1
        if key in hit_keys:
            stats.successes += 1
            stats.bytes_processed += spec.size
            stats.recompute_ms_saved += spec.recompute_cost_ms
        else:
            stats.failures += 1
            stats.misses += 1
            stats.error_counts["MISS"] += 1
    stats.end_time = time.time()
    return stats


def merge_phase_stats(name: str, phases: Iterable[PhaseStats]) -> PhaseStats:
    merged = PhaseStats(name=name)
    starts: list[float] = []
    ends: list[float] = []
    for phase in phases:
        merged.latencies.extend(phase.latencies)
        merged.objects += phase.objects
        merged.successes += phase.successes
        merged.failures += phase.failures
        merged.misses += phase.misses
        merged.verify_failures += phase.verify_failures
        merged.bytes_processed += phase.bytes_processed
        merged.recompute_ms_saved += phase.recompute_ms_saved
        merged.error_counts.update(phase.error_counts)
        if phase.start_time:
            starts.append(phase.start_time)
        if phase.end_time:
            ends.append(phase.end_time)
    merged.start_time = min(starts) if starts else 0.0
    merged.end_time = max(ends) if ends else merged.start_time
    return merged


def run_access_event_phase(
    harness: StoreHarness,
    name: str,
    events: Iterable[tuple[str, str]],
    specs_by_key: dict[str, ObjectSpec],
) -> tuple[PhaseStats, dict[str, PhaseStats]]:
    by_type = {
        "kv": PhaseStats(name=f"{name}_kv", start_time=time.time()),
        "hidden": PhaseStats(name=f"{name}_hidden", start_time=time.time()),
    }
    for object_type, key in events:
        stats = by_type[object_type]
        spec = specs_by_key[key]
        stats.objects += 1
        start = time.perf_counter()
        hit = harness.get(spec)
        stats.latencies.append(time.perf_counter() - start)
        if hit:
            stats.successes += 1
            stats.bytes_processed += spec.size
            stats.recompute_ms_saved += spec.recompute_cost_ms
        else:
            stats.failures += 1
            stats.misses += 1
            stats.error_counts["MISS"] += 1
    end_time = time.time()
    for stats in by_type.values():
        stats.end_time = end_time
    return merge_phase_stats(name, by_type.values()), by_type


def run_request_get_phase(
    harness: StoreHarness,
    name: str,
    plan: RequestReplayPlan,
    specs_by_key: dict[str, ObjectSpec],
    *,
    timing_scale: float,
) -> tuple[PhaseStats, dict[str, PhaseStats], dict[str, set[str]]]:
    by_type = {
        "kv": PhaseStats(name=f"{name}_kv", start_time=time.time()),
        "hidden": PhaseStats(name=f"{name}_hidden", start_time=time.time()),
    }
    hit_keys = {"kv": set(), "hidden": set()}
    previous_timestamp = plan.requests[0].timestamp_ms if plan.requests else 0.0
    scale = max(0.0, timing_scale)

    for request in plan.requests:
        gap_sec = max(0.0, (request.timestamp_ms - previous_timestamp) * scale / 1000.0)
        if gap_sec:
            time.sleep(gap_sec)
        previous_timestamp = request.timestamp_ms

        for object_type, key in request.get_events:
            stats = by_type[object_type]
            spec = specs_by_key[key]
            stats.objects += 1
            start = time.perf_counter()
            hit = harness.get(spec)
            stats.latencies.append(time.perf_counter() - start)
            if hit:
                stats.successes += 1
                hit_keys[object_type].add(key)
                stats.bytes_processed += spec.size
                stats.recompute_ms_saved += spec.recompute_cost_ms
            else:
                stats.failures += 1
                stats.misses += 1
                stats.error_counts["MISS"] += 1

    end_time = time.time()
    for stats in by_type.values():
        stats.end_time = end_time
    return merge_phase_stats(name, by_type.values()), by_type, hit_keys


def run_mixed_event_phase(
    harness: StoreHarness,
    name: str,
    events: Iterable[MixedEvent],
    specs_by_key: dict[str, ObjectSpec],
    args: argparse.Namespace,
) -> tuple[PhaseStats, dict[str, PhaseStats], PhaseStats]:
    read_by_type = {
        "kv": PhaseStats(name=f"{name}_read_kv", start_time=time.time()),
        "hidden": PhaseStats(name=f"{name}_read_hidden", start_time=time.time()),
    }
    write_stats = PhaseStats(name=f"{name}_write", start_time=time.time())
    gap_sec = max(0.0, args.mixed_event_gap_ms / 1000.0)

    for event in events:
        spec = specs_by_key[event.key]
        if event.operation == "read":
            stats = read_by_type[event.object_type]
            stats.objects += 1
            start = time.perf_counter()
            hit = harness.get(spec)
            stats.latencies.append(time.perf_counter() - start)
            if hit:
                stats.successes += 1
                stats.bytes_processed += spec.size
                stats.recompute_ms_saved += spec.recompute_cost_ms
            else:
                stats.failures += 1
                stats.misses += 1
                stats.error_counts["MISS"] += 1
        elif event.operation == "write":
            write_stats.objects += 1
            start = time.perf_counter()
            ret = harness.put(spec)
            write_stats.latencies.append(time.perf_counter() - start)
            if ret == 0:
                write_stats.successes += 1
                write_stats.bytes_processed += spec.size
            else:
                write_stats.failures += 1
                write_stats.error_counts[ret] += 1
        else:
            raise ValueError(f"unknown mixed event operation={event.operation!r}")
        if gap_sec:
            time.sleep(gap_sec)

    end_time = time.time()
    for stats in [*read_by_type.values(), write_stats]:
        stats.end_time = end_time
    return merge_phase_stats(name, [*read_by_type.values(), write_stats]), read_by_type, write_stats


def run_request_replay_phase(
    harness: StoreHarness,
    name: str,
    plan: RequestReplayPlan,
    specs_by_key: dict[str, ObjectSpec],
) -> tuple[PhaseStats, dict[str, PhaseStats], dict[str, PhaseStats]]:
    get_by_type = {
        "kv": PhaseStats(name=f"{name}_get_kv", start_time=time.time()),
        "hidden": PhaseStats(name=f"{name}_get_hidden", start_time=time.time()),
    }
    put_by_type = {
        "kv": PhaseStats(name=f"{name}_put_kv", start_time=time.time()),
        "hidden": PhaseStats(name=f"{name}_put_hidden", start_time=time.time()),
    }

    previous_timestamp = plan.requests[0].timestamp_ms if plan.requests else 0.0
    for request in plan.requests:
        gap_sec = max(0.0, (request.timestamp_ms - previous_timestamp) / 1000.0)
        if gap_sec:
            time.sleep(gap_sec)
        previous_timestamp = request.timestamp_ms

        for object_type, key in request.get_events:
            spec = specs_by_key[key]
            get_stats = get_by_type[object_type]
            get_stats.objects += 1
            start = time.perf_counter()
            hit = harness.get(spec)
            get_stats.latencies.append(time.perf_counter() - start)
            if hit:
                get_stats.successes += 1
                get_stats.bytes_processed += spec.size
                get_stats.recompute_ms_saved += spec.recompute_cost_ms
                continue

            get_stats.failures += 1
            get_stats.misses += 1
            get_stats.error_counts["MISS"] += 1

            put_stats = put_by_type[object_type]
            put_stats.objects += 1
            start = time.perf_counter()
            ret = harness.put(spec)
            put_stats.latencies.append(time.perf_counter() - start)
            if ret == 0:
                put_stats.successes += 1
                put_stats.bytes_processed += spec.size
            else:
                put_stats.failures += 1
                put_stats.error_counts[ret] += 1

    end_time = time.time()
    for stats in [*get_by_type.values(), *put_by_type.values()]:
        stats.end_time = end_time
    return (
        merge_phase_stats(name, [*get_by_type.values(), *put_by_type.values()]),
        get_by_type,
        put_by_type,
    )


def mixed_workload_summary(plan: MixedWorkloadPlan) -> dict[str, Any]:
    read_events = [event for event in plan.events if event.operation == "read"]
    write_events = [event for event in plan.events if event.operation == "write"]
    return {
        "mixed_events": len(plan.events),
        "mixed_read_events": len(read_events),
        "mixed_write_events": len(write_events),
        "mixed_write_objects": len(plan.write_objects),
        "mixed_write_kv_objects": sum(1 for obj in plan.write_objects if obj.modality == "kv"),
        "mixed_write_hidden_objects": sum(1 for obj in plan.write_objects if obj.modality != "kv"),
        "mixed_write_bytes": sum(obj.size for obj in plan.write_objects),
    }


def request_replay_summary(plan: RequestReplayPlan) -> dict[str, Any]:
    return {
        "request_events": len(plan.requests),
        "request_kv_get_objects": plan.kv_get_objects,
        "request_hidden_get_objects": plan.hidden_get_objects,
        "request_rebuild_events": sum(request.is_rebuild for request in plan.requests),
        "request_timeline_ms": (
            plan.requests[-1].timestamp_ms - plan.requests[0].timestamp_ms
            if len(plan.requests) >= 2
            else 0.0
        ),
    }


def request_eviction_workload_summary(
    *,
    prepared_objects: tuple[ObjectSpec, ...],
    pressure_objects: tuple[ObjectSpec, ...],
    request_plan: RequestReplayPlan,
) -> dict[str, Any]:
    return {
        "prepared_objects": len(prepared_objects),
        "prepared_bytes": sum(obj.size for obj in prepared_objects),
        "pressure_objects": len(pressure_objects),
        "pressure_bytes": sum(obj.size for obj in pressure_objects),
        "request_events": len(request_plan.requests),
        "request_kv_get_objects": request_plan.kv_get_objects,
        "request_hidden_get_objects": request_plan.hidden_get_objects,
        "request_rebuild_events": sum(request.is_rebuild for request in request_plan.requests),
        "request_timeline_ms": (
            request_plan.requests[-1].timestamp_ms - request_plan.requests[0].timestamp_ms
            if len(request_plan.requests) >= 2
            else 0.0
        ),
    }


def run_store_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    workload = build_shared_workload(args)
    access_plan = build_proxy_access_plan(workload, proxy_mode=args.proxy_mode, args=args)
    mixed_plan = build_mixed_workload_plan(workload, access_plan, args)
    request_plan = build_request_replay_plan(workload, proxy_mode=args.proxy_mode, args=args)
    pressure_workload = build_pressure_session_workload(args)
    prepared_objects = session_ordered_objects(workload)
    pressure_session_objects = session_ordered_objects(pressure_workload)
    specs_by_key = object_by_key(workload) | {
        obj.key: obj for obj in mixed_plan.write_objects
    } | {
        obj.key: obj for obj in pressure_session_objects
    }
    harness = StoreHarness(args)
    phases: list[PhaseStats] = []
    try:
        if args.execution_mode == "request_eviction":
            phases.append(run_put_phase(harness, "prepare_write", prepared_objects))
            warm_stats, warm_by_type, _warm_hit_keys = run_request_get_phase(
                harness,
                "warm_request_replay",
                request_plan,
                specs_by_key,
                timing_scale=args.request_timing_scale,
            )
            phases.append(warm_stats)
            time.sleep(args.cooldown_sec)
            pressure_stats = run_put_phase(
                harness,
                "pressure_write",
                pressure_session_objects,
            )
            phases.append(pressure_stats)
            time.sleep(args.post_pressure_sec)
            probe_stats, probe_by_type, probe_hit_keys = run_request_get_phase(
                harness,
                "probe_request_replay",
                request_plan,
                specs_by_key,
                timing_scale=args.request_timing_scale,
            )
            phases.append(probe_stats)
            return {
                "mode": "store_request_eviction",
                "workload": workload_summary(workload, access_plan, args)
                | request_eviction_workload_summary(
                    prepared_objects=prepared_objects,
                    pressure_objects=pressure_session_objects,
                    request_plan=request_plan,
                ),
                "phases": [phase_to_dict(phase) for phase in phases],
                "warm_by_access": {
                    group: phase_to_dict(stats)
                    for group, stats in warm_by_type.items()
                },
                "pressure_write": phase_to_dict(pressure_stats),
                "probe_by_access": {
                    group: phase_to_dict(stats)
                    for group, stats in probe_by_type.items()
                },
                "summary": summarize_accessed_metrics_from_keys(
                    kv_keys=request_plan.kv_get_keys,
                    hidden_keys=request_plan.hidden_get_keys,
                    kv_hit_keys=probe_hit_keys["kv"],
                    hidden_hit_keys=probe_hit_keys["hidden"],
                    specs_by_key=specs_by_key,
                )
                | {
                    "pressure_write_objects": pressure_stats.objects,
                    "pressure_write_successful_objects": pressure_stats.successes,
                    "pressure_write_failed_objects": pressure_stats.failures,
                    "pressure_write_bytes": pressure_stats.bytes_processed,
                },
            }

        if args.execution_mode == "request":
            request_stats, request_get_by_type, request_put_by_type = run_request_replay_phase(
                harness,
                "request_replay",
                request_plan,
                specs_by_key,
            )
            phases.append(request_stats)
            return {
                "mode": "store_request",
                "workload": workload_summary(workload, access_plan, args)
                | request_replay_summary(request_plan),
                "phases": [phase_to_dict(phase) for phase in phases],
                "request_get_by_access": {
                    group: phase_to_dict(stats)
                    for group, stats in request_get_by_type.items()
                },
                "request_put_by_access": {
                    group: phase_to_dict(stats)
                    for group, stats in request_put_by_type.items()
                },
                "summary": summarize_accessed_metrics(
                    request_get_by_type["kv"],
                    request_get_by_type["hidden"],
                )
                | {
                    "request_put_kv_objects": request_put_by_type["kv"].objects,
                    "request_put_hidden_objects": request_put_by_type["hidden"].objects,
                    "request_put_kv_bytes": request_put_by_type["kv"].bytes_processed,
                    "request_put_hidden_bytes": request_put_by_type["hidden"].bytes_processed,
                },
            }

        if args.execution_mode == "mixed":
            prepared_objects = session_ordered_objects(workload)
            phases.append(run_put_phase(harness, "prepare_write", prepared_objects))
            mixed_stats, mixed_read_by_type, mixed_write_stats = run_mixed_event_phase(
                harness,
                "mixed_rw",
                mixed_plan.events,
                specs_by_key,
                args,
            )
            phases.append(mixed_stats)
            return {
                "mode": "store_mixed",
                "workload": workload_summary(workload, access_plan, args)
                | mixed_workload_summary(mixed_plan),
                "phases": [phase_to_dict(phase) for phase in phases],
                "mixed_read_by_access": {
                    group: phase_to_dict(stats)
                    for group, stats in mixed_read_by_type.items()
                },
                "mixed_write": phase_to_dict(mixed_write_stats),
                "summary": summarize_accessed_metrics(
                    mixed_read_by_type["kv"],
                    mixed_read_by_type["hidden"],
                ),
            }

        phases.append(run_put_phase(harness, "prepare_kv", workload.kv_objects))
        phases.append(run_put_phase(harness, "prepare_hidden", workload.hidden_objects))
        warm_access_stats, warm_access_by_type = run_access_event_phase(
            harness,
            "warm_access",
            access_plan.get_events,
            specs_by_key,
        )
        phases.append(warm_access_stats)
        time.sleep(args.cooldown_sec)
        phases.append(run_put_phase(harness, "pressure_write", workload.pressure_objects))
        time.sleep(args.post_pressure_sec)

        unique_accessed_kv_keys, unique_accessed_hidden_keys = accessed_probe_keys(access_plan)
        unique_accessed_kv_stats, accessed_kv_hit_keys = run_get_phase_with_hit_keys(
            harness,
            "probe_unique_accessed_kv",
            unique_accessed_kv_keys,
            specs_by_key,
        )
        unique_accessed_hidden_stats, accessed_hidden_hit_keys = run_get_phase_with_hit_keys(
            harness,
            "probe_unique_accessed_hidden",
            unique_accessed_hidden_keys,
            specs_by_key,
        )

        accessed_kv_keys, accessed_hidden_keys = accessed_probe_event_keys(access_plan)
        accessed_probe_stats = {
            "kv": expand_probe_events_from_hit_keys(
                name="probe_accessed_kv",
                keys=accessed_kv_keys,
                specs_by_key=specs_by_key,
                hit_keys=accessed_kv_hit_keys,
            ),
            "hidden": expand_probe_events_from_hit_keys(
                name="probe_accessed_hidden",
                keys=accessed_hidden_keys,
                specs_by_key=specs_by_key,
                hit_keys=accessed_hidden_hit_keys,
            ),
        }
        unique_accessed_probe_stats = {
            "kv": unique_accessed_kv_stats,
            "hidden": unique_accessed_hidden_stats,
        }

        grouped_specs = group_specs(
            workload.kv_objects + workload.hidden_objects + workload.pressure_objects
        )
        probe_stats: dict[str, PhaseStats] = {}
        for group, specs in grouped_specs.items():
            probe_stats[group] = run_get_phase(
                harness, f"probe_{group}", [spec.key for spec in specs], specs_by_key
            )

        group_summary = {
            group: ProbeGroupStats(
                total_objects=stats.objects,
                hit_objects=stats.successes,
                total_bytes=sum(spec.size for spec in grouped_specs[group]),
                hit_bytes=stats.bytes_processed,
                total_recompute_ms=sum(
                    spec.recompute_cost_ms for spec in grouped_specs[group]
                ),
                hit_recompute_ms=stats.recompute_ms_saved,
            )
            for group, stats in probe_stats.items()
        }

        return {
            "mode": "store",
            "workload": workload_summary(workload, access_plan, args),
            "phases": [phase_to_dict(phase) for phase in phases],
            "warm_by_access": {
                group: phase_to_dict(stats)
                for group, stats in warm_access_by_type.items()
            },
            "probe_by_access": {
                group: phase_to_dict(stats)
                for group, stats in accessed_probe_stats.items()
            },
            "probe_by_access_unique": {
                group: phase_to_dict(stats)
                for group, stats in unique_accessed_probe_stats.items()
            },
            "probe_by_group": {group: phase_to_dict(stats) for group, stats in probe_stats.items()},
            "summary": summarize_group_metrics(group_summary)
            | summarize_accessed_metrics(
                accessed_probe_stats["kv"],
                accessed_probe_stats["hidden"],
            ),
        }
    finally:
        harness.close()


def main() -> int:
    args = build_parser().parse_args()
    result = dry_run_result(args) if args.dry_run else run_store_benchmark(args)
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output_json:
        output = Path(args.output_json)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text + "\n", encoding="utf-8")
        print(output)
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
