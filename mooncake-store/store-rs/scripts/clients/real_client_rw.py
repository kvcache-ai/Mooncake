#!/usr/bin/env python3
"""RealClient read/write verification for the current Mooncake store-rs stack.

This script validates real-mode put/get flows against the store-rs compatibility
layer. It is designed for cross-host and cross-container deployments:

- `--local_host` accepts `host:transport_port`
- metadata must use `redis://...` or `etcd://...`
- there is no master / `P2PHANDSHAKE` path in store-rs
- rw-only clients should use `--storage-bytes 0 --routed-writes`

Examples
--------

# storage node
python3 scripts/clients/real_client_rw.py \\
  --local_host 10.0.0.11:17111 \\
  --metadata_url redis://127.0.0.1:6379/0 \\
  --storage-bytes $((128 * 1024 * 1024)) \\
  --mode idle \\
  --hold-seconds 600

# rw-only writer
python3 scripts/clients/real_client_rw.py \\
  --local_host 10.0.0.21:17121 \\
  --metadata_url redis://127.0.0.1:6379/0 \\
  --storage-bytes 0 \\
  --routed-writes \\
  --mode write \\
  --key_prefix demo

# rw-only reader
python3 scripts/clients/real_client_rw.py \\
  --local_host 10.0.0.22:17122 \\
  --metadata_url redis://127.0.0.1:6379/0 \\
  --storage-bytes 0 \\
  --routed-writes \\
  --mode read \\
  --key_prefix demo
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import time
from typing import Iterable
from urllib.parse import urlsplit

from mooncake.store import MooncakeDistributedStore, ReplicateConfig


# ---------------------------------------------------------------------------
# cli helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="RealClient read/write verification for Mooncake store-rs"
    )
    parser.add_argument(
        "--local_host",
        "--local-host",
        required=True,
        help="Local data-plane endpoint, e.g. 192.168.0.158:17111",
    )
    parser.add_argument(
        "--transport_rpc_port",
        "--transport-rpc-port",
        type=int,
        default=None,
        help="Override the TENT TCP data-plane port; defaults to the port embedded in --local_host",
    )
    parser.add_argument(
        "--metadata_url",
        "--metadata-url",
        dest="metadata_url",
        required=True,
        help="Metadata endpoint, e.g. redis://host:6379/0 or etcd://host:2379",
    )
    parser.add_argument(
        "--transport_metadata_url",
        "--transport-metadata-url",
        default=None,
        help="Optional redis:// endpoint used by TENT when store metadata is etcd://",
    )
    parser.add_argument(
        "--master_addr",
        default="",
        help="Deprecated upstream master argument; ignored by store-rs compatibility mode",
    )
    parser.add_argument(
        "--protocol",
        default="tcp",
        help="Transfer protocol: tcp, rdma, or auto (default: tcp)",
    )
    parser.add_argument(
        "--device_names",
        default="",
        help="RDMA device names when --protocol=rdma",
    )
    parser.add_argument(
        "--storage-bytes",
        "--storage_bytes",
        "--global_segment_size",
        type=int,
        default=None,
        help=(
            "Local storage capacity in bytes; use 0 for rw-only clients "
            "(default: 0; also accepts legacy --global_segment_size)"
        ),
    )
    parser.add_argument(
        "--scratch-bytes",
        "--scratch_bytes",
        "--local_buffer_size",
        type=int,
        default=None,
        help=(
            "Local scratch capacity in bytes (default: 16 MiB; "
            "also accepts legacy --local_buffer_size)"
        ),
    )
    parser.add_argument(
        "--stable-id",
        "--stable_id",
        default=None,
        help="Optional stable client identity",
    )
    parser.add_argument(
        "--tenant",
        default="default",
        help="Tenant used for read/write operations (default: default)",
    )
    parser.add_argument(
        "--keyspace",
        default=None,
        help="Optional metadata keyspace prefix",
    )
    parser.add_argument(
        "--state",
        default="active",
        choices=["standby", "active", "draining", "sealed", "offline"],
        help="Initial lifecycle state (default: active)",
    )
    parser.add_argument(
        "--route-control",
        "--route_control",
        default="embedded_wrh",
        help="Route control mode: embedded_wrh / embedded-wrh / metadata_only / metadata-only",
    )
    parser.add_argument(
        "--route-topk",
        "--route_topk",
        type=int,
        default=2,
        help="Embedded WRH route authority replica count (default: 2)",
    )
    parser.add_argument(
        "--transport-backend",
        "--transport_backend",
        default=None,
        help="Real transport backend override: classic_te / classic-te / tent",
    )
    parser.add_argument(
        "--num_kv",
        type=int,
        default=30,
        help="Number of KV pairs to verify (default: 30)",
    )
    parser.add_argument(
        "--value_size",
        type=int,
        default=4096,
        help="Value size in bytes (default: 4096)",
    )
    parser.add_argument(
        "--key_prefix",
        default=None,
        help="Key prefix; keys become <prefix>-0, <prefix>-1, ...; optional in --mode idle",
    )
    parser.add_argument(
        "--mode",
        choices=["write", "read", "both", "idle"],
        default="both",
        help="write / read / both / idle (default: both)",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=1,
        help="Batch size; 1 uses single-op APIs (default: 1)",
    )
    parser.add_argument(
        "--replica_num",
        type=int,
        default=1,
        help="Replica count used for writes (default: 1)",
    )
    parser.add_argument(
        "--routed-writes",
        "--routed_writes",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable cluster-routed writes (default: true)",
    )
    parser.add_argument(
        "--prefer-local",
        "--prefer_local",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Prefer local storage owner during placement when available (default: true)",
    )
    parser.add_argument(
        "--with-soft-pin",
        "--with_soft_pin",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable soft pin placement hints (default: true)",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete all verified keys after reading them",
    )
    parser.add_argument(
        "--evacuate-owned-replicas",
        "--evacuate_owned_replicas",
        action="store_true",
        help="Run evacuate_owned_replicas() before exit",
    )
    parser.add_argument(
        "--hold-seconds",
        "--hold_seconds",
        type=float,
        default=0.0,
        help="Keep the client alive after operations for the specified time",
    )
    parser.add_argument(
        "--label",
        action="append",
        default=[],
        help="Additional client label in key=value form; may be passed multiple times",
    )
    args = parser.parse_args()
    apply_compat_defaults(args)
    validate_args(args)
    return args


def apply_compat_defaults(args: argparse.Namespace) -> None:
    if args.storage_bytes is None:
        args.storage_bytes = 0
    if args.scratch_bytes is None:
        args.scratch_bytes = 16 * 1024 * 1024
    if args.routed_writes is None:
        args.routed_writes = True
    if args.prefer_local is None:
        args.prefer_local = True
    if args.with_soft_pin is None:
        args.with_soft_pin = True
    args.route_control = normalize_route_control(args.route_control)


def validate_args(args: argparse.Namespace) -> None:
    if args.metadata_url == "P2PHANDSHAKE":
        raise SystemExit(
            "store-rs does not support P2PHANDSHAKE; use redis:// or etcd://"
        )
    if not (
        args.metadata_url.startswith("redis://")
        or args.metadata_url.startswith("etcd://")
    ):
        raise SystemExit(
            f"unsupported metadata_url {args.metadata_url!r}; expected redis:// or etcd://"
        )
    if args.batch_size <= 0:
        raise SystemExit("--batch_size must be greater than zero")
    if args.replica_num <= 0:
        raise SystemExit("--replica_num must be greater than zero")
    if args.route_topk < 2:
        raise SystemExit("--route-topk must be greater than or equal to 2")
    if args.transport_backend is not None:
        args.transport_backend = normalize_transport_backend(args.transport_backend)
    if args.storage_bytes < 0 or args.scratch_bytes <= 0:
        raise SystemExit("--storage-bytes must be >= 0 and --scratch-bytes must be > 0")
    host, port = normalize_local_endpoint(args.local_host, args.transport_rpc_port)
    if not host:
        raise SystemExit("--local_host must include a non-empty hostname")
    if port is None:
        raise SystemExit(
            "cross-host real-mode validation requires a fixed transport port; "
            "use --local_host host:port or --transport_rpc_port"
        )
    if (args.mode in ("write", "read", "both") or args.delete) and not args.key_prefix:
        raise SystemExit("--key_prefix is required unless --mode is idle")
    if (
        args.mode in ("write", "both")
        and args.storage_bytes == 0
        and not args.routed_writes
    ):
        raise SystemExit(
            "rw-only writers require --routed-writes when --storage-bytes is 0"
        )


def normalize_route_control(value: str) -> str:
    normalized = value.strip().lower().replace("-", "_")
    if normalized in ("embedded_wrh", "wrh"):
        return "embedded_wrh"
    if normalized in ("metadata_only", "metadata"):
        return "metadata_only"
    raise SystemExit(
        f"unsupported --route-control {value!r}; expected embedded_wrh or metadata_only"
    )


def normalize_transport_backend(value: str) -> str:
    normalized = value.strip().lower().replace("-", "_")
    if normalized == "classic_te":
        return "classic_te"
    if normalized == "tent":
        return "tent"
    raise SystemExit(
        f"unsupported --transport-backend {value!r}; expected classic_te or tent"
    )


def normalize_local_endpoint(
    local_host: str,
    explicit_port: int | None,
) -> tuple[str, int | None]:
    value = local_host.strip()
    embedded_host = value
    embedded_port = None

    if value.startswith("["):
        closing = value.find("]")
        if closing > 0 and closing + 1 < len(value) and value[closing + 1] == ":":
            candidate = value[closing + 2 :]
            if candidate.isdigit():
                embedded_host = value[1:closing]
                embedded_port = int(candidate)
    elif value.count(":") == 1:
        candidate_host, candidate_port = value.rsplit(":", 1)
        if candidate_host and candidate_port.isdigit():
            embedded_host = candidate_host
            embedded_port = int(candidate_port)

    if (
        embedded_port is not None
        and explicit_port is not None
        and embedded_port != explicit_port
    ):
        raise SystemExit(
            "embedded transport port in --local_host conflicts with --transport_rpc_port"
        )
    return embedded_host, explicit_port if explicit_port is not None else embedded_port


def parse_labels(entries: Iterable[str], storage_bytes: int) -> dict[str, str]:
    labels: dict[str, str] = {"storage": "true" if storage_bytes > 0 else "false"}
    for entry in entries:
        if "=" not in entry:
            raise SystemExit(f"invalid --label {entry!r}; expected key=value")
        key, value = entry.split("=", 1)
        key = key.strip()
        if not key:
            raise SystemExit(f"invalid --label {entry!r}; key must not be empty")
        labels[key] = value.strip()
    return labels


# ---------------------------------------------------------------------------
# payload helpers
# ---------------------------------------------------------------------------


def make_value(key: str, value_size: int) -> bytes:
    seed = hashlib.sha256(key.encode("utf-8")).digest()
    payload = bytearray()
    counter = 0
    while len(payload) < value_size:
        payload.extend(
            hashlib.sha256(seed + counter.to_bytes(8, byteorder="little")).digest()
        )
        counter += 1
    return bytes(payload[:value_size])


def chunked(items, chunk_size: int):
    for index in range(0, len(items), chunk_size):
        yield items[index : index + chunk_size]


def print_throughput(
    phase: str, item_count: int, value_size: int, elapsed: float
) -> None:
    total_bytes = item_count * value_size
    throughput_mib = total_bytes / elapsed / (1024 * 1024) if elapsed > 0 else 0.0
    ops_per_sec = item_count / elapsed if elapsed > 0 else 0.0
    print(
        f"[{phase}] {item_count} KVs in {elapsed:.3f}s  "
        f"ops/s={ops_per_sec:.1f}  throughput={throughput_mib:.2f} MiB/s"
    )


# ---------------------------------------------------------------------------
# store operations
# ---------------------------------------------------------------------------


def apply_replication_config(
    config: ReplicateConfig,
    *,
    replica_num: int,
    prefer_local: bool,
    with_soft_pin: bool,
) -> None:
    config.replica_num = replica_num
    if hasattr(config, "prefer_local"):
        config.prefer_local = prefer_local
    if hasattr(config, "prefer_alloc_in_same_node"):
        config.prefer_alloc_in_same_node = prefer_local
    if hasattr(config, "with_soft_pin"):
        config.with_soft_pin = with_soft_pin


def setup_store(
    store: MooncakeDistributedStore,
    *,
    local_hostname: str,
    metadata_url: str,
    storage_bytes: int,
    scratch_bytes: int,
    protocol: str,
    device_names: str,
    master_addr: str,
    stable_id: str | None,
    state: str,
    tenant: str,
    labels: dict[str, str],
    routed_writes: bool,
    replica_num: int,
    keyspace: str | None,
    transport_metadata_url: str | None,
    transport_rpc_port: int | None,
    route_control: str,
    route_topk: int,
    transport_backend: str | None,
) -> int:
    modern_kwargs = {
        "stable_id": stable_id,
        "initial_state": state,
        "tenant": tenant,
        "labels": labels,
        "routed_writes": routed_writes,
        "replica_count": replica_num,
        "route_topk": route_topk,
        "keyspace": keyspace,
        "transport_rpc_port": transport_rpc_port,
        "transport_backend": transport_backend,
        "route_control": route_control,
    }
    # transport_metadata_url (arg2): for the Transfer Engine (default P2PHANDSHAKE).
    # metadata_url (arg7): the Store-RS metadata backend (redis/etcd).
    try:
        return int(
            store.setup(
                local_hostname,
                transport_metadata_url or "P2PHANDSHAKE",
                storage_bytes,
                scratch_bytes,
                protocol,
                device_names,
                metadata_url or master_addr,
                **modern_kwargs,
            )
        )
    except TypeError as error:
        message = str(error)
        if "incompatible function arguments" not in message:
            raise
        print(
            "[WARN] setup() does not support store-rs kwargs; falling back to legacy API"
        )
        legacy_metadata_url = prepare_legacy_metadata_url(metadata_url)
        legacy_master_addr = master_addr or legacy_metadata_url
        return int(
            store.setup(
                local_hostname,
                legacy_metadata_url,
                storage_bytes,
                scratch_bytes,
                protocol,
                device_names,
                legacy_master_addr,
            )
        )


def prepare_legacy_metadata_url(metadata_url: str) -> str:
    if not metadata_url.startswith("redis://"):
        return metadata_url
    parts = urlsplit(metadata_url)
    if parts.hostname is None:
        return metadata_url

    legacy_netloc = parts.hostname
    if parts.port is not None:
        legacy_netloc = f"{legacy_netloc}:{parts.port}"

    if parts.username:
        existing_user = os.environ.get("MC_REDIS_USERNAME")
        if not existing_user:
            os.environ["MC_REDIS_USERNAME"] = parts.username
        elif existing_user != parts.username:
            print(
                "[WARN] MC_REDIS_USERNAME already set; ignoring username embedded in metadata_url"
            )

    if parts.password:
        existing_password = os.environ.get("MC_REDIS_PASSWORD")
        if not existing_password:
            os.environ["MC_REDIS_PASSWORD"] = parts.password
        elif existing_password != parts.password:
            print(
                "[WARN] MC_REDIS_PASSWORD already set; ignoring password embedded in metadata_url"
            )

    db_path = parts.path or ""
    if db_path not in ("", "/"):
        db_index = db_path[1:] if db_path.startswith("/") else db_path
        if db_index.isdigit():
            existing_index = os.environ.get("MC_REDIS_DB_INDEX")
            if not existing_index:
                os.environ["MC_REDIS_DB_INDEX"] = db_index
            elif existing_index != db_index:
                print(
                    "[WARN] MC_REDIS_DB_INDEX already set; ignoring db index embedded in metadata_url"
                )
        else:
            print(
                f"[WARN] legacy Mooncake may not support redis metadata path {db_path!r}"
            )

    return f"redis://{legacy_netloc}"


def write_all(store, keys_vals, batch_size: int, config: ReplicateConfig):
    started = time.perf_counter()
    total = len(keys_vals)
    if batch_size == 1:
        for index, (key, value) in enumerate(keys_vals, start=1):
            status = store.put(key, value, config=config)
            if int(status) != 0:
                raise RuntimeError(f"put failed key={key} status={status}")
            if index % 10 == 0 or index == total:
                print(f"[write] Progress: {index}/{total}")
    else:
        written = 0
        for batch in chunked(keys_vals, batch_size):
            status = store.batch_put(batch, config=config)
            if int(status) != 0:
                raise RuntimeError(
                    f"batch_put failed keys={[key for key, _ in batch]} status={status}"
                )
            written += len(batch)
            print(f"[write] Progress: {written}/{total} (batch_size={batch_size})")
    return time.perf_counter() - started


def read_all(store, keys, value_size: int, batch_size: int):
    started = time.perf_counter()
    total = len(keys)
    if batch_size == 1:
        for index, key in enumerate(keys, start=1):
            value = store.get(key)
            expected = make_value(key, value_size)
            if value != expected:
                got_len = len(value) if value is not None else None
                raise RuntimeError(
                    f"data mismatch key={key} expected_len={len(expected)} got_len={got_len}"
                )
            if index % 10 == 0 or index == total:
                print(f"[read] Progress: {index}/{total}")
    else:
        read = 0
        for batch in chunked(keys, batch_size):
            values = store.batch_get(batch)
            if not isinstance(values, list) or len(values) != len(batch):
                raise RuntimeError(f"batch_get returned unexpected payload: {values!r}")
            for key, value in zip(batch, values):
                expected = make_value(key, value_size)
                if value != expected:
                    got_len = len(value) if value is not None else None
                    raise RuntimeError(
                        f"data mismatch key={key} expected_len={len(expected)} got_len={got_len}"
                    )
            read += len(batch)
            print(f"[read] Progress: {read}/{total} (batch_size={batch_size})")
    return time.perf_counter() - started


def delete_all(store, keys, batch_size: int):
    started = time.perf_counter()
    if batch_size == 1:
        for key in keys:
            status = store.remove(key, force=True)
            if int(status) != 0:
                raise RuntimeError(f"remove failed key={key} status={status}")
    else:
        for batch in chunked(keys, batch_size):
            statuses = store.batch_remove(batch, force=True)
            for key, status in zip(batch, statuses):
                if int(status) != 0:
                    raise RuntimeError(f"batch_remove failed key={key} status={status}")
    return time.perf_counter() - started


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> int:
    args = parse_args()
    local_hostname, transport_rpc_port = normalize_local_endpoint(
        args.local_host,
        args.transport_rpc_port,
    )
    labels = parse_labels(args.label, args.storage_bytes)

    if args.master_addr:
        print(
            f"[WARN] --master_addr={args.master_addr!r} is ignored by store-rs compatibility mode"
        )

    store = MooncakeDistributedStore()
    config = ReplicateConfig()
    apply_replication_config(
        config,
        replica_num=args.replica_num,
        prefer_local=args.prefer_local,
        with_soft_pin=args.with_soft_pin,
    )

    print("[INFO] Setting up RealClient...")
    print(f"  local_host:          {args.local_host}")
    print(f"  local_hostname:      {local_hostname}")
    print(f"  transport_rpc_port:  {transport_rpc_port}")
    print(f"  metadata_url:        {args.metadata_url}")
    print(f"  transport_metadata_url:     {args.transport_metadata_url}")
    print(f"  protocol:            {args.protocol}")
    print(f"  device_names:        {args.device_names}")
    print(f"  storage_bytes:       {args.storage_bytes}")
    print(f"  scratch_bytes:       {args.scratch_bytes}")
    print(f"  routed_writes:       {args.routed_writes}")
    print(f"  stable_id:           {args.stable_id}")
    print(f"  tenant:              {args.tenant}")
    print(f"  keyspace:            {args.keyspace}")
    print(f"  key_prefix:          {args.key_prefix or '<unused>'}")
    print(f"  labels:              {labels}")
    print(f"  mode:                {args.mode}")
    print(f"  batch_size:          {args.batch_size}")
    print(f"  replica_num:         {args.replica_num}")
    print(f"  route_topk:          {args.route_topk}")
    print(f"  prefer_local:        {args.prefer_local}")
    print(f"  with_soft_pin:       {args.with_soft_pin}")
    print(f"  transport_backend:   {args.transport_backend}")

    rc = setup_store(
        store,
        local_hostname=local_hostname,
        metadata_url=args.metadata_url,
        storage_bytes=args.storage_bytes,
        scratch_bytes=args.scratch_bytes,
        protocol=args.protocol,
        device_names=args.device_names,
        master_addr=args.master_addr,
        stable_id=args.stable_id,
        state=args.state,
        tenant=args.tenant,
        labels=labels,
        routed_writes=args.routed_writes,
        replica_num=args.replica_num,
        keyspace=args.keyspace,
        transport_metadata_url=args.transport_metadata_url,
        transport_rpc_port=transport_rpc_port,
        route_control=args.route_control,
        route_topk=args.route_topk,
        transport_backend=args.transport_backend,
    )
    if int(rc) != 0:
        raise RuntimeError(f"setup failed status={rc}")

    keys = (
        [f"{args.key_prefix}-{index}" for index in range(args.num_kv)]
        if args.key_prefix
        else []
    )

    try:
        if args.mode in ("write", "both"):
            items = [(key, make_value(key, args.value_size)) for key in keys]
            elapsed = write_all(store, items, args.batch_size, config)
            print_throughput("write", args.num_kv, args.value_size, elapsed)
            print(f"[write] {args.num_kv} KVs written OK")

        if args.mode in ("read", "both"):
            elapsed = read_all(store, keys, args.value_size, args.batch_size)
            print_throughput("read", args.num_kv, args.value_size, elapsed)
            print(f"[read] {args.num_kv} KVs verified OK")

        if args.delete:
            elapsed = delete_all(store, keys, args.batch_size)
            print(f"[delete] {args.num_kv} KVs deleted OK in {elapsed:.3f}s")

        if args.evacuate_owned_replicas:
            moved = store.evacuate_owned_replicas()
            print(f"[evacuate] evacuated {moved} owned replica(s)")

        if args.hold_seconds > 0:
            print(f"[hold] sleeping for {args.hold_seconds:.1f}s before exit")
            time.sleep(args.hold_seconds)

        print("OK")
        return 0
    finally:
        store.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("[INFO] interrupted", file=sys.stderr)
        sys.exit(130)
    except Exception as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        sys.exit(1)
