#!/usr/bin/env python3
"""
Mooncake Metadata Client CLI.

Usage:
  1) List all keys from master HTTP debug API:
     python mc_meta_cli.py
     python mc_meta_cli.py --master-server-address 26.5.36.248:50051

  2) Query one key from HTTP metadata server:
     python mc_meta_cli.py --query-key <key> --metadata-server http://127.0.0.1:8080/metadata

  3) Delete one key via RPC (default protocol: rpc_only):
     python mc_meta_cli.py --delete-key <key> --force

  4) Remove all keys via RPC:
     python mc_meta_cli.py --remove-all --force
"""
# pyright: reportMissingImports=false

from __future__ import annotations

import argparse
import http.client
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def parse_master_host(master_server_address: str) -> str:
    if ":" not in master_server_address:
        return master_server_address
    return master_server_address.rsplit(":", 1)[0]


def fetch_keys(host: str, http_port: int, timeout: float) -> list[str]:
    url = f"http://{host}:{http_port}/get_all_keys"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP error {exc.code} for {url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to connect {url}: {exc.reason}") from exc
    return [line.strip() for line in text.splitlines() if line.strip()]


def build_metadata_url(metadata_server: str, key: str) -> str:
    encoded_key = urllib.parse.quote(key, safe="")
    sep = "&" if "?" in metadata_server else "?"
    return f"{metadata_server}{sep}key={encoded_key}"


def metadata_hint(metadata_server: str) -> str:
    if ":50051" in metadata_server:
        return (
            " Hint: 50051 is usually Mooncake RPC port, not HTTP metadata port. "
            "Try something like http://<host>:8080/metadata."
        )
    return ""


def query_key_by_http(metadata_server: str, key: str, timeout: float) -> bool:
    url = build_metadata_url(metadata_server, key)
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout):
            return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise RuntimeError(f"HTTP error {exc.code} for {url}") from exc
    except http.client.RemoteDisconnected as exc:
        raise RuntimeError(
            f"Remote closed connection for {url}.{metadata_hint(metadata_server)}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"Failed to connect {url}: {exc.reason}.{metadata_hint(metadata_server)}"
        ) from exc


def setup_store(args: argparse.Namespace) -> Any:
    try:
        from mooncake.store import MooncakeDistributedStore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Cannot import mooncake.store. Please install mooncake wheel first."
        ) from exc

    store = MooncakeDistributedStore()
    ret = store.setup(
        args.local_hostname,
        args.metadata_server,
        args.global_segment_size,
        args.local_buffer_size,
        args.protocol,
        args.rdma_devices,
        args.master_server_address,
    )
    if ret != 0:
        raise RuntimeError(f"store.setup failed, return code={ret}")
    return store


def delete_key_by_rpc(args: argparse.Namespace, key: str, force: bool) -> int:
    store = setup_store(args)
    try:
        return store.remove(key, force)
    finally:
        store.close()


def remove_all_by_rpc(args: argparse.Namespace, force: bool) -> int:
    store = setup_store(args)
    try:
        return int(store.remove_all(force))
    finally:
        store.close()


def tune_rpc_only_memory(args: argparse.Namespace) -> None:
    if args.protocol == "rpc_only":
        if args.local_buffer_size == 128 * 1024 * 1024:
            args.local_buffer_size = 0
        if args.global_segment_size == 512 * 1024 * 1024:
            args.global_segment_size = 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mooncake metadata client manager.")
    parser.add_argument(
        "--master-server-address",
        default="localhost:50051",
        help="Master RPC address (default: 26.5.36.248:50051).",
    )
    parser.add_argument(
        "--master-http-port",
        type=int,
        default=9003,
        help="Master HTTP metrics/debug port (default: 9003).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="HTTP timeout seconds (default: 5).",
    )
    parser.add_argument(
        "--query-key",
        default=None,
        help="Query one key by HTTP metadata endpoint.",
    )
    parser.add_argument(
        "--delete-key",
        default=None,
        help="Delete one key by RPC store.remove().",
    )
    parser.add_argument(
        "--remove-all",
        action="store_true",
        help="Remove all keys by RPC store.remove_all().",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force delete for RPC remove (skip lease/task checks).",
    )
    parser.add_argument(
        "--metadata-server",
        default="http://127.0.0.1:8080/metadata",
        help="HTTP metadata endpoint for query path; also used by RPC setup.",
    )
    parser.add_argument(
        "--local-hostname",
        default="localhost",
        help="store.setup local_hostname for RPC delete.",
    )
    parser.add_argument(
        "--global-segment-size",
        type=int,
        default=512 * 1024 * 1024,
        help="store.setup global_segment_size for RPC delete.",
    )
    parser.add_argument(
        "--local-buffer-size",
        type=int,
        default=128 * 1024 * 1024,
        help="store.setup local_buffer_size for RPC delete.",
    )
    parser.add_argument(
        "--protocol",
        default="rpc_only",
        help="store.setup protocol for RPC delete (default: rpc_only).",
    )
    parser.add_argument(
        "--rdma-devices",
        default="",
        help="store.setup rdma_devices for RPC delete.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.query_key or args.delete_key or args.remove_all:
        try:
            if args.query_key:
                exists = query_key_by_http(
                    args.metadata_server, args.query_key, args.timeout
                )
                print(f"query_key={args.query_key}")
                print(f"exists={exists}")
            if args.delete_key:
                tune_rpc_only_memory(args)
                rc = delete_key_by_rpc(args, args.delete_key, args.force)
                print(f"delete_key={args.delete_key}")
                print(f"remove_rc={rc}")
            if args.remove_all:
                tune_rpc_only_memory(args)
                removed = remove_all_by_rpc(args, args.force)
                print("remove_all=true")
                print(f"removed_count={removed}")
        except RuntimeError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        return 0

    host = parse_master_host(args.master_server_address)
    try:
        keys = fetch_keys(host, args.master_http_port, args.timeout)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(f"key_count={len(keys)}")
    for key in keys:
        print(key)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
