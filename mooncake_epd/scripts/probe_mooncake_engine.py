#!/usr/bin/env python3
"""Subprocess-safe Mooncake direct-engine smoke probe."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.transfer import TransferEngine  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", default=os.getenv("MOONCAKE_PROTOCOL", "tcp"))
    parser.add_argument("--local-hostname", default=os.getenv("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1"))
    parser.add_argument("--metadata-server", default=os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE"))
    parser.add_argument("--device-name", default="")
    parser.add_argument("--buffer-bytes", type=int, default=4096)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine = TransferEngine(
        protocol=args.protocol,
        local_hostname=args.local_hostname,
        metadata_server=args.metadata_server,
        device_name=args.device_name,
    )
    try:
        report = engine.probe_direct_engine(buffer_bytes=args.buffer_bytes)
        print(json.dumps(report, ensure_ascii=False))
    finally:
        engine.shutdown()


if __name__ == "__main__":
    main()
