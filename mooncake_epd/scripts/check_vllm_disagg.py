#!/usr/bin/env python3
"""Health check and request probe for vLLM disaggregated PD serving."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import requests


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def wait_ready(base_url: str, timeout_s: float = 300.0, paths: tuple[str, ...] = ("/health",)) -> None:
    s = _session()
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        for path in paths:
            try:
                r = s.get(f"{base_url}{path}", timeout=5)
                if r.status_code == 200:
                    return
                last = f"{path} -> {r.status_code}: {r.text[:200]}"
            except Exception as e:
                last = f"{path} -> {e}"
        time.sleep(2)
    raise RuntimeError(f"{base_url} not ready: {last}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefill-url", default="http://127.0.0.1:8100")
    ap.add_argument("--decode-url", default="http://127.0.0.1:8200")
    ap.add_argument("--proxy-url", default="http://127.0.0.1:8000")
    ap.add_argument("--request", required=True, help="Path to test request JSON")
    args = ap.parse_args()

    print(f"waiting for {args.prefill_url}")
    wait_ready(args.prefill_url, paths=("/health",))
    print(f"ready: {args.prefill_url}")

    print(f"waiting for {args.decode_url}")
    wait_ready(args.decode_url, paths=("/health",))
    print(f"ready: {args.decode_url}")

    print(f"waiting for {args.proxy_url}")
    wait_ready(args.proxy_url, paths=("/health", "/healthcheck"))
    print(f"ready: {args.proxy_url}")

    body = json.loads(Path(args.request).read_text(encoding="utf-8"))
    s = _session()
    resp = s.post(
        f"{args.proxy_url}/v1/chat/completions",
        json=body,
        timeout=300,
    )
    print("status:", resp.status_code)
    print("headers:", {k: resp.headers.get(k) for k in ["x-request-id", "x-epd-routing-path", "x-epd-admission", "x-epd-degrade-level"]})
    print(resp.text[:2000])
    resp.raise_for_status()

    try:
        metrics = s.get(f"{args.proxy_url}/metrics", timeout=30)
        print("metrics status:", metrics.status_code)
        print(metrics.text[:2000])
    except Exception as e:
        print("metrics probe failed:", e)


if __name__ == "__main__":
    main()
