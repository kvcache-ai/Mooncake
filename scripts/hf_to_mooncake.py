#!/usr/bin/env python3
"""Load model from Hugging Face and store in Mooncake Store."""

import os
import sys
import argparse
import pickle
from huggingface_hub import snapshot_download
from mooncake.store import MooncakeDistributedStore


def setup_store(metadata_server, master_server, local_buffer_size_gb=1):
    """Initialize Mooncake Store client."""
    store = MooncakeDistributedStore()
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "ibp6s0")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    global_segment_size = 3200 * 1024 * 1024
    local_buffer_size = local_buffer_size_gb * 1024 * 1024 * 1024

    retcode = store.setup(
        local_hostname,
        metadata_server,
        global_segment_size,
        local_buffer_size,
        protocol,
        device_name,
        master_server
    )

    if retcode:
        raise RuntimeError(f"Failed to setup store: {retcode}")

    return store


def main():
    parser = argparse.ArgumentParser(description="Load HF model to Mooncake Store")
    parser.add_argument("model_id", help="Hugging Face model ID")
    parser.add_argument("--key", required=True, help="Mooncake Store key")
    parser.add_argument("--metadata-server", default="http://127.0.0.1:8080/metadata")
    parser.add_argument("--master-server", default="127.0.0.1:50051")
    parser.add_argument("--cache-dir", help="HF cache directory")
    parser.add_argument("--buffer-size-gb", type=int, default=1)
    args = parser.parse_args()

    print(f"Downloading {args.model_id}...")
    model_path = snapshot_download(args.model_id, cache_dir=args.cache_dir)
    print(f"Downloaded to {model_path}")

    print("Setting up Mooncake Store...")
    store = setup_store(args.metadata_server, args.master_server, args.buffer_size_gb)

    print(f"Storing model path to key '{args.key}'...")
    data = pickle.dumps({"model_path": model_path, "model_id": args.model_id})
    store.put(args.key, data)
    print("Done")


if __name__ == "__main__":
    main()
