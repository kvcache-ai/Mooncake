#!/usr/bin/env python3
import argparse
import os
import sys
import time


def make_payload(seq: int, size: int) -> bytes:
    seed = f"value{seq}".encode()
    repeat = (size // len(seed)) + 1
    return (seed * repeat)[:size]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Continuous MooncakeDistributedStore put/get workload"
    )
    parser.add_argument("--local-hostname", default="127.0.0.1:50071")
    parser.add_argument("--metadata-server", default="http://127.0.0.1:8080/metadata")
    parser.add_argument("--master-server", default="127.0.0.1:50051")
    parser.add_argument("--protocol", default="tcp")
    parser.add_argument("--device-name", default="")
    parser.add_argument("--global-segment-size", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--local-buffer-size", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--payload-size", type=int, default=4096)
    parser.add_argument("--duration-sec", type=int, default=20)
    parser.add_argument("--sleep-ms", type=int, default=200)
    parser.add_argument("--key-prefix", default="nof-e2e")
    parser.add_argument("--memory-replica-num", type=int, default=1)
    parser.add_argument("--nof-replica-num", type=int, default=1)
    args = parser.parse_args()

    try:
        import store  # type: ignore
    except Exception as exc:
        print(f"import_fail {exc}", flush=True)
        return 10

    mc = store.MooncakeDistributedStore()
    setup_ret = mc.setup(
        args.local_hostname,
        args.metadata_server,
        args.global_segment_size,
        args.local_buffer_size,
        args.protocol,
        args.device_name,
        args.master_server,
    )
    print(f"setup_ret {setup_ret}", flush=True)
    if setup_ret != 0:
        return setup_ret

    replicate_config = store.ReplicateConfig()
    replicate_config.replica_num = args.memory_replica_num
    replicate_config.nof_replica_num = args.nof_replica_num

    deadline = time.time() + args.duration_sec
    seq = 0
    put_ok = 0
    get_ok = 0
    put_fail = 0
    get_fail = 0
    mismatch = 0

    while time.time() < deadline:
        seq += 1
        key = f"{args.key_prefix}-{seq}"
        expected = make_payload(seq, args.payload_size)

        put_ret = mc.put(key, expected, replicate_config)
        if put_ret == 0:
            put_ok += 1
            print(f"put_ok seq={seq} key={key} len={len(expected)}", flush=True)
        else:
            put_fail += 1
            print(f"put_fail seq={seq} key={key} ret={put_ret}", flush=True)
            time.sleep(args.sleep_ms / 1000.0)
            continue

        actual = mc.get(key)
        if actual == expected:
            get_ok += 1
            print(f"get_ok seq={seq} key={key} len={len(actual)}", flush=True)
        elif actual in (b"", None):
            get_fail += 1
            actual_len = 0 if actual in (b"", None) else len(actual)
            print(f"get_fail seq={seq} key={key} len={actual_len}", flush=True)
        else:
            mismatch += 1
            print(
                f"data_mismatch seq={seq} key={key} expected_len={len(expected)} actual_len={len(actual)}",
                flush=True,
            )
            return 20

        time.sleep(args.sleep_ms / 1000.0)

    print(
        "summary "
        f"put_ok={put_ok} put_fail={put_fail} "
        f"get_ok={get_ok} get_fail={get_fail} mismatch={mismatch}",
        flush=True,
    )
    return 0 if mismatch == 0 else 20


if __name__ == "__main__":
    raise SystemExit(main())
