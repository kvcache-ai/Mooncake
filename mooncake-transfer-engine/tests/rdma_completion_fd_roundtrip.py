# Round trip of the Python completion-fd names. Invoked by
# rdma_completion_fd_test, which skips this process when no RDMA device
# is present or MC_SLICE_TIMEOUT disables the path.
import os
import select
import sys

# Transport::TransferStatusEnum::COMPLETED
COMPLETED = 4


def main():
    try:
        import engine
    except Exception as exc:
        print("import engine failed: %s" % exc)
        return 1

    device = os.environ.get("MC_TEST_DEVICE_NAME", "")
    host = os.environ.get("MC_TEST_LOCAL_IP") or "127.0.0.1"
    source = engine.TransferEngine()
    target = engine.TransferEngine()
    if source.initialize("%s:0" % host, "P2PHANDSHAKE", "rdma", device) != 0:
        print("source initialize failed")
        return 1
    if target.initialize("%s:0" % host, "P2PHANDSHAKE", "rdma", device) != 0:
        print("target initialize failed")
        return 1

    peer = "%s:%d" % (host, target.get_rpc_port())
    length = 4096
    local = source.allocate_managed_buffer(length)
    remote = target.allocate_managed_buffer(length)
    if local == 0 or remote == 0:
        print("allocate_managed_buffer failed")
        return 1

    payload = b"P" * length
    source.write_bytes_to_buffer(local, payload.decode("ascii"), length)
    batch_id = source.batch_transfer_async(
        peer,
        [local],
        [remote],
        [length],
        engine.TransferOpcode.Write,
        enable_completion_notification=True,
    )
    fd = source.completion_fd()
    if fd < 0:
        print("completion_fd unavailable")
        return 1
    ready, _, _ = select.select([fd], [], [], 10)
    if not ready:
        print("completion fd was not readable")
        return 1
    rows = list(source.drain_completions())
    match = [row for row in rows if row["batch_id"] == batch_id]
    if len(match) != 1:
        print("drain returned %s" % rows)
        return 1
    row = match[0]
    if row["status"] != COMPLETED or row["transferred_bytes"] != length:
        print("unexpected completion %s" % row)
        return 1
    if target.read_bytes_from_buffer(remote, length) != payload:
        print("remote bytes mismatch")
        return 1
    source.free_batch_id(batch_id)
    print("OK")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("python round trip failed: %s" % exc)
        sys.exit(1)
