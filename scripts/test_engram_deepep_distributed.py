"""Two-node real-RDMA correctness test (run concurrently on both nodes).

    python scripts/test_engram_deepep_distributed.py --shard-id 0 \
        --master http://HOST:PORT/metadata --session UNIQUE --device 0

Repeat on node 1 with --shard-id 1. Start Mooncake Master's metadata service
first. No model weights are used. Both directions include local and remote rows.
"""

import argparse
import json

import numpy as np
import torch

from mooncake.engram import EngramStore


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--shard-id", type=int, required=True)
    p.add_argument("--master", required=True)
    p.add_argument("--session", required=True)
    p.add_argument("--device", type=int, default=0)
    p.add_argument("--num-callers", type=int, default=1)
    p.add_argument("--local-dir", default="/dev/shm/mooncake-engram-test")
    a = p.parse_args()
    torch.cuda.set_device(a.device)
    torch.set_num_threads(2)
    heads = 24
    sizes = [17 + i for i in range(heads)]
    names = [f"head{i}" for i in range(heads)]
    tables = {n: {"num_rows": sizes[i], "row_bytes": 264} for i, n in enumerate(names)}
    owners = {n: i // (heads // 2) for i, n in enumerate(names)}
    reference = {
        n: (
            (np.arange(sizes[i])[:, None] * 17 + np.arange(264)[None, :] * 3 + i * 29)
            % 251
        ).astype(np.uint8)
        for i, n in enumerate(names)
    }
    local = {n: reference[n] for n in names if owners[n] == a.shard_id}
    offsets = tuple(100 * i for i in range(heads))
    stream = torch.cuda.Stream()
    checks = []
    stores, retained = [], []
    lifetime_replays = 0
    for policy in ("native", "flat", "auto"):
        store = EngramStore(
            tables=tables,
            namespace=f"test/{policy}",
            local_dir=a.local_dir,
            persistent=False,
        )
        store.initialize(
            local,
            shard_id=a.shard_id,
            num_shards=2,
            table_owners=owners,
            master_addr=a.master,
            session_id=a.session,
            caller_id=a.device,
            max_lookup_rows=4096 * heads,
            read_policy=policy,
            opt_max_rows=512 * heads,
            num_callers=a.num_callers,
            rdma_device=f"mlx5_bond_{a.device}",
            timeout=600,
        )
        stores.append(store)
        # A later layer may initialize after another layer has captured a graph.
        # Its bootstrap must not reset/unload the process-wide JIT runtime.
        for old_graph, _old_ids, old_out, old_expected in retained:
            with torch.cuda.stream(stream):
                old_graph.replay()
            stream.synchronize()
            np.testing.assert_array_equal(old_out.cpu().numpy(), old_expected)
            lifetime_replays += 1
        for tokens in (1, 32, 192, 512, 4096):
            # Table-major outer output strides exercise the existing API contract.
            out = torch.full(
                (heads, tokens, 264), 203, dtype=torch.uint8, device="cuda"
            ).transpose(0, 1)
            ids = torch.empty((tokens, heads), dtype=torch.int64, device="cuda")
            stream.wait_stream(torch.cuda.current_stream())

            def run():
                store.lookup(
                    names, ids, out, stream=stream.cuda_stream, offsets=offsets
                )

            def update(iteration):
                raw = (
                    np.arange(tokens)[:, None] * 7
                    + np.arange(heads)[None, :]
                    + iteration
                )
                raw %= np.array(sizes)[None, :]
                values = raw + np.array(offsets)[None, :]
                values[0, iteration % heads] = -1
                with torch.cuda.stream(stream):
                    ids.copy_(torch.from_numpy(values))
                    out.fill_(203)
                expected = np.stack(
                    [reference[n][raw[:, h]] for h, n in enumerate(names)], axis=1
                )
                expected[values == -1] = 203
                return expected

            for iteration in range(2):
                expected = update(iteration)
                run()
                stream.synchronize()
                np.testing.assert_array_equal(out.cpu().numpy(), expected)
            graph = torch.cuda.CUDAGraph()
            with torch.cuda.graph(graph, stream=stream):
                run()
            for iteration in range(2, 7):
                expected = update(iteration)
                with torch.cuda.stream(stream):
                    graph.replay()
                stream.synchronize()
                np.testing.assert_array_equal(out.cpu().numpy(), expected)
            if tokens == 1:
                retained.append((graph, ids, out, expected.copy()))
            del graph
            checks.append(
                {
                    "policy": policy,
                    "tokens": tokens,
                    "passed": 7,
                    "allocation_id": store._remote.share.allocation_id,
                }
            )
    retained.clear()
    old_graph = _old_ids = old_out = None
    for store in stores:
        store.close()
    print(
        json.dumps(
            {
                "shard": a.shard_id,
                "device": a.device,
                "checks": checks,
                "lifetime_replays": lifetime_replays,
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
