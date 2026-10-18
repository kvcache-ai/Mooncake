# PG Tests

## Tree Host Planning Tests

The C++ Tree topology and layout tests run without CUDA, PyTorch, or a GPU.
They are included in a normal `WITH_EP=ON`, `BUILD_UNIT_TESTS=ON` build. For a
standalone host build, point `MOONCAKE_PG_YLT_INCLUDE_DIR` at the `include`
directory of the pinned yalantinglibs dependency from a Mooncake build:

```bash
cmake -S mooncake-pg -B /tmp/mooncake-pg-host-tests \
    -DMOONCAKE_PG_HOST_TESTS_ONLY=ON \
    -DMOONCAKE_PG_YLT_INCLUDE_DIR=/path/to/yalantinglibs/include
cmake --build /tmp/mooncake-pg-host-tests --target pg_tree_host_test
ctest --test-dir /tmp/mooncake-pg-host-tests --output-on-failure
```

The tests cover all group sizes up to the PG capacity, sparse active rank
mapping, logical child slots, disjoint receive/staging subviews, channel and
tile coverage, alignment, capacity boundaries, and old/new wire compatibility.
A normal CUDA build additionally provides `pg_tree_runtime_host_test` for
Coordinator admission, policy regressions and complete recovery update budgets.
It needs the CUDA build dependencies but does not allocate GPU resources.

## Tree CUDA Tests

`test_pg_tree.py` explicitly enables `tree` and the `new` GPU backend for its
workers. It covers 1–5 GPUs, supported dtype/op combinations, empty/tail tiles,
slot reuse, caller streams, in-place/out-of-place buffers, two communicators
sharing a context workspace, fixed Graph replay, 2→3→2 membership changes,
rejection of a legacy joiner, and root/internal/leaf failures followed by
reusing the same Graph. Tests requiring more visible GPUs are skipped.

```bash
python -m unittest -v test_pg_tree.TreeGraphTests
```

For Host Proxy Graph tests, disable P2P and RDMA and use the explicit
non-blocking caller stream cases:

```bash
MOONCAKE_PG_DISABLE_P2P_ROUTE=1 MOONCAKE_PG_DISABLE_RDMA_ROUTE=1 \
python -m unittest -v \
  test_pg_tree.TreeGraphTests.test_nonblocking_stream_replay \
  test_pg_tree.TreeGraphTests.test_nonblocking_same_graph_scale_up_and_shrink
```

The default-stream Host Proxy issue recorded on the unmodified #4372 baseline
is unchanged. These tests do not establish support for that combination or
substitute for hardware RDMA validation.

Synchronize Graph replay before querying a captured Work's success/hints.
Failed output can be partially overwritten; discard it and restore the original
input before retrying. The tests capture an input fill on each replay and clear
the caller-owned hint tensor after a failed attempt. They do not recapture the
survivors' Graphs or retry a failed AllReduce inside the runtime.

## Environment Variables

- `MOONCAKE_PGTEST_DEVICE_FILTERS`
  Comma-separated NIC / IB device names passed to
  `pg.set_device_filter(...)`. Leave unset to use the backend default device
  selection.

- `MOONCAKE_PGTEST_MASTER_ADDR`
  Rendezvous address used for the local test process group. Defaults to
  `127.0.0.1`.

- `MOONCAKE_PGTEST_MASTER_PORT`
  Rendezvous port used for the local test process group. If unset, each test
  allocates a free local port automatically.

## Usage

```bash
# Run all test cases
python -m unittest discover -s mooncake-pg/tests -v

# Run CUDA test cases
python -m unittest discover -s mooncake-pg/tests -k CUDA -v

# Run CPU-only test cases
python -m unittest discover -s mooncake-pg/tests -k CPU -v
```
