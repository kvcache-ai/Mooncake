# Mooncake PG Tests

This directory contains functional tests for Mooncake Backend (PG), including
CPU collectives, accelerator collectives, point-to-point operations, process
group initialization, inference-oriented collective patterns, topology cases,
and elastic recovery.

## Environment variables

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

## Quick start

Run all PG tests:

```bash
python -m unittest discover -s mooncake-pg/tests -v
```

Run CUDA tests only:

```bash
python -m unittest discover -s mooncake-pg/tests -k CUDA -v
```

Run CPU-only tests:

```bash
python -m unittest discover -s mooncake-pg/tests -k CPU -v
```

Select HCA devices explicitly:

```bash
MOONCAKE_PGTEST_DEVICE_FILTERS=mlx5_1,mlx5_2 \
python -m unittest discover -s mooncake-pg/tests -k CUDA -v
```

## Test coverage map

| File | Coverage |
| --- | --- |
| `test_pg_init_functional.py` | Backend initialization, backend options, CPU/CUDA setup. |
| `test_pg_collectives.py` | All-reduce, broadcast, all-gather, reduce-scatter, gather, scatter, reduce, barrier, async work. |
| `test_pg_p2p.py` | `send` / `recv` / `batch_isend_irecv` style P2P behavior. |
| `test_pg_elastic.py` | Dynamic world size, extension ranks, `get_peer_state`, `recover_ranks`, `join_group`, elastic subgroups. |
| `test_pg_inference_collectives.py` | Collective patterns used by inference workloads. |
| `test_pg_inference_topologies.py` | Inference-oriented topology and group layout cases. |
| `pg_test_utils.py` | Shared multiprocessing harness and backend setup helpers. |

## Elastic recovery test notes

Elastic tests exercise the two-phase protocol:

1. Healthy ranks initialize with the current `world_size` and optional
   `max_world_size`.
2. Joining ranks initialize with `is_extension=True`.
3. Healthy ranks poll `pg.get_peer_state(...)` for joiner readiness.
4. Healthy ranks call `pg.recover_ranks(...)` to activate joiners.
5. Joining ranks call `pg.join_group(...)` and continue once extension state is
   published.

When adding tests for subgroups, keep `dist.new_group()` call order identical
across processes. PyTorch store prefixes and Mooncake backend indices are order
sensitive.

## Debugging failed tests

Check the following first:

- Does the active-rank tensor use `torch.int32` and the right device?
- If `max_world_size` is set, is `active_ranks` sized to `max_world_size`?
- Are all ranks using the same `MOONCAKE_PGTEST_MASTER_ADDR` and port?
- Are device filters consistent across ranks?
- Do all ranks call collectives and `new_group()` in the same order?
- Are CPU and CUDA failures identical, or does the issue only affect one device
  backend?

See `docs/source/troubleshooting/pg-ep-troubleshooting.md` for a broader
troubleshooting guide.
