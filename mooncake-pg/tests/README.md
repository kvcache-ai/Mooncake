# PG Tests

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
