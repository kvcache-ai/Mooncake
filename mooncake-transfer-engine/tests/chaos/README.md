# TE RDMA Chaos Runner

`te_chaos.py` runs the dedicated `rdma_transport_chaos_test` binary between two
hosts. It is intended to validate the production TransferEngine RDMA path under
manual chaos testing. It does not use `tebench`, and it does not modify or rely
on the CI-oriented `rdma_transport_test`.

Defaults are tailored for `qjh000 -> qjh001`:

- initiator: local host
- target: `qjh001`
- RDMA devices: `mlx5_1,mlx5_2,mlx5_3,mlx5_4`
- metadata mode: `P2PHANDSHAKE`
- protocol: `rdma`

The runner refuses a selection that includes `mlx5_0` or `eth0`.

## Build

```bash
cmake --build build --target rdma_transport_chaos_test -j16
```

## Check Environment

```bash
python3 mooncake-transfer-engine/tests/chaos/te_chaos.py doctor \
  --initiator qjh000 \
  --target qjh001 \
  --devices mlx5_1,mlx5_2,mlx5_3,mlx5_4
```

`doctor` checks SSH, the TE test binary, RDMA link visibility, and selected
device presence. It does not change network state.

## Run A Smoke Test

```bash
python3 mooncake-transfer-engine/tests/chaos/te_chaos.py run \
  --initiator qjh000 \
  --target qjh001 \
  --devices mlx5_1,mlx5_2,mlx5_3,mlx5_4 \
  --iterations 1
```

The target runs in `--mode=target`, the initiator runs in `--mode=initiator`,
and the initiator performs WRITE, READ, and data comparison through
`rdma_transport_chaos_test`.

## Run A Larger TE Correctness Test

`rdma_transport_chaos_test` supports configurable data size, worker count, and
iteration count. Additional flags are passed with `--extra-te-arg`.

```bash
python3 mooncake-transfer-engine/tests/chaos/te_chaos.py run \
  --initiator qjh000 \
  --target qjh001 \
  --devices mlx5_1,mlx5_2,mlx5_3,mlx5_4 \
  --iterations 1 \
  --timeout 300 \
  --extra-te-arg=--data_length=67108864 \
  --extra-te-arg=--num_threads=4 \
  --extra-te-arg=--iterations=50 \
  --extra-te-arg=--total_buffer_size=1073741824
```

This transfers about 25.6 GiB in total:

```text
64 MiB * WRITE/READ * 4 workers * 50 iterations
```

Each worker iteration verifies the data with `Compare: OK`. Any transfer or
compare failure makes `rdma_transport_chaos_test` return non-zero.

## Run Concurrent Fault Injection

Fault injection is opt-in. The default `run` command does not change network
state. With `--inject-faults`, each fault round can apply multiple faults at the
same time across qjh000/qjh001 and eth1..eth4.

```bash
python3 mooncake-transfer-engine/tests/chaos/te_chaos.py run \
  --initiator qjh000 \
  --target qjh001 \
  --devices mlx5_1,mlx5_2,mlx5_3,mlx5_4 \
  --netdevs eth1,eth2,eth3,eth4 \
  --iterations 1 \
  --timeout 420 \
  --inject-faults \
  --fault-kinds link-down,mixed-netem,rate-limit,loss,reorder,corrupt,duplicate \
  --max-concurrent-faults 8 \
  --fault-hold 4 \
  --fault-gap 1 \
  --fault-seed 20260826 \
  --extra-te-arg=--data_length=67108864 \
  --extra-te-arg=--num_threads=4 \
  --extra-te-arg=--iterations=100 \
  --extra-te-arg=--total_buffer_size=1073741824
```

Every applied and reverted fault is recorded in `events.jsonl`.

### Run Asynchronous Random Faults

Use `--fault-mode async` when faults should happen at arbitrary times rather
than in synchronized rounds. In this mode, each fault is scheduled
independently with a random host, netdev, kind, start time, and duration. Faults
can overlap with one another up to `--max-concurrent-faults`.

```bash
python3 mooncake-transfer-engine/tests/chaos/te_chaos.py run \
  --initiator qjh000 \
  --target qjh001 \
  --devices mlx5_1,mlx5_2,mlx5_3,mlx5_4 \
  --netdevs eth1,eth2,eth3,eth4 \
  --iterations 1 \
  --timeout 420 \
  --inject-faults \
  --fault-mode async \
  --fault-kinds link-down,mixed-netem,rate-limit,loss,reorder,corrupt,duplicate,delay \
  --max-concurrent-faults 6 \
  --fault-duration-min 1.5 \
  --fault-duration-max 6.0 \
  --fault-interval-min 0.2 \
  --fault-interval-max 1.0 \
  --fault-seed 424242 \
  --extra-te-arg=--data_length=67108864 \
  --extra-te-arg=--num_threads=4 \
  --extra-te-arg=--iterations=150 \
  --extra-te-arg=--total_buffer_size=1073741824
```

Async mode avoids stacking multiple qdiscs or link state changes on the same
`host:netdev` at the same time, but different rails and hosts can fail
independently and overlap freely.

For higher randomness, also randomize fault intensity, allow short bursts, and
randomize whether a scheduling point affects the initiator, the target, or both:

```bash
python3 mooncake-transfer-engine/tests/chaos/te_chaos.py run \
  --initiator qjh000 \
  --target qjh001 \
  --devices mlx5_1,mlx5_2,mlx5_3,mlx5_4 \
  --netdevs eth1,eth2,eth3,eth4 \
  --iterations 1 \
  --timeout 480 \
  --inject-faults \
  --fault-mode async \
  --fault-randomize-params \
  --fault-burst-probability 0.45 \
  --fault-burst-max 4 \
  --fault-host-scope random \
  --fault-kinds link-down,mixed-netem,rate-limit,loss,reorder,corrupt,duplicate,delay \
  --max-concurrent-faults 8 \
  --fault-duration-min 0.5 \
  --fault-duration-max 8.0 \
  --fault-interval-min 0.0 \
  --fault-interval-max 1.2 \
  --fault-seed 777777 \
  --extra-te-arg=--data_length=67108864 \
  --extra-te-arg=--num_threads=4 \
  --extra-te-arg=--iterations=150 \
  --extra-te-arg=--total_buffer_size=1073741824
```

Every scheduled fault records `host_scope` and the exact `tc` or `ip` command in
`events.jsonl`, so a random failure scene can be replayed from the seed and
inspected from the log.

## Cleanup

Cleanup is explicit. It removes root `tc` qdiscs and brings `eth1..eth4` up on
both hosts:

```bash
python3 mooncake-transfer-engine/tests/chaos/te_chaos.py cleanup \
  --initiator qjh000 \
  --target qjh001
```

## Outputs

Each run writes under `build/chaos-runs/<timestamp>/` by default:

- `events.jsonl`: process lifecycle and command results
- `summary.json`: per-iteration exit status
- `*-target.log`: target-side `rdma_transport_chaos_test` output
- `*-initiator.log`: initiator-side `rdma_transport_chaos_test` output
