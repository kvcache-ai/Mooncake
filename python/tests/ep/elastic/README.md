# Elastic EP parity tests

This directory ports the fixed elastic EP suite from [NIXL commit `1f4afe6b60b2b5114deff13a4d2653e7ba683f07`](https://github.com/ai-dynamo/nixl/tree/1f4afe6b60b2b5114deff13a4d2653e7ba683f07/examples/device/ep/tests/elastic), relative source path `examples/device/ep/tests/elastic`. The original `elastic.py`, `plan.py`, `rank_server.py`, `store_group.py`, `test_utils.py`, and seven JSON plans are retained here with their DeepSeek/NVIDIA notices. `LICENSE-DeepEP` accompanies the MIT-licensed DeepSeek material.

`elastic.py` keeps the reference correctness checks. The adapters translate calls to each backend's public API. `backend.py` imports the selected adapter lazily, so a Mooncake run does not import or link the NIXL extension. `launch_elastic.py` starts only workers for these seven plans, schedules joiners and replacements, and puts ranks at the same test-only phase barriers. It does not implement runtime recovery.

## Fixed plans

The seven JSON files are copied byte-for-byte from the reference:

| Plan | Rank sequence |
| --- | --- |
| `single_rank.json` | `[0]` |
| `no_expansion.json` | `[0,1,2,3]` |
| `single_expansion.json` | `[0,1,2,3]` → `[0,1,2,3,4,5,6,7]` |
| `double_expansion.json` | `[0,1,2,3]` → `[0,1,2,3,4,5]` → `[0,1,2,3,4,5,6,7]` |
| `single_expansion_16_ranks.json` | `[0,1,2,3,4,5,6,7]` → `[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]` |
| `expansion_contraction.json` | 4 → 8 → `[0,1,2,3,4,-6,7]` → 8 ranks |
| `expansion_fault_contraction.json` | `[0,1]` → `[0,1,2,3]` → `[0,1,-2,3]` → `[0,3]` |

Negative rank IDs retain the reference meaning: that worker receives the planned SIGTERM fault during that phase. The original 100 microsecond `threading.Timer` still sends SIGTERM during the first dispatch. The controller catches its own SIGTERM and KeyboardInterrupt to run targeted worker cleanup. For a planned fault, the launcher accepts local status `-15` or `143`; the fixed remote `setsid --wait` wrapper reports signal 15 as exit status `15`. It accepts that remote status only after the worker registered the matching planned-fault marker. Phase-failure validation is always enabled. Clean removals must exit zero.

The test retains checks for expert layout and per-expert counts, `cumulative_local_expert_recv_stats`, source-rank routing, received payloads, NaN-free output, survivor-only weighted contributions, and `calc_diff` thresholds (`1e-5` for BF16 and `9e-4` for standard FP8). It runs both `return_recv_hook` settings, BF16 and standard FP8 dispatch, both `zero_copy` settings, and the caller-provided `out` tensor. LogFMT remains disabled as in the reference worker. The default legal shape is `num_experts_per_rank=8`, `num_topk=8`; this keeps single-rank and two-rank plans valid.

## Format sets

`--format-set common` is the default for both backends. It runs shared BF16 and standard FP8 cases. Mooncake emits explicit `P1_FORMAT_N/A` lines for NIXL-only `round_scale=True` and `use_ue8m0=True`; its adapter raises if either option is accidentally passed. It does not translate those modes to BF16 or silently discard them.

The original NIXL-only extra FP8 cases remain available with `--backend nixl --format-set nixl-full`. This adds `round_scale=True` with both `use_ue8m0=False` and `True`. A `common` run does not cover the full NIXL format set.

## Running

Activate `myenv` before launching. Set `CUDA_VISIBLE_DEVICES` to the chosen devices, or pass explicit per-host IDs with `--gpu-lists`. Ranks are assigned to hosts in a stable round-robin order: global rank `r` uses `worker_hosts[r % host_count]`; each worker selects `local_rank` from that host's visible GPU list. The launcher checks the mapping and records the worker OS PID, hostname, local rank, selected GPU, and task process-group ID.

From the Mooncake repository root:

```bash
conda activate myenv
test "$CONDA_DEFAULT_ENV" = myenv
"$CONDA_PREFIX/bin/python" -V
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:?set the visible GPU IDs first}"
export MOONCAKE_EP_DEVICE_FILTER=mlx5_1,mlx5_2,mlx5_3,mlx5_4
ELASTIC_EP_GPU_LISTS="$CUDA_VISIBLE_DEVICES" \
  "$CONDA_PREFIX/bin/python" python/tests/ep/elastic/launch_elastic.py \
  --backend mooncake \
  --format-set common \
  --plan single_rank.json \
  --worker-hosts local
```

Run the launcher control checks on CPU with no visible GPU:

```bash
CUDA_VISIBLE_DEVICES="" "$CONDA_PREFIX/bin/python" python/tests/ep/elastic/test_launcher_cpu.py
```

For two hosts, give one SSH target and one comma-separated GPU list per host, in the same order. For example, `--worker-hosts local,host-b --gpu-lists '0,1;2,3'`. A four-rank plan then places ranks 0 and 2 on the first host and ranks 1 and 3 on the second. Set `--advertise-address` to an address reachable from every worker. SSH access must already be configured. Remote workers activate `myenv` using `--conda-sh /path/to/conda.sh` or `ELASTIC_EP_CONDA_SH`; this path is configurable and is not assumed by the launcher.

The launcher keeps the caller's `PYTHONPATH` first. To use a source checkout explicitly, set `--worker-pythonpath`; an installed runtime remains the default. The NIXL adapter imports only when selected. Supply an installed or built NIXL Python path with `--nixl-pythonpath` or `NIXL_EP_PYTHONPATH`; the launcher does not prepend an unbuilt reference tree.

The default log root is `../artifacts/p1/launch-runs` relative to the repository. Override it with `--artifact-dir` or `ELASTIC_EP_ARTIFACT_DIR`. Multi-host runs need this directory on shared storage so the launcher can verify each remote worker's PID record. Builds and other test outputs belong in `build/p1/` and `artifacts/p1/`; P0 outputs remain separate.

## Scope

Only this test directory is part of the migration. Its public launcher runs CUDA workers with the preinstalled Mooncake native extension. The Mooncake backend does not import NIXL. The optional NIXL backend runs the fixed reference only when NIXL is separately installed; no NIXL source or binary is a Mooncake product dependency. The CPU launcher check is a separate control-only test.


## Manual native connection contract (three terminals)
Run each terminal command from the Mooncake repository root.

This test needs three real CUDA workers and the installed Mooncake EP native extension. Use one Store address reachable from both nodes, the same free TCP port, and the same unique RUN_ID in every terminal. The TCPStore carries test coordination and peer metadata only; EP payload traffic uses the native transport. Set the device filter to the cluster's allowed NICs on all three workers.
Replace the example Node A address with its reachable IP, choose a free port instead of 29501 if needed, and use a fresh run ID for each run. Keep all three values identical in every terminal.

On Node A, start rank 0 on physical GPU 0:

~~~bash
conda activate myenv
export STORE_HOST="<Node-A-reachable-IP>"
export STORE_PORT=29501
export RUN_ID="connection-contract-unique-id"
export CUDA_VISIBLE_DEVICES=0
export MOONCAKE_EP_DEVICE_FILTER=mlx5_1,mlx5_2,mlx5_3,mlx5_4
python python/tests/ep/elastic/connection_contract_gpu.py \
  --rank 0 --local-gpu 0 --store-host "$STORE_HOST" --store-port "$STORE_PORT" --run-id "$RUN_ID"
~~~

On Node B, start rank 1 on physical GPU 0 with the same STORE_HOST, STORE_PORT, and RUN_ID:

~~~bash
conda activate myenv
export STORE_HOST="<Node-A-reachable-IP>"
export STORE_PORT=29501
export RUN_ID="connection-contract-unique-id"
export CUDA_VISIBLE_DEVICES=0
export MOONCAKE_EP_DEVICE_FILTER=mlx5_1,mlx5_2,mlx5_3,mlx5_4
python python/tests/ep/elastic/connection_contract_gpu.py \
  --rank 1 --local-gpu 0 --store-host "$STORE_HOST" --store-port "$STORE_PORT" --run-id "$RUN_ID"
~~~

Wait until both old-pair workers print P1_OLD_PAIR_READY. Then, on Node A, start rank 2 on physical GPU 1. It uses local CUDA index 0 because the command exposes only physical GPU 1:

~~~bash
conda activate myenv
export STORE_HOST="<Node-A-reachable-IP>"
export STORE_PORT=29501
export RUN_ID="connection-contract-unique-id"
export CUDA_VISIBLE_DEVICES=1
export MOONCAKE_EP_DEVICE_FILTER=mlx5_1,mlx5_2,mlx5_3,mlx5_4
python python/tests/ep/elastic/connection_contract_gpu.py \
  --rank 2 --local-gpu 0 --store-host "$STORE_HOST" --store-port "$STORE_PORT" --run-id "$RUN_ID"
~~~

The three processes check native prepare while the old pair is communicating, prepared disconnect/reconnect, explicit mask commit, three-rank dispatch/combine data and receive statistics, and a barrier timeout when one peer is absent. Keep all assertions enabled. This is a manual native GPU contract check, not a launcher or an expanded test matrix.
