# Reinforcement Learning Training Integration

`mooncake-rl` demonstrates how to use `MooncakeDistributedStore` as a zero-copy data bus between **rollout engines** (inference workers) and **training engines** (gradient workers) in a distributed reinforcement-learning setup.

The key idea is to decouple the rollout and training phases: rollout engines write experience batches into Mooncake Store under a well-known key, and training engines fetch those batches concurrently — without any explicit synchronisation barrier or shared memory.

## Motivation

In a typical RL-from-human-feedback (RLHF) or online RL pipeline:

```
 Rollout Engines                    Training Engines
 (inference, e.g. SGLang/vLLM)      (gradient step, e.g. PyTorch)
 ┌──────────────────┐                ┌─────────────────────┐
 │  generate()      │ ─── RDMA ───▶  │  train(key)         │
 │  put_tensor(key) │                │  get_tensor(key)    │
 └──────────────────┘                └─────────────────────┘
```

Data flows over RDMA (or TCP fallback) via `MooncakeDistributedStore`, bypassing the CPU for GPU-to-GPU transfers and avoiding Python serialisation overhead.

## Example: `rl_samples.py`

The reference implementation lives in [`mooncake-rl/examples/rl_samples.py`](../../../../mooncake-rl/examples/rl_samples.py). It provides a minimal, runnable mock of the full RL loop:

| Class | Role |
|-------|------|
| `RolloutEngine` | Generates random `(obs, action, reward)` samples |
| `RolloutController` | Manages dataset state and connects to Mooncake Store |
| `RolloutManager` | Orchestrates rollout engines; writes samples to the Store |
| `TrainActor` | Consumes samples and performs a dummy training step |
| `TrainGroup` | Coordinates training actors; reads samples from the Store |

### Data Flow Walk-Through

1. **RolloutManager.generate(rollout_id)** — each rollout engine produces a sample dict `{obs, action, reward}`. All samples for this rollout step are assembled into a list and written into the Store:

   ```python
   # RolloutController uses an RDMA-initialised Store client
   self.rollout_client.put_tensor(str(rollout_id), rollout_samples)
   ```

2. **TrainGroup.train(rollout_id, key)** — the training side fetches those samples and distributes them across training actors:

   ```python
   samples = self.training_client.get_tensor(rollout_key)
   for actor, sample in zip(self.actor_handlers, samples):
       actor.train(sample)
   ```

3. **Training loop** — the top-level `train()` function alternates generation, training, evaluation, and checkpoint saving:

   ```python
   for rollout_id in range(start, num_rollout):
       key = rollout_manager.generate(rollout_id)  # write to Store
       actor_model.train(rollout_id, key)           # read from Store
       actor_model.update_weights()                 # sync weights to rollout
   ```

## Running the Example

### Prerequisites

- A running Mooncake metadata server (e.g., the HTTP server or etcd).
- A running `mooncake_master` (for distributed Store coordination).
- Mooncake Python wheel installed (`pip install mooncake-transfer-engine`).

### Start the Metadata Server

```bash
cd mooncake-transfer-engine/example/http-metadata-server-python
pip install aiohttp
python bootstrap_server.py &
```

### Start the Master

```bash
./build/mooncake_master \
    --rpc_port=50051 \
    --enable_http_metadata_server=true \
    --http_metadata_server_host=0.0.0.0 \
    --http_metadata_server_port=8080
```

### Run the RL Example

```bash
python mooncake-rl/examples/rl_samples.py \
    --num_rollout=10 \
    --num_train_actor=2 \
    --num_rollout_actor=2 \
    --save_interval=5 \
    --eval_interval=5 \
    --model_path=./checkpoints
```

## Connecting to a Real Metadata Server

The example initialises two `MooncakeDistributedStore` clients — one on the rollout side and one on the training side — with different local hostnames and NIC device names:

```python
# Rollout client (on rollout node / process)
self.rollout_client = MooncakeDistributedStore()
self.rollout_client.setup(
    "localhost:12346",                     # local_hostname:port
    "http://localhost:8080/metadata",      # metadata_server URL
    512 * 1024 * 1024,                     # global_segment_size (512 MB)
    128 * 1024 * 1024,                     # local_buffer_size (128 MB)
    "rdma",                                # protocol
    "erdma_0",                             # NIC device (e.g. mlx5_0)
    "localhost:50051",                     # mooncake_master address
)

# Training client (on training node / process)
self.training_client = MooncakeDistributedStore()
self.training_client.setup(
    "localhost:12345",
    "http://localhost:8080/metadata",
    512 * 1024 * 1024,
    128 * 1024 * 1024,
    "rdma",
    "erdma_1",
    "localhost:50051",
)
```

In a real deployment:
- Replace `localhost` with actual hostnames or IPs.
- Replace `erdma_0` / `erdma_1` with the NIC names from `ibv_devices`.
- Use separate metadata server and master addresses accessible by both sets of nodes.

## Key/Value Conventions

By default, the example uses the rollout ID as the string key:

```python
key = str(rollout_id)               # e.g. "0", "1", "42"
self.rollout_client.put_tensor(key, rollout_samples)
```

In production, use a structured key scheme to avoid collisions across concurrent runs:

```python
key = f"run:{run_id}/rollout:{rollout_id}"
```

## Integration with Real RL Frameworks

This pattern is directly applicable to frameworks such as [THUDM/slime](https://github.com/THUDM/slime), [veRL](https://github.com/volcengine/verl), and [OpenRLHF](https://github.com/OpenRLHF/OpenRLHF). Replace the dummy `generate()` and `train()` implementations with real policy inference (SGLang / vLLM) and gradient update (PyTorch FSDP / DeepSpeed) calls, keeping the `put_tensor` / `get_tensor` calls as the data handoff point.

## See Also

- [MooncakeDistributedStore Python API](../../python-api-reference/mooncake-store)
- [Mooncake Store Deployment Guide](../../deployment/mooncake-store-deployment-guide)
- [SSD Offload](../../deployment/ssd-offload)
