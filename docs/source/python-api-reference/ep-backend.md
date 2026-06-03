# Mooncake EP & Mooncake Backend

## Overview

Mooncake EP is an adaption of [DeepEP](https://github.com/deepseek-ai/DeepEP) that supports **fault tolerance** and fast data transfer with **IBGDA**, designed as a critical component for large-scale, latency-sensitive MoE (Mixture of Experts) inference. Mooncake EP aims to retain full compatibility with the DeepEP API, with the addition of an `active_ranks` tensor passed to both the `dispatch` and `combine` functions to capture information about rank activeness. By integrating with the EPLB module, Mooncake EP ensures fault tolerance during MoE inference, enabling robust performance even in large-scale, fault-prone environments.

Mooncake Backend is a PyTorch distributed backend (a replacement for NCCL and Gloo) that provides **fault-tolerant collective communication primitives** and can be seamlessly integrated into machine learning systems. Built with the [Transfer Engine](../design/transfer-engine/index.md), Mooncake Backend ensures that collective communications can continue even in the event of rank failures. Furthermore, it reports these failures to the upper layers of the system, allowing for graceful error handling without disrupting ongoing operations.

## Usage

### Mooncake EP

> **Note:** Mooncake EP currently supports only the low-latency transfer mode.

The API is largely consistent with DeepEP's, with only minor differences in a few parameters. Mooncake EP exposes a `Buffer` that can be imported from `mooncake.mooncake_ep_buffer`. For example, refer to `mooncake-wheel/tests/test_mooncake_ep.py`.

#### Buffer.get_buffer_size_hint()

**Signature:**

```python
@staticmethod
def get_ep_buffer_size_hint(num_max_dispatch_tokens_per_rank: int, hidden: int, num_ranks: int, num_experts: int) -> int
```

Calculates the number of bytes to pre-allocate for data transfer.

#### Buffer.\_\_init\_\_()

**Signature:**

```python
def __init__(self, group: dist.ProcessGroup, num_ep_buffer_bytes: int = 0)
```

The constructor. Ensure that only one instance is created.

- **group**: Must be a Mooncake Backend process group.
- **num_ep_buffer_bytes**: The number of bytes acquired with `Buffer.get_buffer_size_hint()`

#### Buffer.dispatch/Buffer.combine

**Signature:** Similar to DeepEP's `low_latency_dispatch`/`low_latency_combine`, with two additional parameters:

- **active_ranks**: A tensor of shape `(num_ranks,)` containing values of 0 or 1. The indices of the broken ranks will be set to 0.
- **timeout_us**: The timeout in microseconds for a rank to be considered broken. Set to -1 for infinite timeout.

### Mooncake Backend

Basic usage:

```python
import torch
import torch.distributed as dist
from mooncake import pg

active_ranks = torch.ones((world_size,), dtype=torch.int32, device="cuda")
dist.init_process_group(
    backend="mooncake",
    rank=rank,
    world_size=world_size,
    pg_options=pg.MooncakeBackendOptions(active_ranks),
)

dist.all_gather(...)           # Standard API usage
assert active_ranks.all()  # Verify that no ranks are broken
```

For a full example, see `mooncake-wheel/tests/test_mooncake_backend.py`.

---

Recover usage (e.g., wants to recover rank #2):

```python
# For the healthy processes, execute:
import torch
import torch.distributed as dist
from mooncake import pg

...

broken_rank = 2
backend = dist.group.WORLD._get_backend(torch.device("cpu"))
while True:
    (peer_state,) = pg.get_peer_state(backend, [broken_rank])
    if peer_state:
        pg.recover_ranks(backend, [broken_rank])
        break
    else:
        # Handle ongoing logic, like inference
        pass

# For the new process, execute:
dist.init_process_group(
    backend="mooncake-cpu",
    rank=broken_rank,
    world_size=num_processes,
    pg_options=pg.MooncakeBackendOptions(
        torch.ones((num_processes,), dtype=torch.int32),
        is_extension=True,  # Must set this option to True
    ),
)
```

For a full example, see `mooncake-wheel/tests/test_mooncake_backend_elastic.py`.

