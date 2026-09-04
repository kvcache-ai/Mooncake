---
name: write-mooncake-test
description: Guide for writing tests for Mooncake components. Use when asked to write, add, or create tests for any Mooncake module. Covers test patterns for Store, Transfer Engine, EP, PG, and Wheel components.
---

# Writing Mooncake Tests

## Test Locations

- **C++ unit tests:** `mooncake-*/tests/` (GoogleTest)
- **Python API tests:** `scripts/test_*.py` and `mooncake-wheel/tests/test_*.py`
- **Integration tests:** `scripts/tone_tests/` (end-to-end with real services)

## Python Test Patterns

### Store API Tests

```python
import torch
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

def test_store_put_get():
    """Test basic put/get roundtrip."""
    store = MooncakeDistributedStore()
    store.setup(
        local_hostname="127.0.0.1",
        metadata_server="etcd://127.0.0.1:2379/mooncake",
        global_segment_size="1GB",
        local_buffer_size="1GB",
    )

    key = "test_key"
    tensor = torch.randn(1024, dtype=torch.float32)
    store.put(key, [tensor])

    result = store.get(key)
    assert torch.equal(result[0], tensor)
    store.remove(key)
    store.teardown()
```

### Transfer Engine Tests

Transfer Engine tests typically require RDMA hardware or TCP fallback:

```python
def test_transfer_engine_tcp():
    """Test TCP transport (works without RDMA hardware)."""
    import os
    os.environ["MC_FORCE_TCP"] = "true"
    # ... test transfer operations
```

## C++ Test Patterns

Use GoogleTest. Test files live alongside source in `mooncake-*/tests/`:

```cpp
#include <gtest/gtest.h>
#include "mooncake/component.h"

TEST(ComponentTest, BasicOperation) {
    auto component = CreateComponent();
    ASSERT_NE(component, nullptr);
    EXPECT_EQ(component->DoWork(), Status::OK);
}
```

## Test Requirements

1. Tests MUST be runnable without RDMA hardware (use TCP transport fallback)
2. Tests MUST clean up resources (teardown stores, free memory)
3. Tests MUST NOT hardcode IP addresses (use 127.0.0.1 or parameterize)
4. Tests MUST include both success and error cases
5. Integration tests go in `scripts/tone_tests/` with a shell wrapper

## Running Tests

```bash
# All Python tests
bash scripts/run_ci_test.sh

# Specific test
python scripts/test_tensor_api.py

# C++ tests (after build)
cd build && ctest --output-on-failure
```
