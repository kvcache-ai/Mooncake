# Mooncake Developer Guide

This guide helps new contributors (human and AI) get started with Mooncake development.

## Prerequisites

- Linux (Ubuntu 22.04+ recommended)
- Python 3.10+
- CMake 3.20+
- C++17 compiler (GCC 11+ or Clang 14+)
- For RDMA testing: Mellanox OFED or equivalent

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/kvcache-ai/Mooncake.git
cd Mooncake
pip install -r requirements-dev.txt
pre-commit install
```

### 2. Build

```bash
bash dependencies.sh
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### 3. Install Python Package (development mode)

```bash
cd mooncake-wheel
pip install -e .
```

### 4. Verify Installation

```bash
bash scripts/test_installation.sh
```

## Architecture Overview

Mooncake has a layered architecture:

```
┌──────────────────────────────────────────┐
│           Applications (Kimi, etc.)       │
├──────────────────────────────────────────┤
│  Integrations (vLLM, SGLang, LMDeploy)   │
├──────────────────────────────────────────┤
│    Python API (mooncake-wheel)            │
├────────────┬─────────────┬───────────────┤
│   Store    │     EP      │      PG       │
│ (KVCache)  │  (Expert    │  (PyTorch     │
│            │  Parallel)  │   Backend)    │
├────────────┴─────────────┴───────────────┤
│       Transfer Engine (RDMA/TCP)          │
├──────────────────────────────────────────┤
│    Transport Plugins                      │
│  (RDMA, TCP, EFA, Ascend, HIP, CXL)     │
└──────────────────────────────────────────┘
```

### Module Responsibilities

- **Transfer Engine:** Low-level data movement. Manages memory registration,
  transport selection, and async transfers. This is the foundation everything
  else builds on.

- **Store:** Distributed KVCache with replication. Builds on Transfer Engine
  for cross-node data movement. Supports hierarchical caching (GPU → CPU → SSD → remote).

- **EP (Expert Parallelism):** Routes tokens to expert GPUs in MoE models.
  Optimized for all-to-all communication patterns.

- **PG (Process Group):** PyTorch distributed backend. Implements
  `torch.distributed` operations using Transfer Engine.

- **Wheel:** Python packaging and user-facing API. Wraps C++ implementations
  with Pythonic interfaces.

## Development Workflow

### Branch Strategy

- `main` — stable, CI-gated
- Feature branches — `feat/description` or `fix/description`
- PRs to `main` require CI pass + code owner approval

### Making Changes

1. Create a branch: `git checkout -b feat/my-feature`
2. Make changes following the style guides (see CONTRIBUTING.md)
3. Format: `./scripts/code_format.sh`
4. Test locally: `bash scripts/run_ci_test.sh`
5. Pre-commit: `pre-commit run --all-files`
6. Push and create PR with appropriate title prefix

### CI Labels

- `run-ci` — triggers full CI on PR (auto-added by labeler)
- `run-e2e-ci` — triggers end-to-end tests (including Ascend, integration)

## Common Tasks

### Adding a New Transport Plugin

1. Create directory: `mooncake-transfer-engine/src/transport/my_transport/`
2. Implement the `Transport` interface (see existing transports for reference)
3. Register in CMakeLists.txt with a build option
4. Add tests in `mooncake-transfer-engine/tests/`
5. Document in `docs/source/`

### Adding a New Python API

1. Implement in `mooncake-wheel/mooncake/`
2. Export in `__init__.py`
3. Add test in `mooncake-wheel/tests/`
4. Add usage example in `scripts/`
5. Document in `docs/source/python-api-reference/`

### Adding a New Integration

1. Add connector in `mooncake-integration/`
2. Add integration test in `scripts/tone_tests/`
3. Document in `docs/source/getting_started/examples/`
