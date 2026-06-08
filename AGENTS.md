# Mooncake Agent Instructions

## Project Overview

Mooncake is a KVCache-centric disaggregated architecture for LLM serving. It is the
serving platform for Kimi (Moonshot AI). The repo is a C++/Python hybrid with multiple
sub-modules: transfer-engine, store, ep, pg, p2p-store, wheel, integration, rl.

## Repository Structure

- `mooncake-transfer-engine/` — Core RDMA/TCP transfer engine (C++)
- `mooncake-store/` — Distributed KVCache store (C++)
- `mooncake-ep/` — Expert parallelism support (C++/Python)
- `mooncake-pg/` — PyTorch process group backend (C++/Python)
- `mooncake-p2p-store/` — Peer-to-peer checkpoint store (C++)
- `mooncake-wheel/` — Python wheel packaging and Python API
- `mooncake-integration/` — Integration with inference engines (vLLM, SGLang)
- `mooncake-common/` — Shared utilities and config parsing
- `scripts/` — Test scripts, build scripts, benchmarks
- `docs/` — Sphinx documentation

## Development Workflow

### Building

```bash
# Install dependencies
bash dependencies.sh
# Build with CMake
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Python Wheel

```bash
cd mooncake-wheel
pip install -e .
```

### Pre-commit Hooks (required before any commit)

```bash
pip install -r requirements-dev.txt
pre-commit install
pre-commit run --all-files
```

### Code Formatting

```bash
./scripts/code_format.sh
```

### Running Tests

```bash
# Local CI validation (recommended before PRs)
bash scripts/run_ci_test.sh

# Individual test scripts
python scripts/test_tensor_api.py
python scripts/test_copy_move_api.py
```

## PR Conventions

### Title Prefixes (required)

- `[Bugfix]` — Bug fixes
- `[CI/Build]` — CI or build improvements
- `[Doc]` — Documentation
- `[Integration]` — mooncake-integration changes
- `[P2PStore]` — mooncake-p2p-store changes
- `[Store]` — mooncake-store changes
- `[TransferEngine]` or `[TE]` — mooncake-transfer-engine changes
- `[PG]` — mooncake-pg changes
- `[EP]` — mooncake-ep changes
- `[Wheel]` — mooncake-wheel changes
- `[Common]` — mooncake-common changes
- `[Misc]` — Other

### RFC Required

Architectural changes >500 LOC (excluding tests) require a GitHub issue (RFC) first.

## Code Style

- Python: Google Python style guide, enforced by ruff
- C/C++: Google C++ style guide, enforced by clang-format
- CMake: cmake-format

## AI Contribution Policy

- AI-assisted PRs are welcome but the human submitter must understand and be able to
  defend every change end-to-end.
- Include `Co-Authored-By` trailer for AI-assisted commits.
- Do not submit PRs that are purely AI-generated boilerplate, trivial refactors, or
  cosmetic changes without functional value.

## Module Ownership

See `.github/CODEOWNERS` for per-module maintainers. Tag the relevant owner when
your PR touches their module.

## Key Contacts

- LLM ecosystem integration: @stmatengss
- SGLang integration: @ShangmingCai
- Transfer Engine: @alogfans
- Store: @ykwd
- EP/PG: @UNIDY2002
