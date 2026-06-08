---
name: mooncake-mechanical-refactor
description: Guide for performing safe mechanical refactors in Mooncake. Use when renaming symbols, moving files, splitting modules, or restructuring code. Ensures refactors are verified and don't break the build or tests.
---

# Mechanical Refactoring in Mooncake

## Pre-Refactor Checklist

1. **Verify the build passes BEFORE starting:**
   ```bash
   cd build && cmake .. && make -j$(nproc)
   ```
2. **Run tests BEFORE starting:**
   ```bash
   bash scripts/run_ci_test.sh
   ```
3. **Commit the current state** so you have a clean rollback point.

## Renaming C++ Symbols

Mooncake is a multi-module C++ project. A rename in one module may affect others.

```bash
# Find all references (case-sensitive)
grep -rn "OldSymbolName" mooncake-*/

# Check header includes
grep -rn "#include.*old_file" mooncake-*/

# After renaming, verify build
cd build && make -j$(nproc) 2>&1 | head -50
```

## Renaming Python APIs

Python API changes affect the wheel and all downstream users.

```bash
# Find all Python references
grep -rn "old_function_name" mooncake-wheel/ scripts/ mooncake-integration/

# Check if the symbol is exported in __init__.py
grep -rn "old_function_name" mooncake-wheel/mooncake/__init__.py
```

## Moving Files Between Modules

1. Update `CMakeLists.txt` in both source and destination modules
2. Update all `#include` paths
3. Update CODEOWNERS if the file crosses ownership boundaries
4. Verify: `cd build && cmake .. && make -j$(nproc)`

## Post-Refactor Verification

1. **Build must pass:** `cd build && make -j$(nproc)`
2. **Format must pass:** `./scripts/code_format.sh`
3. **Tests must pass:** `bash scripts/run_ci_test.sh`
4. **Pre-commit must pass:** `pre-commit run --all-files`

## Commit Strategy

- One commit per logical rename/move (not one giant commit)
- Use prefix: `[Misc] refactor: rename X to Y`
- If the refactor is >500 LOC, file an RFC issue first
