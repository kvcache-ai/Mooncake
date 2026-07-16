# Mooncake Store Agent Instructions

## Random Number Usage

- Production code under `mooncake-store/include/` and `mooncake-store/src/`
  must use the shared random facilities in `include/random.h`. Do not create
  local pseudo-random engines, seed sources, or distributions at call sites.
- If `include/random.h` does not support a required random operation, extend
  that interface and add focused tests instead of introducing another random
  implementation.
- Keep bounded sampling unbiased and validate invalid bounds in the shared
  implementation.
- Tests and benchmarks may use explicitly seeded engines when deterministic
  replay is required, but should use the sampling helpers from `random.h` when
  applicable.
- Treat `src/cachelib_memory_allocator/` as separately maintained code. Do not
  migrate its random implementation without an explicit request covering that
  subtree.
- The shared engine is not cryptographically secure. Do not use it for secrets,
  authentication tokens, or other security-sensitive values; extend the common
  interface with appropriate semantics when such a use case is required.
