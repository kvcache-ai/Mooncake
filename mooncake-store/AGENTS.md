# Mooncake Store Instructions

## Random Numbers

- Production code in `include/` and `src/` must use `include/random.h`; do not
  create random engines, seeds, or distributions at call sites.
- Extend `random.h` and its tests when an operation is missing. Keep bounded
  sampling unbiased and reject invalid bounds.
- Tests and benchmarks may use explicit seeds for reproducibility, but should
  use the shared sampling helpers.
- Do not migrate `src/cachelib_memory_allocator/` unless explicitly requested.
- The shared engine is not cryptographically secure; do not use it for secrets
  or authentication tokens.

## Local-first Host Identity

- Keep the placement host ID separate from the routable transfer endpoint.
- `MOONCAKE_HOST_ID` takes priority after ASCII whitespace trimming. Unset,
  empty, or whitespace-only values fall back to the existing `local_hostname`
  behavior.
- Normalize endpoint-shaped host IDs before rejecting loopback or wildcard
  identities.
