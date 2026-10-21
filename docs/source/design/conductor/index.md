# Mooncake Conductor

Mooncake Conductor is the KV-cache indexer used by cache-aware routers. It
subscribes to KV cache events from inference engines or storage backends,
normalizes those events, maintains a global prefix cache table, and exposes
HTTP APIs for dynamic service registration and cache-hit queries.

:::{toctree}
:maxdepth: 1

conductor-architecture-design
publisher-design
subscriber-guide
:::
