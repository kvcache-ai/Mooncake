# Mooncake Conductor

Mooncake Conductor is the KV-cache indexer used by cache-aware routers. It
subscribes to KV cache events from vLLM, SGLang, and the Mooncake Store master,
maintains a global prefix cache table across device, host, and disk tiers, and
exposes HTTP and RPC APIs for dynamic service registration and cache-hit
queries.

See the [Rust client API](../../api-reference/rust/conductor.md).

:::{toctree}
:maxdepth: 1

conductor-architecture-design
publisher-design
subscriber-guide
:::
