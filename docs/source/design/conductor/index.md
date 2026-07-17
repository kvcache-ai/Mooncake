# Mooncake Conductor

[中文](../../zh/design/conductor/index.md)

Mooncake Conductor keeps an in-memory view of reusable key-value (KV) cache
blocks reported by inference engines and Mooncake Store. A router can query
that view before choosing an inference engine. Use this page to find the guide
for running Conductor, understanding its results, or integrating a client.

## Choose a task

### Run Conductor

Follow the [usage guide](./usage.md) to build the C++ service, configure event
sources, check registrations, query cache availability, and unregister a
source.

### Understand the result

Read the [architecture guide](./conductor-architecture-design.md) to see how
vLLM GPU events and shared Mooncake CPU or Disk events become per-engine query
results.

### Call the HTTP API

Use the [HTTP API reference](./indexer-api-design.md) for the five implemented
endpoints, their accepted fields, response bodies, and error formats.

### Connect event sources

Start with [KV Events](../kv-event/index.md) to compare the vLLM and Mooncake
message paths. That section also explains what Mooncake publishes and what
Conductor accepts.

```{toctree}
:maxdepth: 1
:hidden:

conductor-architecture-design
usage
```
