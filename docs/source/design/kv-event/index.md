# KV Events

[中文](../../zh/design/kv-event/index.md)

KV Events tell Conductor where key-value (KV) cache blocks are currently
available. In this repository, vLLM reports GPU blocks owned by one inference
engine and rank, while Mooncake reports objects available from a shared CPU or
Disk pool. Use this page to choose the guide for the side you are configuring.

## Choose an event source

| Source | What it reports | Where to continue |
|---|---|---|
| vLLM | GPU cache for one registered engine and data-parallel (DP) rank. | Use the [Conductor subscriber guide](./subscriber-guide.md) to see the accepted vLLM message fields. |
| Mooncake Master | CPU or Disk objects shared by compatible registered engines. | Use the [Mooncake publisher guide](./publisher-design.md) to enable and inspect the publisher. |

The registered source `type`, not the ZeroMQ (ZMQ) topic text, decides how
Conductor reads a message. The subscriber guide gives the detailed comparison
between `vLLM` and `Mooncake`.

## Publish Mooncake events

The [Mooncake KV Event publisher guide](./publisher-design.md) covers the build
option, Master settings, status counters, exact three-frame messages,
connector-key parsing, and what happens when events are missed.

## Connect Conductor

The [Conductor KV Event subscriber guide](./subscriber-guide.md) explains what
each registered source type may send, how hashes become lookup values, which
events change or clear cache information, and how unregister cleanup works.

For a complete startup sequence with registration and HTTP checks, follow the
[Conductor usage guide](../conductor/usage.md).

```{toctree}
:maxdepth: 1
:hidden:

publisher-design
subscriber-guide
```
