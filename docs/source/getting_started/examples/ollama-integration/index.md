# Mooncake x Ollama Integration

## Overview

[Ollama](https://github.com/ollama/ollama) is a Go runtime for running local LLMs
on top of a `llama.cpp`/GGML backend. By default each Ollama instance keeps its
KV cache only in its own process, so a prompt prefix computed on one node cannot
be reused by another, and Ollama cannot participate in a
prefill/decode-disaggregated (PD) deployment.

This integration adds an Ollama-side **connector** that offloads and reuses KV
cache through **Mooncake Store**, following the same hash-based prefix-caching
pattern already used by the vLLM `MooncakeStoreConnector` and the SGLang HiCache
L3 backend:

- A **prefill** worker publishes the KV cache it computes for a prompt.
- A **decode** worker (or any peer) reuses the longest cached prefix from the
  store instead of recomputing it, cutting redundant prefill and lowering TTFT
  for workloads with shared prefixes — system prompts, few-shot templates, and
  multi-turn conversations.

The connector lives in the Go store bindings at
[`mooncake-store/go/ollama`](https://github.com/kvcache-ai/Mooncake/tree/main/mooncake-store/go/ollama)
and builds directly on the existing Go `mooncakestore` client, so it reuses the
same RDMA-capable, zero-copy `put`/`get` path as the other integrations.

| Scenario | Store API used | Role |
|----------|----------------|------|
| Publish computed KV (prefill) | `BatchPutFrom` (zero-copy) | `prefill` / `mixed` |
| Reuse cached prefix (decode) | `BatchExists` + `BatchGetInto` | `decode` / `mixed` |

## Architecture

### Page identity

KV cache is paged: a request's tokens are split into fixed-size pages of
`PageTokens` tokens (the trailing partial page is not cacheable). Each page is
addressed by a key that chains SHA-256 over the pages seen so far, seeded with
the model name:

```
seed    = H("mooncake-ollama-kv" || model)
hash[i] = H(hash[i-1] || tokens[page i])
key[i]  = "mc/ollama/<model>/<hex(hash[i])>"
```

Two properties fall out of the chain and matter for correctness:

- **Shared prefixes collide on purpose.** Requests that share a token prefix
  produce identical keys for the shared pages, so any worker can reuse them.
- **Divergence is total.** Changing any earlier token changes every later key,
  so a decode worker can never load KV that does not match its own tokens.

Folding the model name into the seed keeps different models disjoint even in a
shared store, and the `mc/ollama/` namespace keeps Ollama keys from colliding
with vLLM or SGLang keys against the same cluster.

### Workflow

```
                 ┌─────────────────────────── Mooncake Store ───────────────────────────┐
                 │            DRAM + SSD pool, addressed by page keys (RDMA/TCP)          │
                 └───────▲───────────────────────────────────────────────────▲───────────┘
                         │ BatchPutFrom (publish)                             │ BatchGetInto (reuse)
                         │                                                    │
             ┌───────────┴───────────┐                          ┌────────────┴──────────┐
             │   Ollama prefill node │                          │   Ollama decode node  │
             │  compute KV → publish │                          │  match prefix → load  │
             └───────────────────────┘                          └───────────────────────┘
```

- **Decode / reuse:** `LoadCachedPrefix(tokens, pageDst)` computes the page keys,
  probes the store with `BatchExists`, loads the longest present prefix with
  `BatchGetInto`, and reports how many leading tokens were served. The runtime
  recomputes only the suffix.
- **Prefill / publish:** `StoreComputedPrefix(tokens, pageSrc)` computes the same
  keys and publishes pages with `BatchPutFrom`, skipping any page a peer already
  stored (so concurrent workers de-duplicate rather than overwrite).

Because both sides derive keys from tokens alone, prefill and decode workers
agree on page identity without any extra coordination.

### Components

| File | Purpose |
|------|---------|
| `ollama/prefix/` | Pure-Go, content-addressed page keying. No CGo; unit-tested standalone. |
| `ollama/config.go` | `Role` (`prefill` / `decode` / `mixed`), `PageLayout`, `Config`. |
| `ollama/connector.go` | `Connector`: `LoadCachedPrefix`, `StoreComputedPrefix`, buffer registration. |
| `examples/ollama/` | Runnable end-to-end demo over the TCP transport. |

## Build

The connector uses CGo and links against the compiled Mooncake libraries, exactly
like the rest of the Go bindings. Build Mooncake first (see
[Build Guide](../../build)), then set `CGO_CFLAGS` / `CGO_LDFLAGS` as in
`mooncake-store/go/build.sh`.

The `prefix` subpackage has no CGo dependency and can be tested on its own:

```bash
cd mooncake-store/go
go test ./ollama/prefix/...
```

## Quick start

Start a master with the built-in HTTP metadata server:

```bash
mooncake_master --enable_http_metadata_server=true \
  --http_metadata_server_port=8080
```

Then run the example, which plays both roles in one process — it publishes a
prompt's KV pages, loads them back for a second request that shares the prefix,
and verifies the bytes round-trip exactly:

```bash
cd mooncake-store/go
# CGO_CFLAGS / CGO_LDFLAGS / LD_LIBRARY_PATH set as in build.sh
go run ./examples/ollama/
```

Expected output:

```
Connected to Mooncake Store
Published 4/4 pages for the prompt
Cache hit: 3 pages (24 tokens)
Round-trip verified: loaded KV matches published KV
Done!
```

## Using the connector from an Ollama build

```go
c, err := ollama.New(ollama.Config{
    LocalHostname:    "localhost",
    MetadataServer:   "http://localhost:8080/metadata",
    MasterServerAddr: "localhost:50051",
    Protocol:         "rdma",        // or "tcp"
    Role:             ollama.RolePrefill,
    Model:            "qwen2.5:7b",
    Layout: ollama.PageLayout{
        NumLayers:  28,
        NumKVHeads: 4,
        HeadDim:    128,
        PageTokens: 16,
        ElemSize:   2, // fp16
    },
})
// register the runtime's KV staging region once
c.RegisterKVBuffer(base, regionSize)

// decode side: reuse
res, _ := c.LoadCachedPrefix(tokens, pageDst) // recompute tokens[res.MatchedTokens:]

// prefill side: publish
c.StoreComputedPrefix(tokens, pageSrc)
```

`pageDst[i]` / `pageSrc[i]` point at page `i` inside the registered region; each
page is `PageLayout.PageBytes()` bytes.

## Current scope and roadmap

This PR provides the **store-transfer and page-keying half** of the integration:
connector, roles, prefix keying (with tests), an example, and this guide.

Two pieces are intentionally out of scope here and tracked as follow-ups:

- **llama.cpp KV wiring.** Extracting freshly computed KV from, and injecting
  loaded KV back into, `llama.cpp`'s paged KV tensors is Ollama-runtime work and
  belongs in the Ollama codebase. The connector defines the buffer contract
  (`PageLayout` + one pointer per page); the runtime fills/consumes those
  buffers.
- **Scheduler-level PD split.** Routing whole requests between dedicated prefill
  and decode pools is an Ollama scheduling concern layered on top of this
  connector.

## Hardware notes

The zero-copy `put`/`get` path reaches full bandwidth over RDMA (IB / RoCE) and
requires the complete Mooncake build plus RDMA-capable NICs. The **TCP transport**
needs no special hardware and is sufficient to validate functional correctness
(the round-trip check in the example runs over TCP).
