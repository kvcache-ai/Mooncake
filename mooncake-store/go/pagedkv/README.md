# Paged-KV connector for Mooncake Store

`package pagedkv` lets an inference runtime that keeps its KV cache in fixed-size
pages offload and reuse that cache through Mooncake Store, so separate instances
share computed KV instead of each recomputing it. It builds on the Go store
bindings in `../mooncakestore` and follows the hash-based prefix-caching pattern
the vLLM `MooncakeStoreConnector` uses to share KV blocks across instances.

## Layout

| File | Purpose |
|------|---------|
| `prefix/` | Pure-Go, content-addressed page keying (hash chain over token pages). No CGo; unit-tested standalone. |
| `config.go` | `Role`, `PageLayout`, and `Config`. |
| `connector.go` | `Connector`: `LoadCachedPrefix` (reuse) and `StoreComputedPrefix` (publish), plus buffer registration. |
| `../examples/pagedkv` | Runnable end-to-end demo over the TCP transport. |

## How it works

For each request the runtime provides the token ids and one buffer pointer per
KV page (slices of a registered staging region):

- **Publish** — `StoreComputedPrefix(tokens, pageSrc)` writes the freshly
  computed pages to the store, skipping any a peer already published.
- **Reuse** — `LoadCachedPrefix(tokens, pageDst)` loads the longest cached prefix
  into the page buffers and returns how many leading tokens were served, so the
  runtime only recomputes the suffix.

Page keys form a hash chain over fixed-size token pages
(`key[i] = H(key[i-1] || tokens[page i])`, seeded with the model name). Shared
token prefixes therefore map to identical keys across workers, while any earlier
divergence changes every later key — a worker can never load KV that does not
match its own tokens.

## What a runtime has to provide

This package is the store-transfer and keying half only. Moving bytes between
these page buffers and the runtime's own KV tensors is the runtime's job and is
not provided here. A runtime can drive this package when it

- keeps KV cache in fixed-size token pages,
- can expose each page as a pointer into memory registered via
  `RegisterKVBuffer`, and
- derives page identity from stable token ids.

## Build

Like the rest of the Go bindings, this package uses CGo and links against the
compiled `mooncake_store` / `transfer_engine` libraries. Build Mooncake first,
then set `CGO_CFLAGS` / `CGO_LDFLAGS` as in `../build.sh` and the CI workflow.

The `prefix` subpackage has no CGo dependency and can be tested directly:

```bash
go test ./pagedkv/prefix/...
```

Zero-copy RDMA paths need the full build and RDMA hardware; the TCP transport is
enough for functional testing.
