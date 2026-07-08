# Ollama connector for Mooncake Store

`package ollama` lets an [Ollama](https://github.com/ollama/ollama) runtime
offload and reuse KV cache through Mooncake Store, so Ollama instances can join a
prefill/decode-disaggregated (PD) or shared-KV deployment. It builds on the Go
store bindings in `../mooncakestore` and follows the same hash-based
prefix-caching pattern as the vLLM `MooncakeStoreConnector` and SGLang HiCache
integrations.

## Layout

| File | Purpose |
|------|---------|
| `prefix/` | Pure-Go, content-addressed page keying (hash chain over token pages). No CGo; unit-tested standalone. |
| `config.go` | `Role`, `PageLayout`, and `Config`. |
| `connector.go` | `Connector`: `LoadCachedPrefix` (reuse) and `StoreComputedPrefix` (publish), plus buffer registration. |
| `../examples/ollama` | Runnable end-to-end demo over the TCP transport. |

## How it works

For each request the runtime provides the token ids and one buffer pointer per
KV page (slices of a registered staging region):

- **Prefill / publish** — `StoreComputedPrefix(tokens, pageSrc)` writes the
  freshly computed pages to the store, skipping any a peer already published.
- **Decode / reuse** — `LoadCachedPrefix(tokens, pageDst)` loads the longest
  cached prefix into the page buffers and returns how many leading tokens were
  served, so the runtime only recomputes the suffix.

Page keys form a hash chain over fixed-size token pages
(`key[i] = H(key[i-1] || tokens[page i])`, seeded with the model name). Shared
token prefixes therefore map to identical keys across workers, while any earlier
divergence changes every later key — a decode worker can never load KV that does
not match its own tokens.

## Build

Like the rest of the Go bindings, this package uses CGo and links against the
compiled `mooncake_store` / `transfer_engine` libraries. Build Mooncake first,
then set `CGO_CFLAGS` / `CGO_LDFLAGS` as in `../build.sh` and the CI workflow.

The `prefix` subpackage has no CGo dependency and can be tested directly:

```bash
go test ./ollama/prefix/...
```

## Status

Store-transfer and keying are implemented here. Wiring the page buffers to
llama.cpp's KV tensors (extraction/injection) is Ollama-runtime work tracked
separately. Zero-copy RDMA paths need the full build and RDMA hardware; the TCP
transport is enough for functional testing.
