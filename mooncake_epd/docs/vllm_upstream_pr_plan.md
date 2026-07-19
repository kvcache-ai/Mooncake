# vLLM Upstream Hook Proposal

Mooncake EPD currently uses a version-probed compatibility adapter for vLLM
`0.23.x`. The adapter is deliberately optional and fail-closed in strict mode.
Its monkeypatches should be replaced by the following small upstream extension
points rather than carried indefinitely.

## 1. Request-scoped precomputed multimodal embeddings

Add a validated request field for external multimodal embeddings/hidden-state
handles. vLLM should verify model revision, processor revision, tensor schema,
dtype, shape and checksum before the model runner consumes it. Qwen-VL models
can then skip only the relevant vision-tower invocation without an external
wrapper around private `embed_multimodal` methods.

## 2. Prompt-only Prefill completion

Expose a supported prompt-only prefill mode that emits KV transfer metadata
after prompt forward and before token sampling. This removes the need to patch
`SamplingParams(max_tokens=0)` and engine completion behavior.

## 3. Worker-owned external buffer service hook

Allow an API-server plugin to register internal, authenticated worker-owned
allocation routes. The hook must be off by default, preserve vLLM's public API
surface, and bind allocation ownership to worker generation plus request lease.

## 4. KV retention/export hook

Expose connector callbacks around block lifetime: `retain_for_export`,
`export_committed`, and `release_after_consumer_ack`. Agent state capture needs
real allocator/prefix-cache refs, not copied block IDs in an external control
plane.

## Compatibility Contract

Until upstream hooks exist, Mooncake records the vLLM version, Python version,
target methods and capability report in every strict demo artifact. An unknown
minor version must fail before serving starts. No code path may modify
`site-packages`; generated EPD workers invoke
`python -m mooncake_epd.integrations.vllm.launcher -- vllm serve ...`.
