# llm-d Integration

[llm-d](https://github.com/llm-d/llm-d) packages vLLM inference on Kubernetes; Mooncake is the KV layer beneath it. The integration is a **three-layer stack**:

- **Engine — vLLM.** The Mooncake connectors live here: `MooncakeStoreConnector` (offload) and `MooncakeConnector` (P/D transfer), driven by `--kv-transfer-config`, `mooncake_config.json`, and `PYTHONHASHSEED`.
- **Storage — Mooncake.** The Master/Client binaries, the Transfer Engine (RDMA), and the SSD tier.
- **Packaging — llm-d.** Kustomize overlays that wire the engine and storage into a Deployment + DaemonSet, plus a routing sidecar.

Mooncake plugs in at **two independent points**, both configured on the vLLM side:

- **KV cache offloading (storage)** — `MooncakeStoreConnector` uses a Mooncake Store pool as a shared offload tier. This is llm-d's primary Mooncake path.
- **P/D KV transfer** — the routing sidecar's `--kv-connector=mooncake` drives vLLM's `MooncakeConnector` (see [below](#pd-transfer)).

The two share the Transfer Engine but serve different purposes; vLLM's `MultiConnector` can compose them when a deployment needs both. For each layer, this page points at the authoritative source and keeps only what that source leaves for the integrator to reconcile.

## vLLM connector contract

The vLLM side of the integration is a single command. With the Mooncake Master already running and a `mooncake_config.json` on disk, `MooncakeStoreConnector` is wired on a single node with that config file and a connector spec:

```bash
MOONCAKE_CONFIG_PATH=mooncake_config.json \
vllm serve meta-llama/Llama-3.1-8B-Instruct \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_both"}'
```

The `mooncake_config.json` this command names has a `mode` field that selects the storage topology:

| Mode | Who owns the DRAM pool | `global_segment_size` | Components |
|---|---|---|---|
| `embedded` | each vLLM rank contributes DRAM in-process | **> 0** (e.g. `"80GB"`) | Master + vLLM |
| `standalone-store` | an external `mooncake_client` owns the CPU DRAM + SSD pool; vLLM ranks are pure requesters | **`0`** | Master + Client + vLLM |

The connector enforces the pairing at startup: `embedded` requires `global_segment_size > 0`, `standalone-store` requires `global_segment_size == 0`, and `local_buffer_size` must be `> 0` in both. `standalone-store` decouples the pool from vLLM — it survives vLLM restarts and can live on GPU-less nodes with large DRAM and NVMe. The SSD tier is a separate switch, not implied by the mode: `enable_offload` must be set together on the vLLM config, the Master, and the Client (vLLM even accepts `embedded` with `enable_offload`).

llm-d instantiates this same contract at scale: the `--kv-transfer-config`, the `MOONCAKE_CONFIG_PATH` JSON, and `PYTHONHASHSEED` set here are what the guide's `patch-vllm.yaml` sets on each model-server Pod. vLLM's docs are the authoritative field reference:

- [MooncakeStore connector usage](https://github.com/vllm-project/vllm/blob/main/docs/features/mooncake_store_connector_usage.md) — the `mooncake_config.json` schema (including the `mode` field), the single-node form above, and the `MultiConnector[MooncakeConnector + MooncakeStoreConnector]` composition for XpYd.
- [Mooncake connector usage](https://github.com/vllm-project/vllm/blob/main/docs/features/mooncake_connector_usage.md) — the P/D transfer connector and its bootstrap port (`8998`).

```{important}
Set `PYTHONHASHSEED` to the **same fixed value on every instance sharing a Store**. Mooncake keys blocks by a content hash derived from vLLM's block hashes, and Python randomises its hash seed per process by default — leave it unset and two instances compute different keys for identical tokens, so the pool stays healthy while the cross-instance hit rate sits at zero.
```

## KV cache offloading on K8s (llm-d)

llm-d's [Tiered Prefix Cache guide](https://github.com/llm-d/llm-d/tree/v0.8.0/guides/tiered-prefix-cache) packages MooncakeStore as an offload backend: evicted KV blocks move out of GPU HBM into a Mooncake Store pool that serves them back on a later hit, saving the recompute. Because the pool is shared, multiple vLLM instances reuse each other's cached prefixes. The guide carries the deployment sequence, node sizing, and manifests; deploy it with `helm install` for the router chart and `kubectl apply -k` over the Mooncake overlays.

```{note}
**Upstream sources** (llm-d `v0.8.0`):
- Guide: [Tiered Prefix Cache](https://github.com/llm-d/llm-d/tree/v0.8.0/guides/tiered-prefix-cache) — the deployment walkthrough; the MooncakeStore path has `cpu` and `fs` variants.
- Architecture: [KV Offloader](https://github.com/llm-d/llm-d/blob/v0.8.0/docs/architecture/advanced/kv-management/kv-offloader.md) — Master/Client roles and a `mooncake_config.json` field table.
- Overlays: [`helpers/mooncake-master-store/`](https://github.com/llm-d/llm-d/tree/v0.8.0/helpers/mooncake-master-store) (Master; both variants), [`modelserver/gpu/vllm/mooncake-store/`](https://github.com/llm-d/llm-d/tree/v0.8.0/guides/tiered-prefix-cache/modelserver/gpu/vllm/mooncake-store) (`cpu`/`fs` vLLM overlays), [`helpers/mooncake-client/`](https://github.com/llm-d/llm-d/tree/v0.8.0/helpers/mooncake-client) (Client DaemonSet base; `fs` patches it up).
```

```{note}
**Confirming Mooncake specifically.** The guide's offload verification inspects the decode Pod's `/mnt/files-storage/kv-cache`, which is the vLLM-native/LMCache filesystem tier — a Mooncake `fs` deployment writes nowhere near it, so an all-`Ready` cluster does not prove the Store is working. Confirm Mooncake on the `mooncake_client` Pod instead: its `/data/mooncake-offload` should grow after a long request, and the Master should show the pool registered (metrics on `:9003`).
```

(pd-transfer)=
## P/D KV transfer (MooncakeConnector)

The P/D transfer path is independent of the Store. vLLM ships a runnable single-node example for it — [`examples/disaggregated/mooncake_connector/`](https://github.com/vllm-project/vllm/tree/main/examples/disaggregated/mooncake_connector) (proxy + launch scripts, configurable model and bootstrap port). In a prefill/decode disaggregated deployment, llm-d's routing sidecar drives the same connector:

```bash
# on the decode pod's routing sidecar
--kv-connector=mooncake
```

The sidecar queries a bootstrap endpoint on the prefill pods to resolve the target engine, then dispatches prefill and decode so the decoder pulls KV directly from the prefiller.

| Setting | Default | Notes |
|---|---|---|
| `--kv-connector=mooncake` | — | Selects vLLM's `MooncakeConnector` on the sidecar. The router accepts only fixed connector names (`mooncake`, not `MultiConnector`), so the serving pods must run `MooncakeConnector` to match it — either as the top-level `kv_connector`, or nested inside a `MultiConnector`. |
| `--mooncake-bootstrap-port` / `MOONCAKE_BOOTSTRAP_PORT` | `8998` | Port of the Mooncake bootstrap endpoint on prefill pods. Corresponds to vLLM's `VLLM_MOONCAKE_BOOTSTRAP_PORT`. |

**Capability vs. packaged path.** The `mooncake` connector reached a released sidecar in **llm-d-router v0.9.0** ([`supportedKVConnectors` at that tag](https://github.com/llm-d/llm-d-router/blob/v0.9.0/pkg/sidecar/proxy/options.go)), and the umbrella [llm-d v0.8.0](https://github.com/llm-d/llm-d/tree/v0.8.0) release pins that sidecar — so the sidecar **capability** is present. The **deployment path** is reference-only: neither v0.8.0 nor `main` ships a Mooncake P/D overlay, and llm-d's packaged [P/D disaggregation guide](https://github.com/llm-d/llm-d/tree/main/guides/pd-disaggregation) still wires `--kv-connector=nixlv2`, so standing up Mooncake P/D means adapting the serving-pod and sidecar manifests by hand. For the current connector table and flags, see the sidecar's [disaggregation reference](https://github.com/llm-d/llm-d-router/blob/main/docs/disaggregation.md).
