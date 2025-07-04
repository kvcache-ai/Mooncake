# LMDeploy Disaggregated Serving with MooncakeTransferEngine

## Overview

This is the latest version of the MooncakeTransferEngine integration doc with the LMDeploy project based on [PR 3304](https://github.com/InternLM/lmdeploy/pull/3304#issue-2940383503) and [PR 3620](https://github.com/InternLM/lmdeploy/pull/3620) to support KVCache transfer for intra-node and inter-node disaggregated serving scenarios.

***Please note that this is still an experimental version and will be modified anytime based on feedback from the LMDeploy community.***

## Installation

### Prerequisites

```bash
pip install mooncake-transfer-engine
```

Note:

-   If any `.so` file is missing, uninstall the pip package with `pip3 uninstall mooncake-transfer-engine`, and build the binaries manually from source following the [build instructions](https://github.com/kvcache-ai/Mooncake/blob/main/doc/en/build.md).

### Install the latest version of LMDeploy

##### 1. Clone LMDeploy from the official repo

```bash
git clone https://github.com/InternLM/lmdeploy.git
```

##### 2. Build

##### 2.1 Build from source

```bash
cd LMDeploy
pip install -e .
```

-   If you encounter any problems that you cannot solve, please refer to the [LMDeploy official installation guide](https://lmdeploy.readthedocs.io/en/latest/get_started/installation.html).

------

## Configuration

### Running prefill and decode instances on different nodes

#### Proxy:

```bash
lmdeploy serve proxy \
	--server-name 192.168.0.147 \
	--server-port 8000 \
    --routing-strategy "min_expected_latency" \
	--serving-strategy DistServe \
	--log-level INFO
```

-   The `--server-name` and `--server-port` parameters specify the LMDeploy proxy service host and listening port.
-   The `--routing-strategy` parameter determines how requests are routed to prefill/decode servers (choose from`min_expected_latency`, `min_observed_latency` and `random`).
-   The `--serving-strategy` parameter sets the serving mode; `DistServe` enables disaggregated serving. Default mode of `Hybrid` will colocate prefill and decode.
-   The `--log-level` parameter controls the verbosity of runtime logging.

#### Prefill:

```bash
lmdeploy serve api_server Qwen/Qwen3-8B \
	--server-name 192.168.0.101 \ 
	--server-port 23333 \  
	--role Prefill \   
	--proxy-url http://192.168.0.147:8000 \   
	--backend pytorch \
	--migration-backend Mooncake
```

-   The `--role` parameter sets the node role in the disaggregated system (`Prefill` for token embedding and KV cache generation).
-   The `--proxy-url` parameter connects the worker instance back to the proxy for coordination.
-   The `--backend` parameter specifies the model execution backend (e.g., `pytorch`, `turbomind`).
-   The `--migration-backend` parameter defines the KV cache transport mechanism (e.g., `Mooncake` and `DlSllime`).

#### Decode:

```bash
lmdeploy serve api_server Qwen/Qwen3-8B \
	--server-name 192.168.0.147 \ 
	--server-port 23334 \  
	--role Decode \   
	--proxy-url http://192.168.0.147:8000 \   
	--backend pytorch \
	--migration-backend Mooncake
```

#### Test Inference:

```bash
curl -X POST "http://192.168.0.147:8000/v1/completions" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen3-8B",
    "temperature": 0,
    "prompt": "Shanghai is a city that ",
    "max_tokens": 16,
    "stream": false
  }'
```

------

### Running prefill and decode Instances on the same nodes

#### Proxy:

```bash
lmdeploy serve proxy \
	--server-name 192.168.0.147 \
	--server-port 8000 \
    --routing-strategy "min_expected_latency" \
	--serving-strategy DistServe \
	--log-level INFO
```

#### Prefill:

```bash
CUDA_VISIBLE_DEVICES=0 \
lmdeploy serve api_server Qwen/Qwen3-8B \
    --server-name 192.168.0.147 \
    --server-port 23333 \
    --role Prefill \
    --proxy-url http://192.168.0.147:8000 \
    --backend pytorch \
    --migration-backend Mooncake
```

-   The `CUDA_VISIBLE_DEVICES=0` specify the available GPU for prefill service since generally one GPU may not have enough VRAM to run both prefill and decode process.

#### Decode:

```bash
CUDA_VISIBLE_DEVICES=1 \
lmdeploy serve api_server Qwen/Qwen3-8B \
	--server-name 192.168.0.147 \ 
	--server-port 23334 \  
	--role Decode \   
	--proxy-url http://192.168.0.147:8000 \   
	--backend pytorch \
	--migration-backend Mooncake
```

#### Test Inference:

```bash
curl -X POST "http://192.168.0.147:8000/v1/completions" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen3-8B",
    "temperature": 0,
    "prompt": "Shanghai is a city that ",
    "max_tokens": 16,
    "stream": false
  }'
```

## Notes

-   You can specify multiple prefill or decode instances with distinct `--server-port` and different GPUs using `CUDA_VISIBLE_DEVICES`.
-   MooncakeTransferEngine supports both intra-node (PCIe) and inter-node (RDMA/CXL) transfer, and device selection is automatic or customizable via config.
-   When using HF models that timeout during prefill, consider setting model path to `~/Qwen3-8B` to accelerate loading from localhost.
-   Use `--log-level DEBUG` to get detailed runtime logs for troubleshooting.