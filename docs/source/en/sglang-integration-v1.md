# SGLang Disaggregated Serving with MooncakeTransferEngine

## Overview
This is the latest version of the MooncakeTransferEngine integration doc with the SGLang project based on [PR 4654](https://github.com/sgl-project/sglang/pull/4654) and [PR 4880](https://github.com/sgl-project/sglang/pull/4880) to support KVCache transfer for intra-node and inter-node disaggregated serving scenarios.


**_Please note that this is still an experimental version and will be modified anytime based on feedback from the SGLang community._**

## Installation
### Prerequisite
```bash
pip3 install mooncake-transfer-engine
```

Note: If you encounter problems such as missing `lib*.so`, you should uninstall this package by `pip3 uninstall mooncake-transfer-engine`, and build the binaries manually according to the [instructions](build.md).

### Install the latest version of SGLang
#### 1. Clone SGLang from official repo
```bash
git clone git@github.com:sgl-project/sglang.git
```
#### 2. Build
##### 2.1 Build from source
```bash
cd sglang
pip install --upgrade pip
pip install -e "python[all]"
```

#####  If running on AMD GPU, tried below steps to install sglang for rocm or just run in rocm docker from https://hub.docker.com/r/rocm/sgl-dev directly
```bash

pip install --upgrade pip
cd sgl-kernel
python setup_rocm.py install
cd ..
pip install -e "python[all_hip]"

or

docker run -it --rm --network=host \
    --device=/dev/kfd --device=/dev/dri \
    --ipc=host --shm-size 16G \
    --group-add video \
    --cap-add=SYS_PTRACE \
    --security-opt seccomp=unconfined \
    -v /home/workspace:/workspace \
   rocm/sgl-dev:20250707

pip3 install mooncake-transfer-engine
sudo apt update
sudo apt install libibverbs1 libibverbs-dev -y
sudo apt-get install lsof net-tools iputils-ping -y

```

 - If you encounter any problems that you cannot solve, please refer to the [SGLang official compilation guide](https://docs.sglang.ai/start/install.html).

## Configuration

 - **Update(Apr 10, 2025)** Good news: The configuration file requirement has been removed since [PR 5460](https://github.com/sgl-project/sglang/pull/5460). There is no need to prepare the _mooncake.json_ file anymore.


### To run prefill instance and decode instance on different node

Prefill: 
```bash
python -m sglang.launch_server --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --disaggregation-mode prefill --port 30000 --host 192.168.0.137 --tp-size 2 --page-size 32
```
 - The `--model-path` parameter specifies the model to use.
 - The `--host` parameter specifies the SGLang service host.
 - The `--port` parameter specifies the SGLang service port on which to listen.
 - The `--disaggregation-mode` is the node's role, either 'prefill' or 'decode'.
 - The `--disaggregation-ib-device` is the device to be used for data transmission, it is optional since we will detect this config automatically. Or you can still explicitly specify devices if needed. If multiple NIC devices are used, they can be separated by commas, such as "erdma_0,erdma_1". Please note that there are no spaces between them.
 - The `--page-size` is the page size for the radix tree, a larger page size will help reduce kvcache indice fragmentation.
 - Option `--tp-size` is supported. Example: append `--tp-size 2` to the run command to run SGLang with multiple GPUs.

Decode:
```bash
python -m sglang.launch_server --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --disaggregation-mode decode --port 30001 --host 192.168.0.140 --tp-size 2 --page-size 32
```

Proxy:
```bash
python3 -m sglang.srt.disaggregation.mini_lb --prefill http://192.168.0.137:30000 --decode http://192.168.0.140:30001 --host 0.0.0.0 --port 8000
```

Test:
```bash
curl -X POST http://127.0.0.1:8000/generate -H "Content-Type: application/json" -d '{
  "text": "Let me tell you a long story ",
  "sampling_params": {
    "temperature": 0
  }
}'
```

### To run prefill instance and decode instance on the same node

Prefill: 
```bash
python -m sglang.launch_server --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --disaggregation-mode prefill --port 30000 --host 192.168.0.137 --tp-size 2 --page-size 32
```

Decode:
```bash
python -m sglang.launch_server --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --disaggregation-mode decode --port 30001 --base-gpu-id 2 --host 192.168.0.137 --tp-size 2 --page-size 32
```
 - The function of `--base-gpu-id` is similar to env var`CUDA_VISIBLE_DEVICES`, which is used to avoid reusing the 0th GPU card. The difference is that it is used to specify the starting number of the GPU. If it is set to 2, the first and second cards will be skipped, and the third card will be used directly.

Proxy:
```bash
python3 -m sglang.srt.disaggregation.mini_lb --prefill http://192.168.0.137:30000 --decode http://192.168.0.137:30001 --host 0.0.0.0 --port 8000
```

Test:
```bash
curl -X POST http://127.0.0.1:8000/generate -H "Content-Type: application/json" -d '{
  "text": "Let me tell you a long story ",
  "sampling_params": {
    "temperature": 0
  }
}'
```

Note:
 - TP is supported but not required, you can remove `--tp-size 2` if you want.
 - The `--disaggregation-ib-device` is the device to be used for data transmission, it is optional since we will detect this config automatically. Or you can still explicitly specify devices if needed. If multiple NIC devices are used, they can be separated by commas, such as "erdma_0,erdma_1". Please note that there are no spaces between them.
 - XpYd is supported. It is ok to run multiple prefill and multiple decode instances on the same node, but you need to set up `--prefill-bootstrap-ports` when running multiple prefill instances on the same node.
   - e.g., `python3 -m sglang.srt.disaggregation.mini_lb --prefill http://192.168.0.137:30000 http://192.168.0.140:30000 --decode http://192.168.0.137:30001 http://192.168.0.140:30001 --host 0.0.0.0 --port 8000`
   - `python3 -m sglang.srt.disaggregation.mini_lb --prefill http://192.168.0.137:30000 http://192.168.0.137:30100 --decode http://192.168.0.137:30200 http://192.168.0.137:30300 --host 0.0.0.0 --port 8000 --prefill-bootstrap-ports 8998 8999`
 - HuggingFace timeout can be addressed by `export SGLANG_USE_MODELSCOPE=true`