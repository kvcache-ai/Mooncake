# Ecosystem Support Status

This page summarizes how Mooncake's components are adopted across the LLM inference, middleware, and RL post-training ecosystem.

**Legend:** ✅ Supported · 🚧 Work in progress · ❌ Not supported · — Not applicable

| <sub>Feature</sub> | <sub>Project</sub> | <sub>Type</sub> | <sub>Transfer</sub> | <sub>EP/Torch Backend</sub> | <sub>Store</sub> | <sub>Ckpt Engine</sub> |
| --- | --- | --- | :---: | :---: | :---: | :---: |
| <sub>**Inference**</sub> | <img src="https://github.com/vllm-project.png" width="16" height="16" alt="vLLM"/><br><sub>[vLLM V0](https://github.com/vllm-project/vllm)</sub> | <sub>Inference</sub> | <sub>✅</sub> | <sub>❌</sub> | <sub>✅</sub> | <sub>✅</sub> |
| | <img src="https://github.com/vllm-project.png" width="16" height="16" alt="vLLM"/><br><sub>[vLLM V1 (Omni)](https://github.com/vllm-project/vllm)</sub> | <sub>Inference</sub> | <sub>✅</sub> | <sub>🚧</sub> | <sub>✅ (native / LMCache / Nixl)</sub> | <sub>❌</sub> |
| | <img src="https://github.com/sgl-project.png" width="16" height="16" alt="SGLang"/><br><sub>[SGLang (Omni)](https://github.com/sgl-project/sglang)</sub> | <sub>Inference</sub> | <sub>✅</sub> | <sub>✅</sub> | <sub>✅</sub> | <sub>✅</sub> |
| | <img src="https://github.com/InternLM.png" width="16" height="16" alt="LMDeploy"/><br><sub>[LMDeploy](https://github.com/InternLM/lmdeploy)</sub> | <sub>Inference</sub> | <sub>✅</sub> | <sub>❌</sub> | <sub>✅</sub> | <sub>❌</sub> |
| | <img src="https://github.com/NVIDIA.png" width="16" height="16" alt="NVIDIA"/><br><sub>[TensorRT-LLM](https://github.com/NVIDIA/TensorRT-LLM)</sub> | <sub>Inference</sub> | <sub>✅</sub> | <sub>❌</sub> | <sub>✅</sub> | <sub>❌</sub> |
| | <img src="https://github.com/thu-pacman.png" width="16" height="16" alt="Chitu"/><br><sub>[Chitu](https://github.com/thu-pacman/chitu)</sub> | <sub>Inference</sub> | <sub>✅</sub> | <sub>❌</sub> | <sub>❌</sub> | <sub>❌</sub> |
| | <img src="https://github.com/jd-opensource.png" width="16" height="16" alt="xLLM"/><br><sub>[xLLM](https://github.com/jd-opensource/xllm)</sub> | <sub>Inference</sub> | <sub>✅</sub> | <sub>❌</sub> | <sub>✅</sub> | <sub>❌</sub> |
| | <img src="https://github.com/alibaba.png" width="16" height="16" alt="Alibaba"/><br><sub>[RTP (Alibaba)](https://github.com/alibaba/rtp-llm)</sub> | <sub>Inference</sub> | <sub>✅</sub> | <sub>❌</sub> | <sub>✅</sub> | <sub>❌</sub> |
| <sub>**Middleware**</sub> | <img src="https://github.com/alibaba.png" width="16" height="16" alt="Alibaba"/><br><sub>[KVCM (Alibaba)](https://github.com/alibaba)</sub> | <sub>Middleware</sub> | <sub>✅</sub> | <sub>—</sub> | <sub>✅</sub> | <sub>—</sub> |
| | <img src="https://github.com/antgroup.png" width="16" height="16" alt="Ant Group"/><br><sub>[TBase (Ant)](https://github.com/antgroup)</sub> | <sub>Middleware</sub> | <sub>✅</sub> | <sub>—</sub> | <sub>❌</sub> | <sub>—</sub> |
| | <img src="https://github.com/ai-dynamo.png" width="16" height="16" alt="Dynamo"/><br><sub>[Dynamo](https://github.com/ai-dynamo/dynamo)</sub> | <sub>Framework</sub> | <sub>✅ (w/ Nixl)</sub> | <sub>❌</sub> | <sub>❌</sub> | <sub>❌</sub> |
| | <img src="https://github.com/LMCache.png" width="16" height="16" alt="LMCache"/><br><sub>[LMCache](https://github.com/LMCache/LMCache)</sub> | <sub>Middleware</sub> | <sub>❌</sub> | <sub>—</sub> | <sub>✅</sub> | <sub>—</sub> |
| | <img src="https://github.com/Ascend.png" width="16" height="16" alt="TransferQueue"/><br><sub>[TransferQueue](https://github.com/Ascend/TransferQueue)</sub> | <sub>Middleware</sub> | <sub>—</sub> | <sub>—</sub> | <sub>✅</sub> | <sub>—</sub> |
| <sub>**RL Post-Training**</sub> | <img src="https://github.com/THUDM.png" width="16" height="16" alt="slime"/><br><sub>[Slime/Miles](https://github.com/THUDM/slime)</sub> | <sub>RL</sub> | <sub>🚧</sub> | <sub>—</sub> | <sub>✅</sub> | <sub>—</sub> |
| | <img src="https://github.com/alibaba.png" width="16" height="16" alt="Alibaba"/><br><sub>[ROLL (Alibaba)](https://github.com/alibaba/ROLL)</sub> | <sub>RL</sub> | <sub>❌</sub> | <sub>—</sub> | <sub>✅</sub> | <sub>—</sub> |
| | <img src="https://github.com/volcengine.png" width="16" height="16" alt="verl"/><br><sub>[Verl](https://github.com/volcengine/verl)</sub> | <sub>RL</sub> | <sub>❌</sub> | <sub>—</sub> | <sub>✅ (w/ TransferQueue)</sub> | <sub>—</sub> |
