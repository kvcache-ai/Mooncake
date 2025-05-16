<div align="center">
  <img src=image/mooncake-icon.png width=44% />
  <h2 align="center">
      A KVCache-centric Disaggregated Architecture for LLM Serving
  </h2>
  <a href="https://www.usenix.org/system/files/fast25-qin.pdf" target="_blank"><strong>Paper</strong></a>
  | <a href="https://www.usenix.org/system/files/fast25_slides-qin.pdf" target="_blank"><strong>Slides</strong></a>
  | <a href="FAST25-release/traces" target="_blank"><strong>Traces</strong></a>
  | <a href="https://arxiv.org/abs/2407.00079" target="_blank"><strong>Technical Report</strong></a>
  | <a href="https://kvcache-ai.github.io/Mooncake/" target="_blank"><strong>Blog</strong></a>
</div>
<br/>

Mooncake is the serving platform for  <a href="https://kimi.ai/"><img src="image/kimi.png" alt="icon" style="height: 16px; vertical-align: middle;"> Kimi</a>, a leading LLM service provided by <a href="https://www.moonshot.cn/"><img src="image/moonshot.jpg" alt="icon" style="height: 16px; vertical-align: middle;"> Moonshot AI</a>.
Now both the Transfer Engine and Mooncake Store are open-sourced!
This repository also hosts its technical report and the open sourced traces. 

<h2 id="updates">🔄 Updates</h2>

 - **May 9, 2025**: NIXL officially supports Mooncake Transfer Engine as [a backend plugin](https://github.com/ai-dynamo/nixl/blob/main/src/plugins/mooncake/README.md).
 - **May 8, 2025**: Mooncake x LMCache <a href="doc/en/lmcache-integration.md" target="_blank">unite</a> to pioneer KVCache-centric LLM serving system.
 - **May 5, 2025**: Supported by Mooncake Team, SGLang release <a href="https://lmsys.org/blog/2025-05-05-large-scale-ep/" target="_blank">guidance</a> to deploy DeepSeek with PD Disaggregation on 96 H100 GPUs.
 - **Apr 22, 2025**: LMCache officially supports Mooncake Store as a <a href="https://blog.lmcache.ai/2025-04-22-tencent/" target="_blank">remote connector</a>.
 - **Apr 10, 2025**: SGLang officially supports Mooncake Transfer Engine for disaggregated prefilling and KV cache transfer.
 - **Mar 7, 2025**: We open sourced the Mooncake Store, a distributed KVCache based on Transfer Engine. vLLM's xPyD disaggregated prefilling & decoding based on Mooncake Store will be released soon.
 - **Feb 25, 2025**: Mooncake receives the **Best Paper Award** at **FAST 2025**!
 - **Feb 21, 2025**: The updated <a href="FAST25-release/traces" target="_blank">traces</a> used in our FAST'25 paper have been released.
 - **Dec 16, 2024**: vLLM officially supports Mooncake Transfer Engine for disaggregated prefilling and KV cache transfer.
 - **Nov 28, 2024**: We open sourced the Transfer Engine, the central component of Mooncake. We also provide two demonstrations of Transfer Engine: a P2P Store and vLLM integration.
 - **July 9, 2024**: We open sourced the trace as a <a href="https://github.com/kvcache-ai/Mooncake/blob/main/mooncake_trace.jsonl" target="_blank">jsonl file</a>.
 - **June 27, 2024**: We present a series of Chinese blogs with more discussions on <a href="https://zhuanlan.zhihu.com/p/705754254">zhihu 1</a>, <a href="https://zhuanlan.zhihu.com/p/705910725">2</a>, <a href="https://zhuanlan.zhihu.com/p/706204757">3</a>, <a href="https://zhuanlan.zhihu.com/p/707997501">4</a>.
 - **June 26, 2024**: Initial technical report release.


<h2 id="overview">🎉 Overview</h2>

Mooncake features a KVCache-centric disaggregated architecture that separates the prefill and decoding clusters. It also leverages the underutilized CPU, DRAM, and SSD resources of the GPU cluster to implement a disaggregated cache of KVCache. 

![architecture](image/architecture.png)

The core of Mooncake is its KVCache-centric scheduler, which balances maximizing overall effective throughput while meeting latency-related Service Level Objectives (SLOs) requirements. Unlike traditional studies that assume all requests will be processed, Mooncake faces challenges due to highly overloaded scenarios. To mitigate these, we developed a prediction-based early rejection policy. Experiments show that Mooncake excels in long-context scenarios. Compared to the baseline method, Mooncake can achieve up to a 525% increase in throughput in certain simulated scenarios while adhering to SLOs. Under real workloads, Mooncake’s innovative architecture enables <a href="https://kimi.ai/">Kimi</a> to handle 75% more requests.

<h2 id="components">🧩 Components</h2>

![components](image/components.png)

**Mooncake Core Component: Transfer Engine (TE)**  
The core of Mooncake is the Transfer Engine (TE), which provides a unified interface for batched data transfer across various storage devices and network links. Supporting multiple protocols including TCP, RDMA, CXL/shared-memory, and NVMe over Fabric (NVMe-of), TE is designed to enable fast and reliable data transfer for AI workloads. Compared to Gloo (used by Distributed PyTorch) and traditional TCP, TE achieves significantly lower I/O latency, making it a superior solution for efficient data transmission.

**P2P Store and Mooncake Store**  
Both P2P Store and Mooncake Store are built on the Transfer Engine and provide key/value caching for different scenarios. P2P Store focuses on sharing temporary objects (e.g., checkpoint files) across nodes in a cluster, preventing bandwidth saturation on a single machine. Mooncake Store, on the other hand, supports distributed pooled KVCache, specifically designed for XpYd disaggregation to enhance resource utilization and system performance.

**Mooncake Integration with Leading LLM Inference Systems**  
Mooncake has been seamlessly integrated with several popular large language model (LLM) inference systems. Through collaboration with the vLLM and SGLang teams, Mooncake now officially supports prefill-decode disaggregation. By leveraging the high-efficiency communication capabilities of RDMA devices, Mooncake significantly improves inference efficiency in prefill-decode disaggregation scenarios, providing robust technical support for large-scale distributed inference tasks.

<h2 id="show-cases">🔥 Show Cases</h2>

### Use Transfer Engine Standalone ([Guide](doc/en/transfer-engine.md))

Transfer Engine is a high-performance data transfer framework. Transfer Engine provides a unified interface to transfer data from DRAM, VRAM or NVMe, while the technical details related to hardware are hidden. Transfer Engine supports TCP, RDMA (InfiniBand/RoCEv2/eRDMA/NVIDIA GPUDirect) and NVMe over Fabric (NVMe-of) protocols.

#### Highlights
- **Efficient use of multiple RDMA NIC devices.** Transfer Engine supports the use of multiple RDMA NIC devices to achieve the *aggregation of transfer bandwidth*.

- **Topology aware path selection.** Transfer Engine can *select optimal devices* based on the location (NUMA affinity, etc.) of both source and destination.

- **More robust on temporary network error.** Once transmission fails, Transfer Engine will try to use alternative paths for data delivery automatically.

#### Performance
With 40 GB of data (equivalent to the size of the KVCache generated by 128k tokens in the LLaMA3-70B model), Mooncake Transfer Engine delivers up to **87 GB/s** and **190 GB/s** of bandwidth in 4×200 Gbps and 8×400 Gbps RoCE networks respectively, which are about **2.4x and 4.6x faster** than the TCP protocol.

<!-- ![transfer-engine-performance.png](image/transfer-engine-performance.png) -->
<img src=image/transfer-engine-performance.png width=75% />

### P2P Store  ([Guide](doc/en/p2p-store.md))
P2P Store is built on the Transfer Engine and supports sharing temporary objects between peer nodes in a cluster. P2P Store is ideal for scenarios like checkpoint transfer, where data needs to be rapidly and efficiently shared across a cluster. 
**P2P Store has been used in the checkpoint transfer service of Moonshot AI.**

#### Highlights
- **Decentralized architecture.** P2P Store leverages a pure client-side architecture with global metadata managed by the etcd service.

- **Efficient data distribution.** Designed to enhance the efficiency of large-scale data distribution, P2P Store *avoids bandwidth saturation* issues by allowing replicated nodes to share data directly. This reduces the CPU/RDMA NIC pressures of data providers (e.g., trainers).

<!-- #### Performance
Thanks to the high performance of Transfer Engine, P2P Stores can also distribute objects with full utilization of *hardware incoming bandwidth* (e.g., A 25Gbps NIC was used in the following figure, and the throughput of get replica is about 3.1 GB/s). -->

<!-- ![p2p-store.gif](image/p2p-store.gif) -->

### Mooncake Store ([Guide](doc/en/mooncake-store-preview.md))
Mooncake Store is a distributed KVCache storage engine specialized for LLM inference based on Transfer Engine. It is the central component of the KVCache-centric disaggregated architecture. The goal of Mooncake Store is to store the reusable KV caches across various locations in an inference cluster. Mooncake Store has been supported in [vLLM's prefill serving](https://docs.vllm.ai/en/latest/features/disagg_prefill.html) and is now integrated with [LMCache](doc/en/lmcache-integration.md) to provide enhanced KVCache management capabilities.

#### Highlights
- **Multi-replica support**: Mooncake Store supports storing multiple data replicas for the same object, effectively alleviating hotspots in access pressure.

- **High bandwidth utilization**: Mooncake Store supports striping and parallel I/O transfer of large objects, fully utilizing multi-NIC aggregated bandwidth for high-speed data reads and writes.

### vLLM Integration ([Guide v0.2](doc/en/vllm-integration-v0.2.md))
To optimize LLM inference, the vLLM community is working on supporting [disaggregated prefilling (PR 10502)](https://github.com/vllm-project/vllm/pull/10502). This feature allows separating the **prefill** phase from the **decode** phase in different processes. The vLLM uses `nccl` and `gloo` as the transport layer by default, but currently it cannot efficiently decouple both phases in different machines.

We have implemented vLLM integration, which uses Transfer Engine as the network layer instead of `nccl` and `gloo`, to support **inter-node KVCache transfer** [(PR 10884)](https://github.com/vllm-project/vllm/pull/10884). Transfer Engine provides simpler interfaces and more efficient use of RDMA devices. 

We will soon release the new vLLM integration based on Mooncake Store, which supports xPyD prefill/decode disaggregation.

**_Update[Dec 16, 2024]: Here is the latest vLLM Integration ([Guide v0.2](doc/en/vllm-integration-v0.2.md)) that is based on vLLM's main branch._**

#### Performance
By supporting Topology Aware Path Selection and multi-card bandwidth aggregation, Mean TTFT of vLLM with Transfer Engine is up to 25% lower than traditional TCP-based transports.
In the future, we will further improve TTFT through GPUDirect RDMA and zero-copy.

| Backend/Setting                                         | Output Token Throughput (tok/s) | Total Token Throughput (tok/s) | Mean TTFT (ms) | Median TTFT (ms) | P99 TTFT (ms)|
|---------------------------------------------------------|---------------------------------|--------------------------------|----------------|------------------|---------------|
| Transfer Engine (RDMA) | 12.06                           | 2042.74                        | 1056.76        | 635.00           | 4006.59       |
| TCP  | 12.05                           | 2041.13                        | 1414.05        | 766.23          | 6035.36       |

- Click [here](doc/en/vllm-benchmark-results-v0.2.md) to access detailed benchmark results.

**More advanced features will coming soon, so stay tuned!**

<h2 id="quick-start">🚀 Quick Start</h2>

### Before using Mooncake

Mooncake is designed and optimized for high-speed RDMA networks. Though Mooncake supports TCP-only data transfer, we **strongly** recommend users to evaluate the functionality and performance of Mooncake with RDMA network support.

The following needs to be installed before running any component of Mooncake:
- RDMA Driver & SDK, such as Mellanox OFED. 
- Python 3.10, virtual environment is recommended.
- CUDA 12.1 and above, including NVIDIA GPUDirect Storage Support, if the package is build with `-DUSE_CUDA` (disabled by default). *You may install them from [here](https://developer.nvidia.com/cuda-downloads)*.

### Use Python package
The most simple way to use Mooncake Transfer Engine is using `pip`:
```python
pip install mooncake-transfer-engine
```
> [!IMPORTANT]
> If users encounter problems such as missing `lib*.so`, they should uninstall this package by `pip uninstall mooncake-transfer-engine`, and build the binaries manually.

### Use Docker image
Mooncake supports Docker-based deployment, see [Build Guide](doc/en/build.md) in detail.

### Build and use binaries
The following are additional dependencies of building Mooncake:
- Build essentials, including gcc, g++ (9.4+) and cmake (3.16+).
- Go 1.20+, if you want to build with `-DWITH_P2P_STORE` or `-DUSE_ETCD` (enabled by default). 
- CUDA 12.1 and above, including NVIDIA GPUDirect Storage Support, if the package is build with `-DUSE_CUDA` . *This is NOT included in the `dependencies.sh` script. You may install them from [here](https://developer.nvidia.com/cuda-downloads)*.
- [Optional] Rust Toolclain, if you want to build with `-DWITH_RUST_EXAMPLE`. *This is NOT included in the `dependencies.sh` script.*
- [Optional] `hiredis`, if you want to build with `-DUSE_REDIS`, so that you use Redis instead of etcd as metadata servers.
- [Optional] `curl`, if you want to build with `-DUSE_HTTP`, so that you use HTTP instead of etcd as metadata servers.

The building and installation steps are the following:
1. Retrieve source code from GitHub repo
   ```bash
   git clone https://github.com/kvcache-ai/Mooncake.git
   cd Mooncake
   ```

2. Install dependencies
   ```bash
   bash dependencies.sh
   ```

3. Compile Mooncake and examples
   ```bash
   mkdir build
   cd build
   cmake ..
   make -j
   sudo make install # optional, make it ready to be used by vLLM/SGLang
   ```


<h2 id="milestones"> 🛣️ Incoming Milestones</h2>

- [x] First release of Mooncake and integrate with latest vLLM
- [ ] Share KV caches across multiple serving engines
- [ ] User and developer documentation

<h2 id="trace">📦 Open Source Trace</h2>

```json
{
    "timestamp": 27482,
    "input_length": 6955,
    "output_length": 52,
    "hash_ids": [46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 2353, 2354]
}
{
    "timestamp": 30535,
    "input_length": 6472,
    "output_length": 26,
    "hash_ids": [46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 2366]
}
```
The above presents two samples from our trace dataset. The trace includes the timing of request arrivals, the number of input tokens, the number of output tokens, and the remapped block hash. To protect our customers' privacy, we applied several mechanisms to remove user-related information while preserving the dataset's utility for simulated evaluation. More descriptions of the trace (e.g., up to 50% cache hit ratio) can be found in Section 4 of the technical report.

**_Update[Feb 21, 2025]: The updated [traces](FAST25-release/traces) used in our FAST'25 paper have been released! Please refer to the paper's appendix (found [here](FAST25-release/Mooncake-FAST25.pdf)) for more details._**

<h2 id="citation">📑 Citation</h2>
Please kindly cite our paper if you find the paper or the traces are useful:

```bibtex
@article{qin2024mooncake,
  title        = {Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving},
  author       = {Ruoyu Qin, Zheming Li, Weiran He, Mingxing Zhang, Yongwei Wu, Weimin Zheng, and Xinran Xu},
  year         = {2024},
  url          = {https://arxiv.org/abs/2407.00079}
}

@inproceedings {qin2025mooncake,
  author       = {Ruoyu Qin and Zheming Li and Weiran He and Jialei Cui and Feng Ren and Mingxing Zhang and Yongwei Wu and Weimin Zheng and Xinran Xu},
  title        = {Mooncake: Trading More Storage for Less Computation {\textemdash} A {KVCache-centric} Architecture for Serving {LLM} Chatbot},
  booktitle    = {23rd USENIX Conference on File and Storage Technologies (FAST 25)},
  year         = {2025},
  isbn         = {978-1-939133-45-8},
  address      = {Santa Clara, CA},
  pages        = {155--170},
  url          = {https://www.usenix.org/conference/fast25/presentation/qin},
  publisher    = {USENIX Association},
  month        = feb
}
```
