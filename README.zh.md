<div align="center">
  <img src=image/mooncake-icon.png width=44% />
  <h2 align="center">
      面向大语言模型服务的以 KVCache 为中心的分离式架构
  </h2>
  <a href="https://www.usenix.org/system/files/fast25-qin.pdf" target="_blank"><strong>学术论文</strong></a>
  | <a href="https://www.usenix.org/system/files/fast25_slides-qin.pdf" target="_blank"><strong>演讲幻灯片</strong></a>
  | <a href="FAST25-release/traces" target="_blank"><strong>真实请求轨迹 (Traces)</strong></a>
  | <a href="https://kvcache-ai.github.io/Mooncake/" target="_blank"><strong>官方文档</strong></a>
  | <a href="https://kvcache.ai/" target="_blank"><strong>技术博客</strong></a>
  | <a href="https://join.slack.com/t/mooncake-project/shared_invite/zt-3qx4x35ea-zSSTqTHItHJs9SCoXLOSPA" target="_blank"><strong>Slack 社区</strong></a>
  <br />
  <br />

  [English](./README.md) | **简体中文**
  <br />
  <br />

  [![PyPI - Downloads](https://static.pepy.tech/badge/mooncake-transfer-engine?period=month)](https://pypi.org/project/mooncake-transfer-engine)
  [![GitHub commit activity](https://img.shields.io/github/commit-activity/w/kvcache-ai/Mooncake)](https://github.com/kvcache-ai/Mooncake/graphs/commit-activity)
  [![license](https://img.shields.io/github/license/kvcache-ai/mooncake.svg)](https://github.com/kvcache-ai/Mooncake/blob/main/LICENSE-APACHE)
  [![Docker](https://img.shields.io/docker/v/kvcacheai/mooncake?label=docker&logo=docker&logoColor=white&color=2496ED)](https://hub.docker.com/r/kvcacheai/mooncake)
  <br />

  [![PyPI CUDA <=12.9](https://img.shields.io/static/v1?label=pypi&message=CUDA%20%3C%3D12.9&color=76B900)](https://pypi.org/project/mooncake-transfer-engine)
  [![PyPI CUDA 13.0/13.1](https://img.shields.io/static/v1?label=pypi&message=CUDA%2013.0%2F13.1&color=76B900)](https://pypi.org/project/mooncake-transfer-engine-cuda13)
  [![PyPI Non-CUDA](https://img.shields.io/static/v1?label=pypi&message=non-CUDA&color=00BFFF)](https://pypi.org/project/mooncake-transfer-engine-non-cuda/)
  [![PyPI NPU](https://img.shields.io/static/v1?label=pypi&message=NPU&color=F87171)](https://pypi.org/project/mooncake-transfer-engine-npu/)
  [![PyPI MUSA](https://img.shields.io/static/v1?label=pypi&message=MUSA&color=F97316)](https://pypi.org/project/mooncake-transfer-engine-musa/)
  [![PyPI EFA](https://img.shields.io/static/v1?label=pypi&message=EFA&color=F59E0B)](https://pypi.org/project/mooncake-transfer-engine-efa/)
  [![PyPI ROCm](https://img.shields.io/static/v1?label=pypi&message=ROCm&color=black)](https://pypi.org/project/mooncake-transfer-engine-rocm/)
</div>
<br/>

Mooncake 是由 <a href="https://www.moonshot.cn/"><img src="image/moonshot.jpg" alt="icon" style="height: 16px; vertical-align: middle;"> 月之暗面（Moonshot AI）</a> 提供的大语言模型服务 <a href="https://kimi.ai/"><img src="image/kimi.png" alt="icon" style="height: 16px; vertical-align: middle;"> Kimi</a> 的底层推理服务平台。
在真实的业务工作负载下，Mooncake 的创新架构使 Kimi 在严格满足服务等级目标（SLO）的前提下，能够多承载 75% 的用户请求。

<h2 id="updates">🔄 最新动态</h2>

<!-- 保留最近的 7 条动态可见；更早的条目折叠于下方。新 PR/发布日期采用北京时间 (UTC+8)。 -->

- **2026年10月10日**：TensorRT-LLM 在其 KVCMv2 架构中合入了原生的 [Mooncake Store 连接器](https://github.com/NVIDIA/TensorRT-LLM/pull/19171)，支持在跨推理实例之间实现基于内容寻址的分布式 KV Cache 资源池化与前缀复用。
- **2026年10月6日**：[TokenSpeed v0.1.1](https://github.com/lightseekorg/tokenspeed/releases/tag/v0.1.1) 将 Mooncake Store 作为其主机缓存之下的 L3 KV Cache 后端，并支持使用 Mooncake Transfer Engine 实现 PD（Prefill/Decode）与 EPD（Encode/Prefill/Decode）分离式推理。
- **2026年9月17日**：vLLM 通过 Mooncake Store 支持了[跨编码器 Embedding 复用](https://github.com/vllm-project/vllm/pull/56242)，与其针对多模态解耦服务的[编码器缓存传输连接器](https://github.com/vllm-project/vllm/pull/41567)相辅相成。两项特性均已包含在 [vLLM v0.31.0](https://github.com/vllm-project/vllm/releases/tag/v0.31.0) 中。
- **2026年8月28日**：LMDeploy 合入了 [Mooncake Store 支持](https://github.com/InternLM/lmdeploy/pull/4903)，支持在 PyTorch 推理实例间实现共享 KV Cache 池化与前缀复用。已发布于 [LMDeploy v0.18.0](https://github.com/InternLM/lmdeploy/releases/tag/v0.18.0)。
- **2026年8月24日**：[AgentX / InferenceX v3](https://newsletter.semianalysis.com/p/agentx-inferencexv3-does-cuda-moat) 专题报道了 Mooncake 在 Agent 推理工作负载中的卓越贡献，涵盖 ROCm Wheels、CI 及打包环境的优化，详情请见 [Mooncake 优化专页](https://inferencex.semianalysis.com/agentx/optimizations/mooncake)。

<details>
<summary>查看更多历史动态</summary>

- **2026年8月20日**：Mooncake 作为 Rollout 数据传输后端集成入 [Miles](https://github.com/radixark/miles)，专为解耦强化学习（Disaggregated RL）中在经验采样（Rollout）与训练之间流转的碎片化、异构数据提供高效传输。技术博客详见：[KVCache.AI](https://kvcache.ai/blog/mooncake-rl-rollout-data-transfer/)、[lmsys](https://www.lmsys.org/blog/2026-08-20-miles-mooncake-rollout-data-transfer)。
- **2026年8月17日**：Mooncake 作为多节点在线训练的分布式后端集成入 [Speculators](https://github.com/vllm-project/speculators)，通过 RDMA 高效地在 vLLM 推理 Worker 与 Speculators 训练 Worker 之间传输隐层状态（Hidden-state），彻底免除了离线训练对海量隐层状态落盘存储的需求。[GB300 NVL72 上的基准测试结果](https://x.com/mgoin_/status/2072785822231728363)。
- **2026年8月5日**：llm-d 新增了 [Mooncake PD 部署指南](https://github.com/llm-d/llm-d/pull/1511)，配合其针对 Kubernetes 上 vLLM 服务的 [Mooncake Store 分级卸载集成](https://github.com/llm-d/llm-d/pull/1826)。
- **2026年8月1日**：Dynamo 为 SGLang HiCache 路由合入了[事件驱动的 Mooncake 共享缓存索引](https://github.com/ai-dynamo/dynamo/pull/11239)，利用 Mooncake 对象事件实时追踪共享池中可复用的 KV 页面。
- **2026年7月24日**：FlexKV 新增 [Mooncake Store 作为分布式 REMOTE 层后端](https://github.com/taco-project/FlexKV/pull/231)，在其现有的 Mooncake Transfer Engine 集成之外，实现了基于内容寻址的 KV Cache 共享能力。
- **2026年7月12日**：TransferQueue 为其 Mooncake Store 后端新增了 [GPUDirect RDMA（GDR）支持](https://github.com/Ascend/TransferQueue/pull/131)，使 GPU 客户端能够通过 GDR 直接与主机内存存储交互强化学习张量。
- **2026年5月7日**：🚀 [vLLM 官方博客发布 Mooncake Store 专题介绍](https://vllm.ai/blog/mooncake-store) —— 深度解析 Mooncake 分布式 KVCache 引擎如何通过高吞吐、高内存效率的跨实例 KV Cache 共享为 vLLM 推理注入澎湃性能！
- **2026年4月29日**：SGLang 借助 Mooncake TransferEngine 实现了[面向大规模分布式强化学习的基于 RDMA 的 P2P 权重传输机制](https://lmsys.org/blog/2026-04-29-p2p-update/)，通过跨数千块 GPU 的零拷贝 RDMA 传输，将 1 万亿参数的 Kimi-K2 模型权重更新速度提升了 7 倍（由 53 秒大幅缩减至 7.2 秒）。
- **2026年4月3日**：LMCache 引入了原生的 [Mooncake Store 多进程模式 L2 适配器](https://github.com/LMCache/LMCache/pull/2911)，将 LMCache MP 服务直连至分布式 KV Cache 池。
- **2026年4月1日**：[TENT 技术报告](https://arxiv.org/abs/2604.00368)发布，介绍了面向异构互联的声明式数据移动编排引擎，并在 SGLang HiCache 和 Checkpoint Engine 业务负载中完成了实测验证。
- **2026年3月19日**：大规模投机解码训练项目 [TorchSpec: Speculative Decoding Training at Scale](https://pytorch.org/blog/torchspec-speculative-decoding-training-at-scale) 正式[开源](https://github.com/torchspec-project/TorchSpec)，其利用 Mooncake 高效的隐层状态管理，成功实现了推理与训练的彻底解耦。
- **2026年3月16日**：FastDeploy 在其 PD 流水线中新增了 [Mooncake Store 缓存读写支持](https://github.com/PaddlePaddle/FastDeploy/pull/6768)，完善了其在跨推理实例间的[全局 KV Cache 池化架构](https://github.com/PaddlePaddle/FastDeploy/blob/develop/docs/features/global_cache_pooling.md)。
- **2026年3月5日**：[LightX2V](https://github.com/ModelTC/LightX2V/pull/893) 现已支持基于 Mooncake 的解耦部署，利用 Mooncake Transfer Engine 实现编码器与 Transformer 服务的解耦，提供跨设备与跨节点的极致数据传输性能。博客详情参见 [LightX2V 专文](https://light-ai.top/LightX2V-BLOG/posts/Disaggregation/)。
- **2026年3月3日**：verl 新增了 [Mooncake 检查点引擎后端](https://github.com/verl-project/verl/pull/5176)，用于在训练 Worker 与 Rollout Worker 之间进行点对点模型权重同步。
- **2026年2月25日**：[SGLang](https://github.com/sgl-project/sglang) 合入了[编码器全局缓存管理器（Encoder Global Cache Manager）](https://github.com/sgl-project/sglang/pull/16137)，引入由 Mooncake 驱动的全局多模态 Embedding 缓存，支持跨实例共享 ViT 嵌入表示，避免 GPU 重复计算。
- **2026年2月24日**：[vLLM-Omni](https://docs.vllm.ai/projects/vllm-omni/en/latest/design/feature/disaggregated_inference/) 推出解耦推理连接器，全面支持 `MooncakeStoreConnector` 与 `MooncakeTransferEngineConnector`，专为多节点全模态处理流水线服务。
- **2026年2月12日**：[Mooncake 正式加入 PyTorch 生态](https://pytorch.org/blog/mooncake-joins-pytorch-ecosystem/)！我们非常荣幸地宣布 Mooncake 已正式成为 PyTorch 官方生态项目的一员！
- **2026年1月28日**：腾讯与 NVIDIA 联合社区开源的分布式 KV 存储与缓存系统 [FlexKV](https://github.com/taco-project/FlexKV) 现已基于 Mooncake Transfer Engine 支持[分布式 KVCache 复用](https://github.com/taco-project/FlexKV/blob/main/docs/dist_reuse/README_en.md)。
- **2025年12月27日**：与阿里巴巴 [ROLL](https://github.com/alibaba/ROLL) 达成合作！欢迎阅读[合作论文](https://arxiv.org/abs/2512.22560)。
- **2025年12月23日**：SGLang 引入了以 Mooncake 为传输后端的[编码-预填充-解码解耦（EPD Disaggregation）架构](https://lmsys.org/blog/2026-01-12-epd/)。该集成使得计算密集型的多模态编码器（如 Vision Transformer）能够与大语言模型节点解耦，利用 Mooncake 的 RDMA 引擎实现海量多模态 Embedding 的零拷贝高速传输。
- **2025年12月19日**：Mooncake Transfer Engine 已正式[集成至 TensorRT LLM](https://github.com/NVIDIA/TensorRT-LLM/tree/main/cpp/tensorrt_llm/executor/cache_transmission/mooncake_utils)，用于 PD 分离推理中的 KVCache 传输。
- **2025年12月19日**：Mooncake Transfer Engine 已作为 [KV Connector](https://docs.vllm.ai/en/latest/features/mooncake_connector_usage/) 原生集成至 vLLM v1 的 PD 分离配置中。
- **2025年11月7日**：推出 [RBG + SGLang HiCache + Mooncake](https://github.com/sgl-project/rbg/blob/main/keps/74-mooncake-integration/README.md)，这是一套开箱即用、基于角色的云原生部署方案，兼备弹性伸缩与超高性能。
- **2025年10月15日**：SGLang 引入了 [Mooncake EP 与集合通信后端](https://github.com/sgl-project/sglang/pull/10423)，奠定了容错弹性专家并行（Elastic Expert Parallel）推理的基石。
- **2025年9月18日**：Mooncake Store 作为[分布式 KV Cache 池后端](https://github.com/vllm-project/vllm-ascend/blob/main/docs/source/user_guide/feature_guide/kv_cache_pool_ascend_store.md)全面赋能 vLLM 昇腾（Ascend）生态。
- **2025年9月10日**：SGLang 官方宣布支持 Mooncake Store 作为[分层 KV 缓存存储后端](https://lmsys.org/blog/2025-09-10-sglang-hicache/)。该集成扩展了 RadixAttention，支持跨设备显存、主机内存和远端存储层构建多层级 KV Cache。
- **2025年9月10日**：Mooncake P2P Store 的高性能官方生产版本正式开源为 [checkpoint-engine](https://github.com/MoonshotAI/checkpoint-engine/)。该引擎已成功应用于 K1.5 与 K2 的线上生产训练，在 ~20 秒内完成了 Kimi-K2 模型（1 万亿参数）在数千块 GPU 上的全量更新。
- **2025年8月23日**：京东开源的高性能推理引擎 [xLLM](https://github.com/jd-opensource/xllm) 基于 Mooncake 构建了混合 KV 缓存管理体系，支持带智能卸载与预取的全局 KV 缓存管理。
- **2025年8月18日**：vLLM-Ascend [集成了 Mooncake Transfer Engine](https://github.com/vllm-project/vllm-ascend/blob/main/docs/source/tutorials/features/pd_disaggregation_mooncake_multi_node.md)，用于 KV Cache 注册与分离式 Prefill，在昇腾 NPU 上实现了高效的分布式推理。
- **2025年7月20日**：Mooncake 全面支持了 [Kimi K2 的线上部署](https://lmsys.org/blog/2025-07-20-k2-large-scale-ep/)，在 128 块 H200 GPU 上采用 PD 分离与大规模专家并行（EP），达成了 224k tokens/s 的 Prefill 吞吐量和 288k tokens/s 的 Decode 吞吐量。
- **2025年6月20日**：Mooncake 成为 LMDeploy 的 PD 分离[传输后端](https://kvcache-ai.github.io/Mooncake/deployment/integrations/lmdeploy.html)。
- **2025年5月9日**：NIXL 官方正式支持 Mooncake Transfer Engine 作为[后端插件](https://github.com/ai-dynamo/nixl/blob/main/src/plugins/mooncake/README.md)。
- **2025年5月8日**：[Mooncake 携手 LMCache](https://kvcache-ai.github.io/Mooncake/deployment/integrations/lmcache/index.html) 联合探索以 KVCache 为中心的大模型服务架构。
- **2025年5月5日**：在 Mooncake 团队的支持下，SGLang 发布了在 96 块 H100 GPU 上采用 PD 分离部署 DeepSeek 的<a href="https://lmsys.org/blog/2025-05-05-large-scale-ep/" target="_blank">实践指南</a>。
- **2025年4月22日**：LMCache 官方宣布支持 Mooncake Store 作为<a href="https://blog.lmcache.ai/2025-04-22-tencent/" target="_blank">远端连接器</a>。
- **2025年4月10日**：SGLang 官方正式支持 Mooncake Transfer Engine 用于分离式 Prefill 与 KV Cache 高速传输。
- **2025年3月7日**：我们开源了 Mooncake Store —— 基于 Transfer Engine 构建的分布式 KVCache 存储引擎。基于 Mooncake Store 的 vLLM xPyD 分离式预填充与解码能力随后推出。
- **2025年2月25日**：Mooncake 荣获存储系统国际顶会 **FAST 2025 最佳论文奖（Best Paper Award）**！
- **2025年2月21日**：FAST'25 论文中所使用的更新版<a href="FAST25-release/traces" target="_blank">真实请求轨迹（Traces）</a>已正式发布。
- **2024年12月16日**：vLLM 官方正式支持 Mooncake Transfer Engine 用于分离式 Prefill 与 KV Cache 高速传输。
- **2024年11月28日**：开源 Mooncake 的核心组件 Transfer Engine，并提供了两套演示范例：P2P Store 与 vLLM 原生集成。
- **2024年7月9日**：以 <a href="https://github.com/kvcache-ai/Mooncake/blob/main/FAST25-release/arxiv-trace/mooncake_trace.jsonl" target="_blank">JSONL 文件格式</a>开源了真实请求轨迹数据。
- **2024年6月27日**：发布了关于 Mooncake 设计理念的知乎系列技术深度解析长文：<a href="https://zhuanlan.zhihu.com/p/705754254">知乎专栏 1</a>、<a href="https://zhuanlan.zhihu.com/p/705910725">2</a>、<a href="https://zhuanlan.zhihu.com/p/706204757">3</a>、<a href="https://zhuanlan.zhihu.com/p/707997501">4</a>、<a href="https://zhuanlan.zhihu.com/p/9461861451">5</a>、<a href="https://zhuanlan.zhihu.com/p/1939988652114580803">6</a>、<a href="https://zhuanlan.zhihu.com/p/1959366095443064318">7</a>。
- **2024年6月26日**：发布首版初始技术报告。

</details>

<h2 id="overview">🎉 项目概览</h2>

<!-- ![components](image/components.png) -->
<div align="center">
  <img src=image/components.png width=74% />
</div>

Mooncake 是一套专为超大规模大语言模型推理与训练打造的基础设施项目。其核心特色在于采用了**以 KV Cache 为中心的分离式架构**，将 Prefill（预填充）与 Decode（解码）计算集群彻底分离解耦，并盘活 GPU 集群中此前未被充分利用的 CPU、DRAM 和 SSD 存储资源，构建起弹性、跨节点的解耦 KV Cache 存储池。

Mooncake 涵盖了用于在异构网络与加速硬件间实现超低延迟数据迁移的高性能**传输引擎（Transfer Engine）**；用于分布式管理 KV Cache 和模型权重的 **Mooncake Store**；以及专为弹性 MoE 专家并行推理设计的 **Mooncake EP 与 PG**。通过与 SGLang、vLLM 等主流生态项目的深度原生集成，Mooncake 助力各大模型系统大幅提升缓存复用率、显著压降首字及整体服务延迟，并在多节点大规模集群中实现卓越的高效扩展。

<h2 id="show-cases">🔥 核心特性与架构展示</h2>

### 传输引擎 (Transfer Engine, TE)

Mooncake 的核心支柱是**传输引擎（Transfer Engine, TE）**，这是一套高性能的数据传输框架。TE 为跨多样化存储、网络拓扑与加速芯片环境下的批量数据移动提供了高度统一的抽象接口。通过深度支持多重传输协议、拓扑感知路径优选、多网卡（Multi-NIC）带宽聚合以及故障自动转移机制，TE 为分布式 AI 负载提供了极低延迟、高可扩展且极其稳健的数据通道。详见 [Transfer Engine 架构指南](https://kvcache-ai.github.io/Mooncake/design/transfer-engine/index.html)。

<details>
<summary>亮点特性</summary>

- **高效利用多张 RDMA 网卡**：Transfer Engine 原生支持同时调度多个 RDMA 网卡设备，实现传输带宽的*线性聚合*。
- **拓扑感知路径选择**：Transfer Engine 能够根据源端与目的端的物理位置拓扑（NUMA 亲和性等）自动*选择最优硬件设备与传输路径*。
- **网络临时抖动与故障自动容错**：一旦某条链路传输发生故障，Transfer Engine 将自动无缝切换到备用路径重新递送数据。
- **规模化极致吞吐表现**：在传输 40 GB 数据量（相当于 LLaMA3-70B 模型在 128k 上下文长度下生成的 KVCache 大小）的场景中，Mooncake Transfer Engine 在 4×200 Gbps 与 8×400 Gbps RoCE 网络环境下分别达成了高达 **87 GB/s** 与 **190 GB/s** 的实际有效传输带宽，相比传统 TCP 协议分别提速了约 **2.4 倍与 4.6 倍**。

<!-- ![transfer-engine-performance.png](image/transfer-engine-performance.png) -->
<img src=image/transfer-engine-performance.png width=75% />

- **广泛支持异构传输协议与加速硬件**：Transfer Engine 提供了跨多样化通信协议的统一数据传输层，涵盖 TCP、RDMA、AWS EFA、NVMe-oF、NVLink、HIP、Barex、CXL 以及华为昇腾（Ascend）系列传输通道。配合对应的运行时编译，Transfer Engine 能够智能探测加速卡显存并自动匹配最优传输路径，全面覆盖 CUDA、MUSA（摩尔线程）、HIP（AMD ROCm）、MACA（天数智芯）、寒武纪 MLU、华为昇腾以及通过 TENT 支持的英特尔 XPU 环境。有关完整支持协议清单与配置手册，请参阅[支持协议文档](https://kvcache-ai.github.io/Mooncake/getting_started/supported-protocols.html)。
- **主流大模型生态深度采纳**：TE 已广泛应用于多个一线生产级推理框架中，包括 [SGLang](https://github.com/sgl-project/sglang)、[vLLM](https://github.com/vllm-project/vllm)、[TensorRT-LLM](https://github.com/NVIDIA/TensorRT-LLM)、[vLLM-Ascend](https://github.com/vllm-project/vllm-ascend)、[checkpoint-engine](https://github.com/MoonshotAI/checkpoint-engine) 与 [NIXL](https://github.com/ai-dynamo/nixl) 等，用于高效搬运 KV Cache、Embedding、模型权重等各类关键张量数据。

</details>

### Mooncake Store

Mooncake Store 是专为大语言模型推理场景打造的高性能分布式键值缓存（KV Cache）存储引擎。它建立在 Transfer Engine 坚实的基础之上，负责跨整个推理集群存储和统一调度可复用的 KV Cache 与模型权重，原生支持高效的大对象分块存储、多副本冗余、自适应淘汰策略以及极高带宽的数据流转。详见 [Mooncake Store 架构指南](https://kvcache-ai.github.io/Mooncake/design/mooncake-store.html)。

<details>
<summary>亮点特性</summary>

- **高带宽利用率**：Mooncake Store 支持大对象分片条带化（Striping）、并行 I/O 调度以及端到端零拷贝数据传输，充分释放多网卡聚合后的极限网络物理带宽。
- **多层级缓存体系**：Mooncake Store 创新性地构建了跨主机 DRAM 内存与 SSD/NVMe 本地固态硬盘的分级缓存架构，极大扩展了单节点与集群级缓存容量上限。
- **弹性与计算存储解耦**：Mooncake Store 将 KVCache 存储生命周期与推理计算引擎彻底剥离，存储节点可按需动态扩缩容，且缓存数据不受推理引擎重启、版本热更新或请求调度策略调整的影响。
- **声明式与程序化对象治理**：Mooncake Store 赋予上层应用通过细粒度对象级策略（包含副本数设置、偏好存储分段、软固定 Soft Pin、硬固定 Hard Pin）精细化控制对象分布与生命周期的能力。推理系统可据此保护高价值的核心 KV Cache 与基础模型权重，指导底层的多副本复制、放置和缓存淘汰决策。
- **广泛的生态落地支持**：Mooncake Store 已成为大模型系统生态中被普遍采用的高性能分布式 KV Cache、隐层状态与模型权重存储底座。它无缝支撑了 [SGLang 的分层 KV 缓存（HiCache）](https://lmsys.org/blog/2025-09-10-sglang-hicache/)、[vLLM 的 Prefill 解耦服务](https://docs.vllm.ai/en/latest/features/disagg_prefill.html)与 [LMCache](https://kvcache-ai.github.io/Mooncake/deployment/integrations/lmcache/index.html)，并被 [TorchSpec](https://pytorch.org/blog/torchspec-speculative-decoding-training-at-scale/) 和 [TransferQueue](https://github.com/Ascend/TransferQueue) 等前沿系统采用，通过高效的状态管理与异步数据迁移，实现推理、训练及强化学习任务的模块化解耦。

</details>

### Mooncake EP 与分布式进程组 (PG)

Mooncake EP 与 Mooncake PG 将 Mooncake 的能力从高性能数据移动进一步拓宽至针对超大规模 MoE（Mixture of Experts）模型推理的容错分布式执行领域。Mooncake EP 在兼容 DeepEP 风格的专家并行调度（Dispatch）与聚合（Combine）算子基础上，引入了对 Rank 节点存活状态（Activeness Awareness）的原生感知能力；而 Mooncake PG 则提供了可直接注册为 PyTorch 分布式进程组后端的集合通信原语，能够实时捕获故障 Rank 节点、向上层透传报警，并在无需重启整个推理集群服务的情况下完成单个 Rank 的故障热恢复。详见 [Mooncake EP 与后端使用指南](https://kvcache-ai.github.io/Mooncake/api-reference/python/ep-backend.html)。

<details>
<summary>亮点特性</summary>

- **容错专家并行（Fault-tolerant EP）**：Mooncake EP 在专家并行 Dispatch 与 Combine 算子中注入了 `active_ranks` 存活感知机制，使 MoE 推理系统在部分节点失联时能自动规避故障 Rank，由存活的健康专家节点持续对外提供推理服务。
- **高度兼容 DeepEP 的编程范式**：Mooncake EP 的核心 API 与 DeepEP 低延迟模式高度保持一致，帮助各类推理引擎几乎零成本改造即可接入高容错专家并行能力，无需重写底层 MoE 通信逻辑。
- **原生接入 PyTorch ProcessGroup**：Mooncake PG 可直接注册为 `torch.distributed` 通信后端，不仅原生支持诸如 `all_gather` 等标准集合通信算子，底层同时依托 Mooncake 强大的通信内核与故障通报机制。
- **弹性 Rank 动态恢复**：Mooncake PG 对外暴露了面向故障自愈的原语（如对端状态轮询与节点恢复重新接入），允许新启动的替代进程平滑重连进存量通信进程组，协助推理服务实现局部异常自动愈合。
- **SGLang 生产级 MoE 服务集成**：Mooncake 的集合通信后端与专家并行算子已正式集成入 SGLang，为超大参数规模 MoE 模型提供坚实的高容错专家并行推理基座，完美适配弹性专家并行（Elastic EP）服务体系。

</details>

### 以张量为中心（Tensor-Centric）的系统生态

Mooncake 构建了全栈式、以张量（Tensor）为一等公民的 AI 系统基础设施体系，张量在其中充当着最根本的数据载体。整个生态纵贯三层：底层是由 Transfer Engine 驱动的跨异构多级存储（DRAM/VRAM/NVMe）的高性能张量迁移加速；中层依托 Mooncake Store 实现张量级大对象（如 KVCache、模型权重、隐层状态）的全局分布式治理；顶层则由 Mooncake Backend 支持基于张量的高弹性分布式计算。该架构从底层硬件到上层调度均针对张量处理效率进行了端到端极限调优，为大规模模型的推理与训练提供强劲支撑。

### SGLang 官方集成 ([使用指南](https://kvcache-ai.github.io/Mooncake/deployment/integrations/sglang/index.html))

Mooncake 作为核心的高性能通信与存储后端与 [SGLang](https://github.com/sgl-project/sglang/) 实现了深度原生融合。这些集成全方位覆盖了：PD 分离场景下的高效 KV Cache 传输、依托 HiCache 的多级扩展 KV 缓存池、高容错专家并行推理、多模态全流程高速数据流转、以及针对大规模强化学习训练基于 RDMA 的模型权重极速同步。Mooncake 与 SGLang 的强强联合，为业界构建兼具高弹性、超高吞吐与卓越资源利用效率的生产级大语言模型及多模态服务系统奠定了标杆基石。

<details>
<summary>集成细节</summary>

- **PD 分离服务架构**：SGLang 官方原生支持以 Mooncake Transfer Engine 作为分离式推理与 KV Cache 传输的底层核心，让 Prefill 与 Decode 节点能够以极致带宽在不同机器和加速硬件间高效交换 KV 数据。
- **分层 KV 缓存存储（HiCache）**：Mooncake Store 作为 SGLang HiCache 体系的核心外置分布式存储后端，在原有的 RadixAttention 基础上将 KV 缓存容量纵向拓展至主机内存与远端存储节点，突破单机显存容量瓶颈。
- **弹性专家并行（Elastic EP）**：Mooncake 集合通信后端与专家并行算子深度整合进 SGLang，实现了具有局部故障容忍能力的容错专家并行推理（[Elastic EP](https://www.lmsys.org/blog/2026-03-25-eep-partial-failure-tolerance/)）。
- **结合 RBG 的云原生 SGLang HiCache 弹性部署**：通过结合 [RBG](https://github.com/sgl-project/rbg)、SGLang HiCache 与 Mooncake，打造了开箱即用、基于角色解耦的云原生推理部署范式，为生产级大规模高并发推理提供了极佳的弹性扩缩容支持。
- **多模态服务中的 EPD 解耦（编码-预填充-解码解耦）**：SGLang 引入了以 Mooncake 为传输底座的 Encode-Prefill-Decode 分离架构，使 Vision Transformer 等重型多模态编码器与 LLM 语言节点独立部署，海量多模态 Embedding 借由 Mooncake RDMA 引擎实现零拷贝闪电传输。
- **SGLang-Omni 多阶段流水线数据中继**：[SGLang-Omni](https://github.com/sgl-project/sglang-omni) 选用 Mooncake 作为中继后端，负责多模态推理流水线各阶段间张量与大型数据块的高效互通，显著加速了 Thinker（思考）、Talker（表达）、Codec（编解码器）及 Vocoder（声码器）等异构阶段之间的数据流转。
- **面向分布式强化学习基于 RDMA 的 P2P 权重同步**：SGLang 采用 Mooncake TransferEngine 在大规模强化学习集群中进行基于 RDMA 的点对点模型权重同步，支持跨数千张 GPU 的零拷贝极速分发，大幅压降了万亿参数模型的权重同步耗时。

</details>

### vLLM 官方集成 ([使用指南](https://kvcache-ai.github.io/Mooncake/deployment/integrations/vllm/index.html))

Mooncake 与 [vLLM](https://github.com/vllm-project/vllm) 紧密结合，通过极致性能的 KV Cache 跨节点传输与分布式 KV Cache 共享存储，全方位加速大语言模型服务。该集成不仅全面支持 Prefill-Decode 计算解耦架构，更实现了跨实例全局 KV Cache 共享，助力 vLLM 集群显著削减首字延迟（TTFT）、最大化前缀缓存复用率，并在多节点大规模推理集群中达成近乎线性的扩展效率。

<details>
<summary>集成细节</summary>

- **分离式 Prefill-Decode 计算服务**：Mooncake 赋能 vLLM 将预填充（Prefill）与解码（Decode）负载解耦调度至不同物理节点上。通过 `MooncakeConnector`，vLLM 可借助 Mooncake 高性能传输引擎将 KV Cache 块从 Prefill 节点闪电推送到 Decode 节点，使两类计算资源得以按需独立扩缩容，同时将跨节点 KV 传输带来的额外开销降至最低。
- **分布式 KV Cache 池化与多实例共享**：[Mooncake Store 将 vLLM 从单实例隔离的本地缓存延伸至集群级共享缓存池](https://vllm.ai/blog/2026-05-06-mooncake-store)。通过 `MooncakeStoreConnector`，多个并发 vLLM 实例能够基于哈希前缀匹配机制（Prefix Caching）共同存取和复用同一份 KV Cache 块，极大规避了重复的前缀计算开销，特别是在复杂智能体（Agent）工作流与长文本多轮对话场景下表现尤为亮眼。
- **vLLM-Omni 跨阶段通信赋能**：Mooncake 同样深度接入了 [vLLM-Omni](https://github.com/vllm-project/vllm-omni)，借助 `MooncakeTransferEngineConnector` 与 `MooncakeStoreConnector`，为多模态复合流水线的各个计算阶段搭建了超低延迟的跨节点数据交换立交桥。

</details>

<h2 id="ecosystem-integrations">🌐 开源生态集成地图</h2>

Mooncake 广泛连接了国际主流推理引擎、缓存中间件、强化学习框架及云原生部署工具。下表汇集了各项集成的上游指南与开源代码实现链接（核准日期：2026年10月15日）。

<details>
<summary>生态集成目录清单</summary>

**TE** = 传输引擎（Transfer Engine）；**Store** = Mooncake Store 存储引擎；**EP/PG** = 专家并行与分布式进程组后端。各项功能视上游组件版本及具体部署配置而定；在生产部署前请查阅对应关联指南或实现源码。

| 项目名称 | 采用的 Mooncake 组件 | 核心集成应用场景 | 官方指南或代码实现 |
| --- | --- | --- | --- |
| SGLang | TE, Store, EP/PG | PD/EPD 传输、HiCache 分层存储、多模态 Embedding 缓存、权重同步与弹性 EP | [集成指南](https://kvcache-ai.github.io/Mooncake/deployment/integrations/sglang/index.html), [Elastic EP 实现](https://github.com/sgl-project/sglang/pull/10423) |
| vLLM | TE, Store | PD 传输、共享 KV 池化、编码器缓存传输与跨编码器 Embedding 复用 | [PD 连接器](https://docs.vllm.ai/en/latest/features/mooncake_connector_usage/), [Store 连接器](https://docs.vllm.ai/en/latest/features/mooncake_store_connector_usage/), [编码器连接器](https://docs.vllm.ai/en/latest/features/mooncake_ec_connector_usage/) |
| TokenSpeed | TE, Store | PD/EPD 传输以及在主机 KV 缓存之下提供 Store 驱动的 L3 存储层 | [服务器配置手册](https://github.com/lightseekorg/tokenspeed/blob/main/docs/configuration/server.md) |
| TensorRT-LLM | TE, Store | PD 传输以及通过已合入主分支的 KVCMv2 Store 连接器实现共享 KV 池化 | [TE 实现代码](https://github.com/NVIDIA/TensorRT-LLM/tree/main/cpp/tensorrt_llm/executor/cache_transmission/mooncake_utils), [Store 连接器](https://github.com/NVIDIA/TensorRT-LLM/pull/19171) |
| LMDeploy | TE, Store | PD 传输与跨实例 KV 前缀共享复用 | [TE 指南](https://kvcache-ai.github.io/Mooncake/deployment/integrations/lmdeploy.html), [Store 连接器](https://github.com/InternLM/lmdeploy/pull/4903) |
| vLLM-Ascend | TE, Store | 昇腾 NPU 架构下的 PD 传输与依托 AscendStoreConnector 的 KV 缓存池化 | [PD 部署文档](https://github.com/vllm-project/vllm-ascend/blob/main/docs/source/tutorials/features/pd_disaggregation_mooncake_multi_node.md), [KV 缓存池](https://github.com/vllm-project/vllm-ascend/blob/main/docs/source/user_guide/feature_guide/kv_cache_pool_ascend_store.md) |
| FastDeploy | Store | PD 服务中的全局 KV 缓存池化与跨节点复用 | [全局缓存池化指南](https://github.com/PaddlePaddle/FastDeploy/blob/develop/docs/features/global_cache_pooling.md) |
| xLLM | TE, Store | KV 高速传输与分布式多级缓存管理 | [KV 传输后端实现](https://github.com/jd-opensource/xllm/tree/main/xllm/core/kv_cache/transfer) |
| 赤兔 (Chitu) | TE | 基于 RDMA 的 Multi-Prefill / Multi-Decode 多对多 KV 传输 | [PD 分离架构文档](https://github.com/thu-pacman/chitu/blob/public-main/chitu/distributed/pd_disaggregation/README.md) |
| LMCache | TE, Store | 远端 KV 存储、原生多进程 L2 适配器与 P2P 缓存共享 | [Store 指南](https://docs.lmcache.ai/kv_cache/storage_backends/mooncake.html), [MP 适配器](https://docs.lmcache.ai/mp/l2_storage.html#mooncake-store-mooncake-store-native-connector), [P2P 后端](https://github.com/LMCache/LMCache/pull/4182) |
| FlexKV | TE, Store | 分布式缓存复用与基于内容寻址的 REMOTE 存储分层 | [分布式复用指南](https://github.com/taco-project/FlexKV/blob/main/docs/dist_reuse/README_en.md), [Store 后端实现](https://github.com/taco-project/FlexKV/pull/231) |
| NIXL | TE | 面向 NIXL 数据传输框架的 Mooncake 传输插件 | [插件开发文档](https://github.com/ai-dynamo/nixl/blob/main/src/plugins/mooncake/README.md) |
| Dynamo | Store（依托 SGLang HiCache） | 共享缓存池拓扑感知与事件驱动的 KV 缓存智能路由 | [HiCache 集成手册](https://github.com/ai-dynamo/dynamo/blob/main/docs/fern/pages/developer-guide/knowledge-base/modular-components/backends/sglang/hicache.md) |
| llm-d | TE, Store（依托 vLLM） | Kubernetes 环境下的 PD 服务与多层级 KV 缓存卸载 | [PD 集成代码](https://github.com/llm-d/llm-d/pull/1511), [分级前缀缓存手册](https://github.com/llm-d/llm-d/tree/main/guides/tiered-prefix-cache) |
| RBG | TE, Store（依托推理引擎） | 基于角色的 Store、PD 传输与缓存复用编排机制 | [部署范例集](https://github.com/sgl-project/rbg/tree/main/examples/inference/ecosystem/mooncake) |
| srt-slurm | TE, Store（依托推理引擎） | Slurm 超算调度与 Mooncake 服务部署配置 | [Mooncake 服务指南](https://github.com/NVIDIA/srt-slurm/blob/main/docs/mooncake-kv-store.md) |
| NeMo RL | Store（依托 TransferQueue） | 经验生成到模型训练的数据高速通道，支持可选的 GDR 传输 | [数据平面说明](https://github.com/NVIDIA-NeMo/RL/blob/main/nemo_rl/data_plane/README.md), [GDR 支持实现](https://github.com/NVIDIA-NeMo/RL/pull/3501) |
| TransferQueue | Store | 强化学习张量/大对象存储、GDR 传输与 SSD 卸载分流 | [Store 客户端代码](https://github.com/Ascend/TransferQueue/blob/main/transfer_queue/storage/clients/mooncake_client.py), [GDR 配置指南](https://github.com/Ascend/TransferQueue/blob/main/docs/storage_backends/mooncake_gdr.md) |
| verl | TE | 训练节点与 Rollout 采样节点间基于 Checkpoint-engine 的权重同步 | [Mooncake 引擎后端](https://github.com/verl-project/verl/blob/main/verl/checkpoint_engine/mooncake_checkpoint_engine.py) |
| checkpoint-engine | TE | 点对点（P2P）模型权重分发与实时增量更新 | [P2P 后端项目仓库](https://github.com/MoonshotAI/checkpoint-engine) |
| Miles | Store | Rollout 经验采样到模型训练的数据迁移通道 | [Rollout 传输指南](https://github.com/radixark/miles/blob/main/docs/advanced/mooncake-rollout-transfer.md) |
| TorchSpec | Store | 推理实例与草稿模型训练间的隐层状态（Hidden-state）快速中继 | [训练架构说明](https://github.com/torchspec-project/TorchSpec) |
| Speculators | Store | 分布式在线草稿模型训练中的隐层状态交换通道 | [隐层状态连接器](https://github.com/vllm-project/speculators/tree/main/hs_connectors) |
| vLLM-Omni | TE, Store | 多模态异构推理流水线各阶段间的高速数据交换 | [Omni 连接器目录](https://github.com/vllm-project/vllm-omni/tree/main/vllm_omni/distributed/omni_connectors/connectors) |
| LightX2V | TE | 视频生成场景中的编码器与 Transformer 计算解耦 | [传输后端实现](https://github.com/ModelTC/LightX2V/blob/main/lightx2v/disagg/mooncake.py) |
| slime | TE（依托 SGLang） | PD 分离架构下的强化学习 Rollout 采样服务 | [RL 系统架构文档](https://github.com/THUDM/slime/blob/main/docs/en/advanced/rl-systems.md) |

注：Dynamo 条目描述了基于 SGLang HiCache 的共享缓存路由能力，其单独的 [KVBM G4 Store 适配器提案](https://github.com/ai-dynamo/dynamo/pull/9786)目前处于待合入状态。NeMo RL 的 GDR 链路主要优化了客户端张量传输，排队对象仍交由 Mooncake 托管的主机内存管理。

</details>

<h2 id="supported-hardware">🖥️ 硬件支持生态</h2>

Mooncake 支持跨各大算力芯片厂商、云原生网络矩阵以及主流数据中心互联硬件的后端平台，列表如下。技术细节请参见[支持协议说明](https://kvcache-ai.github.io/Mooncake/getting_started/supported-protocols.html)与 [Transfer Engine 架构设计文档](https://kvcache-ai.github.io/Mooncake/design/transfer-engine/index.html)。

| <img src="image/partners/nvidia_logo.png" width="120" alt="NVIDIA"/> | <img src="image/partners/huawei_logo.png" width="120" alt="Huawei"/> | <img src="image/partners/amd_logo.png" width="120" alt="AMD"/> | <img src="image/hardwares/cambricon_logo.png" width="120" alt="Cambricon"/> | <img src="image/partners/moore_thread_logo.jpg" width="120" alt="Moore Threads"/> | <img src="image/partners/aws-logo.png" width="120" alt="AWS"/> |
| --- | --- | --- | --- | --- | --- | --- |
| <img src="image/hardwares/MetaX_logo.png" width="120" alt="MetaX"/> | <img src="image/hardwares/T-Head_logo.png" width="120" alt="T-Head"/> | <img src="image/partners/aliyun_logo.png" width="120" alt="Alibaba Cloud"/> | <img src="image/partners/sunrise_logo.png" width="120" alt="Sunrise"/> | <img src="image/partners/hygon_logo.png" width="120" alt="Hygon"/> | <img src="image/hardwares/biren_logo.png" width="120" alt="Biren Technology"/> |

<h2 id="quick-start">🚀 快速上手</h2>

推荐通过 `pip` 安装 Mooncake。预编译的 `mooncake-transfer-engine` Python 包内嵌包含了 Mooncake Transfer Engine、Mooncake Store 以及 Mooncake EP 与 PG 模块：

- **CUDA < 13.0**：
```bash
pip install mooncake-transfer-engine
```
- **CUDA >= 13.0**：
```bash
pip install mooncake-transfer-engine-cuda13
```

除了 CUDA 架构，Mooncake 亦全面支持其他异构硬件加速后端，并提供极为灵活的编译与生产部署选项。详情请查阅以下官方指引：

- [快速入门指南](https://kvcache-ai.github.io/Mooncake/getting_started/quick-start.html)
- [源码构建手册](https://kvcache-ai.github.io/Mooncake/getting_started/build.html)
- [集群生产部署指南](https://kvcache-ai.github.io/Mooncake/deployment/mooncake-store-deployment-guide.html)

### AI 编程助手专用技能库 (Skills)

Mooncake 在 [`.claude/skills`](.claude/skills) 目录下随仓附带了一整套**开箱即用的内置技能（Skills）** —— 这些是专为特定任务打造的标准剧本，当你的自然语言提问与场景命中时，AI 编程助手（如 Claude Code）将自动调用，亦可通过 Slash 斜杠命令主动触发。

<details>
<summary>技能清单与使用说明</summary>

| 技能命令 | 技能说明 |
|---|---|
| `/mooncake-troubleshoot` | 全面诊断 Mooncake 部署与运行时问题（包含后台服务状态、RDMA 链路、环境变量与系统日志排查）。 |
| `/mooncake-ci-local` | 通过 `scripts/run_ci_test.sh` 执行 PR 提交前的本地轻量化验证。 |
| `/mooncake-api` | 交互式指引与调用 Mooncake Store、Transfer Engine 以及 EP/Backend 的 Python API。 |

无需完整克隆整个代码仓库，即可直接通过 [Claude Code 插件市场](https://code.claude.com/docs/en/plugin-marketplaces)一键安装：

```text
/plugin marketplace add kvcache-ai/Mooncake --sparse .claude-plugin
/plugin install mooncake-troubleshoot@mooncake
/plugin install mooncake-ci-local@mooncake
/plugin install mooncake-api@mooncake
```

其中 `--sparse .claude-plugin` 参数仅拉取插件市场的目录索引，且各个插件均以 `git-subdir` 独立源发布，因此安装某项技能时只会精准拉取该技能子目录，绝不会拉取整个庞大仓库。若你已经在 Mooncake 代码仓库中工作，`.claude/skills/` 下的所有技能将自动生效，无需任何额外配置。

</details>

<h2 id="trace">📦 开源请求轨迹 (Traces) 与评估工具</h2>

我们公开发布了经过严格脱敏的真实请求访问轨迹数据，其中记录了请求到达时间戳、输入与输出 Token 规模统计、以及重映射后的 KV 块哈希值。该轨迹旨在保护真实用户隐私的同时，为社区重现真实的缓存行为与学术模拟评测提供标准参考基准。发布轨迹及详细说明请见 [FAST25-release](FAST25-release)。

随同轨迹数据，我们还配套开源了两款实用的 KV Cache 性能与容量分析工具：
- [KV Cache 显存容量计算器 (KV Cache Size Calculator)](https://kvcache.ai/tools/kv-cache-size-calculator/)：用于精确核算主流开源大模型架构下的 KV 缓存容量需求；
- [KV Cache 命中率模拟器 (KV Cache Hit Rate Simulator)](https://kvcache.ai/tools/kv-cache-hit-rate-simulator/)：用于在多样化业务负载与模型配置下模拟 KV Cache 命中率，辅助系统容量科学规划。

上述工具协助开发者在分析或复现推理服务负载时，更清晰地评估 KV Cache 的存储成本与缓存收益。工具源码开源于 [kvcache-blog 仓库](https://github.com/kvcache-ai/kvcache-blog)。

<h2 id="citation">📑 学术引用</h2>

如果 Mooncake 的技术设计、论文或开源请求轨迹对你的研究工作有所启发或帮助，欢迎引用我们的学术论文：

```bibtex
@inproceedings{qin2025mooncake,
  author    = {Ruoyu Qin and Zheming Li and Weiran He and Jialei Cui and Feng Ren and Mingxing Zhang and Yongwei Wu and Weimin Zheng and Xinran Xu},
  title     = {Mooncake: Trading More Storage for Less Computation {\textemdash} A {KVCache-centric} Architecture for Serving {LLM} Chatbot},
  booktitle = {23rd USENIX Conference on File and Storage Technologies (FAST 25)},
  year      = {2025},
  isbn      = {978-1-939133-45-8},
  address   = {Santa Clara, CA},
  pages     = {155--170},
  url       = {https://www.usenix.org/conference/fast25/presentation/qin},
  publisher = {USENIX Association},
  month     = {feb},
}
```

<details>
<summary>查看更多相关学术论文引用</summary>

```bibtex
@misc{ren2026tentdeclarativeslicespraying,
  title     = {TENT: A Declarative Slice Spraying Engine for Performant and Resilient Data Movement in Disaggregated LLM Serving},
  author    = {Feng Ren and Ruoyu Qin and Teng Ma and Shangming Cai and Zheng Liu and Chao Lei and Dejiang Zhu and Ke Yang and Zheming Li and Jialei Cui and Weixiao Huang and Yikai Zhao and Yineng Zhang and Hao Wu and Xiang Gao and Yuhao Fu and Jinlei Jiang and Yongwei Wu and Mingxing Zhang},
  year      = {2026},
  eprint    = {2604.00368},
  archivePrefix = {arXiv},
  primaryClass  = {cs.DC},
  url       = {https://arxiv.org/abs/2604.00368},
}

@article{sun2026survivingpartialrankfailures,
  title     = {Surviving Partial Rank Failures in Wide Expert-Parallel MoE Inference},
  author    = {Xun Sun and Shaoyuan Chen and Pingchuan Ma and Yue Chen and Ziwei Yuan and Zhanhao Cao and Han Han and Shangming Cai and Teng Ma and Xuchun Shang and Xinpeng Zhao and Ke Yang and Junlin Wei and Lianzhi Lin and Yuji Liu and Feng Ren and Haoran Hu and Cheng Wan and Yingdi Shan and Yongwei Wu and Mingxing Zhang},
  year      = {2026},
  url       = {https://arxiv.org/abs/2605.10670},
}

@article{qin2025mooncake_tos,
  author    = {Qin Ruoyu and Li Zheming and He Weiran and Cui Jialei and Tang Heyi and Ren Feng and Ma Teng and Cai Shangming and Zhang Yineng and Zhang Mingxing and Wu Yongwei and Zheng Weimin and Xu Xinran},
  title     = {Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving},
  year      = {2025},
  publisher = {Association for Computing Machinery},
  address   = {New York, NY, USA},
  issn      = {1553-3077},
  url       = {https://doi.org/10.1145/3773772},
  doi       = {10.1145/3773772},
  journal   = {ACM Trans. Storage},
  month     = {nov},
  keywords  = {Machine learning system, LLM serving, KVCache},
}

@article{qin2024mooncake_arxiv,
  title  = {Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving},
  author = {Ruoyu Qin and Zheming Li and Weiran He and Mingxing Zhang and Yongwei Wu and Weimin Zheng and Xinran Xu},
  year   = {2024},
  url    = {https://arxiv.org/abs/2407.00079},
}
```

</details>

---

> **免责声明**：本简体中文文档由社区志愿者出于交流学习目的进行翻译维护。英文原版文档始终作为项目最权威的单一事实来源（Single Source of Truth）；如中文翻译与英文原版存在任何语义或版本脱节，请以 [英文原版 README](https://github.com/kvcache-ai/Mooncake/blob/main/README.md) 为准。
