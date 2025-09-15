# Welcome to Mooncake

:::{figure} ./image/mooncake-icon.png
:align: center
:alt: Mooncake
:class: no-scaled-link
:width: 60%
:::

:::{raw} html
<p style="text-align:center">
<strong>A KVCache-centric Disaggregated Architecture for LLM Serving.
</strong>
</p>

<p style="text-align:center">
<script async defer src="https://buttons.github.io/buttons.js"></script>
<a class="github-button" href="https://github.com/kvcache-ai/Mooncake" data-show-count="true" data-size="large" aria-label="Star">Star</a>
<a class="github-button" href="https://github.com/kvcache-ai/Mooncake/subscription" data-icon="octicon-eye" data-size="large" aria-label="Watch">Watch</a>
<a class="github-button" href="https://github.com/kvcache-ai/Mooncake/fork" data-icon="octicon-repo-forked" data-size="large" aria-label="Fork">Fork</a>
</p>
:::

Mooncake is the serving platform for  <a href="https://kimi.ai/">Kimi</a>, a leading LLM service provided by <a href="https://www.moonshot.cn/">Moonshot AI</a>.
Now both the Transfer Engine and Mooncake Store are open-sourced!
This repository also hosts its technical report and the open sourced traces. 

<h2 id="updates">🔄 Updates</h2>

 - **Sept 10, 2025**: SGLang officially supports Mooncake Store as a [hierarchical KV caching storage backend](https://lmsys.org/blog/2025-09-10-sglang-hicache/). The integration extends RadixAttention with multi-tier KV cache storage across device, host, and remote storage layers.
 - **Sept 10, 2025**: The official & high-performance version of Mooncake P2P Store is open-sourced as [checkpoint-engine](https://github.com/MoonshotAI/checkpoint-engine/). It has been successfully applied in K1.5 and K2 production training, updating Kimi-K2 model (1T parameters) across thousands of GPUs in ~20s.
 - **Aug 23, 2025**: [xLLM](https://github.com/jd-opensource/xllm) high-performance inference engine builds hybrid KV cache management based on Mooncake, supporting global KV cache management with intelligent offloading and prefetching.
 - **Aug 18, 2025**: vLLM-Ascend [integrates Mooncake Transfer Engine](https://github.com/vllm-project/vllm-ascend/blob/main/examples/disaggregated_prefill_v1/mooncake_connector_deployment_guide.md) for KV cache register and disaggregate prefill, enabling efficient distributed inference on Ascend NPUs.
 - **Jul 20, 2025**: Mooncake powers [the deployment of Kimi K2](https://lmsys.org/blog/2025-07-20-k2-large-scale-ep/) on 128 H200 GPUs with PD disaggregation and large-scale expert parallelism, achieving 224k tokens/sec prefill throughput and 288k tokens/sec decode throughput.
 - **Jun 20, 2025**: Mooncake becomes a PD disaggregation [backend](https://github.com/kvcache-ai/Mooncake/blob/main/doc/en/lmdeploy-integration-v0.9.md) for LMDeploy.
 - **May 9, 2025**: NIXL officially supports Mooncake Transfer Engine as [a backend plugin](https://github.com/ai-dynamo/nixl/blob/main/src/plugins/mooncake/README.md).
 - **May 8, 2025**: Mooncake x LMCache <a href="https://github.com/kvcache-ai/Mooncake/blob/main/doc/en/lmcache-integration.md" target="_blank">unite</a> to pioneer KVCache-centric LLM serving system.
 - **May 5, 2025**: Supported by Mooncake Team, SGLang release <a href="https://lmsys.org/blog/2025-05-05-large-scale-ep/" target="_blank">guidance</a> to deploy DeepSeek with PD Disaggregation on 96 H100 GPUs.
 - **Apr 22, 2025**: LMCache officially supports Mooncake Store as a <a href="https://blog.lmcache.ai/2025-04-22-tencent/" target="_blank">remote connector</a>.
 - **Apr 10, 2025**: SGLang officially supports Mooncake Transfer Engine for disaggregated prefilling and KV cache transfer.
 - **Mar 7, 2025**: We open sourced the Mooncake Store, a distributed KVCache based on Transfer Engine. vLLM's xPyD disaggregated prefilling & decoding based on Mooncake Store will be released soon.
 - **Feb 25, 2025**: Mooncake receives the **Best Paper Award** at **FAST 2025**!
 - **Feb 21, 2025**: The updated <a href="https://github.com/kvcache-ai/Mooncake/tree/main/FAST25-release/traces" target="_blank">traces</a> used in our FAST'25 paper have been released.
 - **Dec 16, 2024**: vLLM officially supports Mooncake Transfer Engine for disaggregated prefilling and KV cache transfer.
 - **Nov 28, 2024**: We open sourced the Transfer Engine, the central component of Mooncake. We also provide two demonstrations of Transfer Engine: a P2P Store and vLLM integration.
 - **July 9, 2024**: We open sourced the trace as a <a href="https://github.com/kvcache-ai/Mooncake/blob/main/FAST25-release/arxiv-trace/mooncake_trace.jsonl" target="_blank">jsonl file</a>.
 - **June 27, 2024**: We present a series of Chinese blogs with more discussions on <a href="https://zhuanlan.zhihu.com/p/705754254">zhihu 1</a>, <a href="https://zhuanlan.zhihu.com/p/705910725">2</a>, <a href="https://zhuanlan.zhihu.com/p/706204757">3</a>, <a href="https://zhuanlan.zhihu.com/p/707997501">4</a>.
 - **June 26, 2024**: Initial technical report release.

## Documentation

% How to start using Mooncake?

:::{toctree}
:caption: Getting Started
:maxdepth: 2

getting_started/build
getting_started/quick-start
getting_started/plugin-usage/3FS-USRBIO-Plugin
getting_started/examples/lmcache-integration
getting_started/examples/lmdeploy-integration-v0.9
getting_started/examples/sglang-integration-v1
getting_started/examples/sglang-integration/hicache-integration-v1
getting_started/examples/vllm-integration/index
:::

% Making the most out of Mooncake

:::{toctree}
:caption: Performance
:maxdepth: 1

performance/sglang-benchmark-results-v1
performance/vllm-benchmark-results-v0.2
performance/vllm-benchmark-results-v1
performance/sglang-hicache-benchmark-results-v1
performance/allocator-benchmark-result.md
:::

% API Documentation

:::{toctree}
:caption: Python API Reference
:maxdepth: 1

python-api-reference/mooncake-store
python-api-reference/transfer-engine
:::

% Explanation of Mooncake internals

:::{toctree}
:caption: Design Documents
:maxdepth: 1

design/architecture
design/mooncake-store
design/p2p-store
design/transfer-engine
:::

% Q&A for Mooncake

:::{toctree}
:caption: Troubleshooting
:maxdepth: 1

troubleshooting/error-code
troubleshooting/troubleshooting
:::
