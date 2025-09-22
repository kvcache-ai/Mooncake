# Mooncake x LMCache: Unite to Pioneer KVCache-Centric LLM Serving System

Mooncake and LMCache have announced a strategic collaboration aimed at pioneering a KVCache-centric Large Language Model (LLM) serving system. This partnership seeks to significantly enhance the efficiency, scalability, and responsiveness of LLM applications.

By combining LMCache’s advanced KVCache management techniques with Mooncake’s powerful and optimized backend infrastructure, the collaboration aims to improve the capability of LLM serving systems to efficiently handle diverse workloads and meet demanding latency requirements.

## About Mooncake

Mooncake originated as the serving platform for Kimi, a prominent LLM chatbot service from Moonshot AI. It utilizes a KVCache-centric, disaggregated architecture that substantially boosts throughput by effectively separating Prefill and Decode operations and optimizing KVCache reuse. After integrating contributions from multiple infrastructure providers, Mooncake has become an open-source platform, integrated with popular systems such as vLLM (via LMCache) and SGLang, establishing itself as a critical component within the open-source LLM serving ecosystem.

## About LMCache

LMCache is an open-source Knowledge Delivery Network (KDN), specifically designed to accelerate LLM applications by up to eightfold while also substantially reducing costs. It achieves this by storing Key-Value (KV) caches of reusable texts across multiple storage layers, facilitating effective reuse of caches across various serving instances. This strategy significantly cuts down GPU resource usage and reduces response times, particularly in long-context applications.

## Collaboration Highlights

This strategic alliance delivers substantial mutual technological advancements:

*   **LMCache Integrates Mooncake for Enhanced Performance**: LMCache leverages Mooncake’s advanced transfer engine and KVCache storage solutions, significantly enhancing data distribution efficiency and overall system performance.
*   **Mooncake Integrates LMCache for Enhanced Functionality**: Mooncake now includes LMCache as its KVCache management layer, benefiting from LMCache's flexible cache control mechanisms and future-oriented functionalities, such as CacheBlend. This integration enables Mooncake to offer more sophisticated features and improved operational flexibility.

## Getting Started

For a complete deployment guide with step-by-step instructions, see:

👉 **[vLLM V1 Disaggregated Serving with Mooncake Store and LMCache](vllm-integration/vllmv1-lmcache-integration.md)**

## Performance Benchmarking and Results

To illustrate the benefits of this collaboration, a comprehensive performance evaluation of the integrated vLLM, LMCache, and Mooncake Store was conducted. The selected experimental conditions simulate realistic LLM deployment scenarios, specifically comparing the initial cold start with subsequent cache-hit performance to demonstrate the advantages of KVCache reuse.

### Experimental Setup

*   **Hardware:** 8 × H800 GPUs
*   **Model:** Qwen2.5-72B-Instruct (non-quantized)
*   **Workload:** 50 requests, each with an input of 9,728 tokens and generating 64 tokens per request, without concurrency limits.
*   **Cache Configuration:** LMCache’s local CPU cache was disabled to ensure a direct assessment of Mooncake Store’s effectiveness.

### Performance Metrics and Improvements

The test revealed significant performance enhancements under cache-hit conditions:

| Metric                       | Cold Start (First Round) | Cache Hit (Second Round) | **Improvement** |
|------------------------------|--------------------------|--------------------------|-----------------|
| Average TTFT                 | 21,707.62 ms             | 6,708.39 ms              | **↓ 69.1%**     |
| P50 TTFT                     | 22,102.51 ms             | 7,253.38 ms              | **↓ 67.2%**     |
| P90 TTFT                     | 38,170.54 ms             | 11,128.26 ms             | **↓ 70.9%**     |
| Average TPOT                 | 368.12 ms                | 140.17 ms                | **↓ 61.9%**     |
| P50 TPOT                     | 362.08 ms                | 132.98 ms                | **↓ 63.3%**     |
| P90 TPOT                     | 632.90 ms                | 221.93 ms                | **↓ 64.9%**     |
| Request Throughput (req/s)   | 1.11                     | 3.23                     | **↑ 191.0%**    |
| Output Token Throughput (tok/s)| 71.24                    | 202.91                   | **↑ 184.8%**    |
| Total Token Throughput (tok/s)| 10,899.84                | 31,665.01                | **↑ 190.5%**    |

These results clearly illustrate how the collaborative integration of LMCache and Mooncake **substantially improves latency, throughput, and overall system efficiency** through **KVCache reuse**.

## Future Developments

Moving forward, LMCache and Mooncake plan to collaborate closely on several key improvements highly valued by the community, including:

*   Developing **optimized KVCache eviction and placement strategies** to maximize throughput.
*   Implementing **asynchronous KVCache scheduling** and **zero-copy KVCache transferring mechanisms** to minimize costs through effective prefetching and offloading to cheaper storage tiers.
*   **Expanding caching strategies beyond simple prefix matching**, enhancing KVCache reusability by supporting more flexible matching patterns.

This strategic partnership represents a significant advancement toward fully realizing the potential of next-generation LLM serving architectures.
