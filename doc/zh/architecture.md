# Mooncake Architecture

Mooncake 旨在通过在高速互联的 DRAM/SSD 资源上构建一个多级缓存池，来提升大型语言模型（LLM）等场景下的推理效率，尤其是在慢速对象存储环境中。与传统缓存系统相比，Mooncake 的独特之处在于，它能够利用 (GPUDirect) RDMA 技术，以零拷贝的方式将数据从发起端的 DRAM/VRAM 直接传输到接收端的 DRAM/VRAM，同时最大限度地利用单机多网卡资源。

- 提供对象级别的数据存储服务
- 支持在缓存层多副本保存数据，提供slice级别的分布保证和尽力而为的分配策略，由于不保证绝对的高可用，因此系统设计更为轻量化
- 保证对象写操作的原子性，即 Get 一定会读到某次 Put 生成的完整的数据，但不一定是最新的
- 支持对较大的对象进行条带化和并行 I/O 传输，以利用多网卡的聚合带宽
- 支持 Eager/Lazy/None 三种下刷慢速对象存储的模式，分别对应持久化要求从高到低的对象
- 支持动态增删缓存资源

## 架构概览
![architecture](../../image/mooncake-store.png)
- Mooncake 对外提供对象（Object）级别的 Get/Put/List/Del 等操作，同时支持按用户要求动态调整复制策略等（Replicate 操作）；
- Mooncake 支持基于 VRAM/DRAM/NVMe SSD 等存储介质的数据传输，同时尽可能实现零拷贝和多网卡池化数据传输。相应逻辑已抽象为 Transfer Engine 子系统，目前已完全开源；
- Master 节点集中管理对象（Object）到 VRAM/DRAM/NVM 缓冲区（Buffer）的映射关系及空间管理策略。同时，Master 节点通过调用 Transfer Engine 的相关接口，驱动 Managed Pool Buffer 节点实现数据传输；
- Managed Pool Buffer 节点主要提供存储空间，所有的对象最终均会按照特定规则写入此类节点所分配的存储空间内。

> Mooncake 目前开源了位于下层的 Transfer Engine 子系统，后续更新敬请期待！
