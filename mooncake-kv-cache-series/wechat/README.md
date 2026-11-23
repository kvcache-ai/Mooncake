# Mooncake KV Cache 微信公众号系列

本目录保存从聊天发布稿整理的 11 篇 Markdown 文章，采用公众号阅读顺序独立编号。源码解析版保留在[上一级目录](../README.md)，其篇号与本目录不同。

稿件减少代码行号和长代码块，以问题、图解、设计取舍和故障场景组织内容。本文档是编辑说明，不是公众号文章正文。

## 阅读与发布顺序

| 篇次 | 文章 | 对应源码解析版 |
|---|---|---|
| 1 | [两台推理实例，怎么复用同一段 KV Cache？](01-shared-kv-cache.md) | [篇0：架构总览](../00-architecture-overview.md) |
| 2 | [一个 KV Cache 对象，在 Mooncake 里到底怎么放？](02-objects-and-replicas.md) | [篇4：核心数据结构](../04-core-data-structures.md) |
| 3 | [字节已经写完，为什么另一个 Client 还读不到？](03-put-visibility.md) | [篇2：Put 路径](../02-put-path.md) |
| 4 | [字节读回来了，这次 Get 为什么还可能失败？](04-get-and-lease.md) | [篇3：Get 路径](../03-get-path.md) |
| 5 | [读 Mooncake 源码，从哪里开始才不容易迷路？](05-source-reading.md) | [篇1：源码导航](../01-source-navigation.md) |
| 6 | [Transfer Engine 返回成功，数据就传完了吗？](06-transfer-lifecycle.md) | [篇5：TE 生命周期](../05-transfer-engine-task-lifecycle.md) |
| 7 | [一次 RDMA 传输，究竟经历了什么？](07-rdma-data-path.md) | [篇6：RDMA 数据路径](../06-rdma-data-path.md) |
| 8 | [网卡越多就越快？Mooncake 如何面对 NUMA 和多网卡](08-multi-nic-numa.md) | [篇7：多网卡与 NUMA](../07-multi-nic-numa.md) |
| 9 | [缓存满了，哪些对象能删？理解 Lease、Pin 与淘汰](09-cache-eviction.md) | [篇8：缓存生命周期](../08-cache-lifecycle.md) |
| 10 | [写进 SSD 就安全了吗？Mooncake 分层缓存的三个完成点](10-ssd-tiering.md) | [篇9：SSD 分层](../09-ssd-tiering.md) |
| 11 | [共享了 KV Cache，为什么推理不一定更快？](11-kv-reuse-and-inference.md) | [篇10：vLLM 集成](../10-vllm-integration.md) |

## 公众号排版

- 标题填入公众号标题栏，正文不重复文章一级标题；摘要可用作公众号摘要字段。
- 正文建议 16px、行距 1.75、深灰文字；小标题统一蓝色，保持两级层次。
- 一段解释一个意思，保留短段落和适量加粗，不把整段标成重点。
- 图注建议 14px 灰色；如增加代码，使用浅灰底并避免横向滚动。
- 正文不堆文件路径与行号；文末源码延伸指向源码解析版，保留查证入口。
- 仓库中的上一篇、下一篇为相对路径；公众号发布时替换为已发布文章的真实链接。

## 配图使用

11 篇文章共含 17 个 Mermaid 配图源稿，均放在相应正文的位置。需在支持 Mermaid 的阅读器中渲染；公众号编辑器不能直接使用 Mermaid 代码块。

发布前将图导出为清晰的 PNG，上传到公众号素材库后插入对应位置。优先纵向布局，时序图控制参与方数量，统一字体、白色背景与角色配色。缩放到手机正文宽度后检查文字是否无需放大就能阅读。

“配图”说明用于标识插图位置；发布时用导出的图片替换代码块，并保留有解释作用的图注。原理图不表达未经验证的性能倍数。

第 1–4 篇已由 `build_publish.py` 生成 `publish/` 目录下的发布稿 Markdown、内联 CSS HTML 与 PNG；第 5–11 篇仍为源稿，尚未生成对应产物。本轮未向公众号发布，未做微信编辑器粘贴预览，也未上传公众号素材库。

## 版本与证据边界

Mooncake 基线为原系列采用的 `v0.3.13.post1`，不是对当前 main 的逐行行为承诺。详细的历史行号和源码入口保留在源码解析版；本次稿件整理未新增历史版本构建、硬件测试或端到端验证。

第11篇限于共享 KV 存储与推理复用的职责和成本模型，未固定并验证 vLLM commit。具体 connector 的接口调用、调度通知、布局转换、失败重试与重算行为，仍应按对应版本复核。

引用性能数据时补齐硬件、模型、负载、命中长度、并发与版本；不要从单一带宽数字推导端到端 TTFT 收益。
