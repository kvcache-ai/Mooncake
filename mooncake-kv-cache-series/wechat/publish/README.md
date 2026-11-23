# Mooncake KV Cache 微信公众号发布稿

本目录由 `wechat/build_publish.py` 生成。已生成的篇目包含 Markdown、内联样式 HTML 与配图 PNG；其余篇目保留源 Markdown。

基线：`v0.3.13.post1` / `7197358`。生成时间：2026-11-23T09:42:25+00:00。

## 系列目录（11 篇）

| 篇次 | 文章 | 状态 | Markdown | HTML | 配图 |
|---|---|---|---|---|---|
| 1 | 第1篇｜两台推理实例，怎么复用同一段 KV Cache？ | 已生成 | [MD](./01-shared-kv-cache.md) | [HTML](./01-shared-kv-cache.html) | 2 |
| 2 | 第2篇｜一个 KV Cache 对象，在 Mooncake 里到底怎么放？ | 已生成 | [MD](./02-objects-and-replicas.md) | [HTML](./02-objects-and-replicas.html) | 2 |
| 3 | 第3篇｜字节已经写完，为什么另一个 Client 还读不到？ | 已生成 | [MD](./03-put-visibility.md) | [HTML](./03-put-visibility.html) | 1 |
| 4 | 第4篇｜字节读回来了，这次 Get 为什么还可能失败？ | 已生成 | [MD](./04-get-and-lease.md) | [HTML](./04-get-and-lease.html) | 2 |
| 5 | 第5篇｜读 Mooncake 源码，从哪里开始才不容易迷路？ | 源稿 | [源稿](../05-source-reading.md) | — | 1 |
| 6 | 第6篇｜Transfer Engine 返回成功，数据就传完了吗？ | 源稿 | [源稿](../06-transfer-lifecycle.md) | — | 2 |
| 7 | 第7篇｜一次 RDMA 传输，究竟经历了什么？ | 源稿 | [源稿](../07-rdma-data-path.md) | — | 1 |
| 8 | 第8篇｜网卡越多就越快？Mooncake 如何面对 NUMA 和多网卡 | 源稿 | [源稿](../08-multi-nic-numa.md) | — | 2 |
| 9 | 第9篇｜缓存满了，哪些对象能删？理解 Lease、Pin 与淘汰 | 源稿 | [源稿](../09-cache-eviction.md) | — | 1 |
| 10 | 第10篇｜写进 SSD 就安全了吗？Mooncake 分层缓存的三个完成点 | 源稿 | [源稿](../10-ssd-tiering.md) | — | 2 |
| 11 | 第11篇｜共享了 KV Cache，为什么推理不一定更快？ | 源稿 | [源稿](../11-kv-reuse-and-inference.md) | — | 1 |

## 构建与校验

```bash
cd mooncake-kv-cache-series/wechat
python3 build_publish.py --articles 1,2,3,4 \
  --md2wx /home/yangxx/.claude/skills/producing-tech-blog/scripts/md2wx.py \
  --chrome /usr/bin/google-chrome --renderer npx
```

脚本只替换 Mermaid 图与相对链接，不改写正文。`manifest.json` 记录每篇的产物、配图尺寸与 md2wx 报告。

## 未执行项

- 未在公众号编辑器中粘贴预览（非 GUI 环境）。
- 未做手机宽度下的人工可读性检查；配图宽高与是否空白由脚本校验。
- 未上传图片到公众号素材库，也未发布。
- GitHub 永久链接按本地 tag 的 commit 构造，未做 HTTP 可达性校验。
