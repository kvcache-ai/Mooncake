# Mooncake 最新 PR 总结

本文档按照 PR ID 大小顺序，总结最新的一些 PR 中的内容。

## MoonCake Transfer Engine 部分

1. PR1333, [TENT] 修复设备宕机时的崩溃问题
2. PR1366, [TE] 支持通过环境变量配置本地通信资源
3. PR1375, [TE] 增加 IB 设备可用性过滤功能
4. PR1393, [TE] 修复 RDMA 设备路径映射不正确的问题
5. PR1395, [Doc] 为传输引擎增加批处理 (Batch) API 文档和示例

## Mooncake Object Store 部分

1. PR1347, [Store][Feature] 引入 DataManager 以统一本地和远程数据访问
2. PR1360, [Store][FIX] 修复 put 操作时 preferred segments 不生效的问题
3. PR1362, [Store][Feature] 为 P2P 架构提取 rpc_service 和 master_client 的公共接口
