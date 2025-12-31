# etcd热备架构图表说明

本文档包含基于当前代码实现的etcd热备架构的PlantUML图表。

## 图表文件

### 1. `etcd-hot-standby-architecture.puml` - 整体架构图

展示了etcd热备系统的整体架构，包括：

- **Primary Master组件**:
  - `MasterService`: 核心服务，处理客户端请求
  - `OpLogManager`: 生成和管理OpLog
  - `EtcdOpLogStore`: 将OpLog写入etcd

- **Standby Master组件**:
  - `HotStandbyService`: Standby服务主控制器
  - `OpLogWatcher`: 从etcd监听OpLog变化
  - `OpLogApplier`: 应用OpLog到本地metadata store
  - `StandbyMetadataStore`: Standby的metadata存储

- **协调组件**:
  - `MasterServiceSupervisor`: 管理Primary/Standby切换
  - `MasterViewHelper`: 处理Leader选举
  - `EtcdHelper`: etcd操作的C++ wrapper

- **etcd存储**:
  - OpLog存储: `/oplog/{cluster_id}/{sequence_id}`
  - Leader选举: `/mooncake-store/{cluster_id}/master_view`

### 2. `etcd-hot-standby-sequence.puml` - 时序图

展示了关键流程的时序关系，包括：

1. **Primary启动流程**:
   - Leader选举
   - MasterService初始化
   - OpLogManager设置EtcdOpLogStore

2. **Standby启动流程**:
   - 检测已有Leader
   - 热启动 vs 冷启动
   - 快照加载(可选)
   - 历史OpLog读取
   - Watch启动

3. **写入操作流程**:
   - 客户端写入请求
   - Primary生成OpLog
   - 写入etcd
   - Standby接收并应用

4. **故障切换流程**:
   - Leader lease过期检测
   - Standby最终同步
   - 重新选举
   - 新Primary初始化

### 3. `etcd-hot-standby-flow.puml` - 流程图

展示了数据流和控制流，包括：

1. **OpLog写入流程**: Primary如何生成和写入OpLog
2. **Standby同步流程**: Standby如何启动和同步数据
3. **OpLog应用流程**: Standby如何应用OpLog(包括乱序处理)
4. **故障切换流程**: Standby如何提升为Primary
5. **OpLog清理流程**: 如何清理etcd中的旧OpLog
6. **批量更新流程**: latest_sequence_id的批量更新机制

## 关键设计点

### 1. 顺序保证
- 使用全局`sequence_id`保证OpLog顺序
- Standby通过`expected_sequence_id`检测乱序
- 乱序的OpLog进入`pending_entries_`队列
- 缺失的OpLog从etcd主动请求

### 2. 一致性保证
- 使用etcd revision实现"read then watch"的一致性
- `ReadOpLogSinceWithRevision`返回revision
- Watch从`revision + 1`开始，确保不丢失事件

### 3. 性能优化
- `latest_sequence_id`批量更新(每100条或每1秒)
- OpLog读取使用分页(每批1000条)
- 使用固定宽度sequence_id确保etcd key的字典序

### 4. 故障恢复
- Standby提升前进行最终同步
- 新Primary从Standby的metadata快照恢复
- OpLog sequence_id连续，避免回退

## 使用方法

### 查看图表

1. **在线查看**: 使用PlantUML在线服务器
   - 访问: http://www.plantuml.com/plantuml/uml/
   - 复制`.puml`文件内容粘贴查看

2. **VS Code插件**: 安装PlantUML插件
   - 插件: `PlantUML`
   - 打开`.puml`文件，按`Alt+D`预览

3. **命令行工具**: 使用PlantUML命令行工具
   ```bash
   java -jar plantuml.jar etcd-hot-standby-architecture.puml
   ```

### 导出图片

```bash
# 导出为PNG
java -jar plantuml.jar -tpng *.puml

# 导出为SVG
java -jar plantuml.jar -tsvg *.puml

# 导出为PDF
java -jar plantuml.jar -tpdf *.puml
```

## 相关文档

- [RFC: etcd热备完整方案](../rfc-oplog-hot-standby-complete.md)
- [实现计划](../rfc-oplog-implementation-plan.md)

