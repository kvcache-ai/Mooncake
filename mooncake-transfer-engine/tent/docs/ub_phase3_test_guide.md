# TENT UB Transport Phase 3 — 测试指南

本文档涵盖 Phase 3 所有改动的验证方法，分为**本地 Mock 测试**（无需真实硬件，在 CI 中运行）和**真机集成测试**（需 Kunpeng + URMA 硬件）两部分。

---

## 一、编译前置条件

```bash
# 在 build 目录中配置（需要 USE_UB + USE_TENT 同时开启）
cmake .. \
  -DUSE_UB=ON \
  -DUSE_TENT=ON \
  -DMOCK_URMA=ON \       # 本地 Mock 测试时加此选项
  -DBUILD_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Debug

# 编译目标
cmake --build . --target tent_xport_ub -j4          # 核心库
cmake --build . --target tent_ub_transport_test -j4 # 单元测试
```

---

## 二、Mock 单元测试（无硬件可运行）

> 文件：`mooncake-transfer-engine/tent/tests/ub_tent_transport_test.cpp`

### 2.1 运行方式

```bash
# 在 build 目录
./mooncake-transfer-engine/tent/tests/tent_ub_transport_test
```

或使用 CTest：

```bash
ctest -R tent_ub_transport_test -V
```

### 2.2 覆盖的测试用例

| 测试名称 | 验证点 |
|---|---|
| `UbSelectorTest` | TransportSelector 在策略配置 UB 时能正确选到 UB transport |
| `UbInstallUninstallTest` | `install()` + `uninstall()` 生命周期，Mock URMA 环境下无崩溃 |
| `UbAddRemoveBufferTest` | `addMemoryBuffer()` / `removeMemoryBuffer()` 成功注册 MR，`tseg` 序列化写入 `BufferDesc.transport_attrs[UB]` |
| `UbSubBatchLifecycleTest` | `allocateSubBatch()` / `freeSubBatch()` 正常分配和释放 |
| `UbSubmitMockTransferTest` | Mock 模式下 `submitTransferTasks()` 不返回错误 |
| `UbGetStatusTest` | `getTransferStatus()` 返回合法状态 |

### 2.3 关键检查点（手动查看日志）

运行时应看到以下日志（`GLOG_logtostderr=1`）：

```
I UbTentTransport: installed on segment 'test_node'
I UbTransport: initialize Ub resources done
I UbTransport: allocate local segment done
I UbTransport: start handshake daemon done    # 来自 bridge，实际为 no-op
I UbTentTransport: setupUbLocalSegment()      # 写入 EID 到 TENT segment
```

---

## 三、桥接层（UbTentMetadataBridge）单元验证

> 这部分改动可在 Mock 环境下通过间接行为验证，无需专门的 Bridge 测试二进制。

### 3.1 验证 `startHandshakeDaemon` 变为 no-op

**方法：** 在 Mock 测试中，`install()` 成功后检查 `control_service_` 的 UB bootstrap 回调已被注册（通过 `setBootstrapUbCallback`）。

**预期：** 没有 TCP daemon 启动，没有绑定端口的 log。

### 3.2 验证 `getSegmentDescByID(LOCAL_SEGMENT_ID)` 正常

**方法：** 调用 `addMemoryBuffer()` 后，验证 `BufferDesc.transport_attrs[UB]` 非空（说明 bridge 成功读回了 local segment 里的 tseg）。

**检查代码（伪）：**

```cpp
BufferDesc desc;
desc.addr = reinterpret_cast<uint64_t>(buf);
desc.length = 4096;
ASSERT_TRUE(transport.addMemoryBuffer(desc, options).ok());
// tseg 写入了 transport_attrs
EXPECT_FALSE(desc.transport_attrs.count(TransportType::UB) == 0);
auto tseg_json = desc.transport_attrs.at(TransportType::UB);
EXPECT_FALSE(nlohmann::json::parse(tseg_json).empty());
```

---

## 四、真机集成测试（需 Kunpeng URMA 硬件）

以下测试需要两台配置了 Kunpeng URMA 网卡的节点，分别称为 **节点 A**（sender）和 **节点 B**（receiver）。

### 4.1 环境准备

```bash
# 两台机器均执行
# 1. 确认 URMA 设备可用
ls /dev/urma*        # 应有设备文件

# 2. 加载驱动（如需）
modprobe urma_udrv

# 3. 检查 EID（每块网卡一个）
urma_cmd -q all      # 应显示 EID 列表
```

### 4.2 编译（不加 MOCK_URMA）

```bash
cmake .. \
  -DUSE_UB=ON \
  -DUSE_TENT=ON \
  -DBUILD_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)
```

### 4.3 测试一：UB Local Segment 发布（单节点）

**目的：** 验证 `setupUbLocalSegment()` 正确把 EID 写进 TENT 段。

**步骤：**

```bash
# 节点 A 上
GLOG_logtostderr=1 ./tent/tests/tent_ub_transport_test \
  --gtest_filter="UbInstallUninstallTest"
```

**预期日志：**

```
I UbTentTransport: setupUbLocalSegment — writing N devices
I SegmentManager: synchronizeLocal succeeded
```

**手动检查（gdb 或 instrumentation）：**

1. 在 `setupUbLocalSegment()` 返回后，调用 `control_service_->segmentManager().getLocal()`
2. 取 `MemorySegmentDesc.devices`，每个 device 的 `transport_attrs[UB]` 应等于 `urma_cmd -q` 输出的 EID 字符串

---

### 4.4 测试二：双节点 Metadata 同步

**目的：** 验证节点 A 的 TENT segment（含 tseg/eid）能被节点 B 的 `UbTentMetadataBridge::getSegmentDescByID()` 正确读回。

**步骤：**

```bash
# 节点 A：启动 TENT transfer engine，注册一块内存
./tent/tests/tent_ub_transfer_test --role=server \
  --segment_name=node_a_seg \
  --metastore=etcd://ETCD_IP:2379

# 节点 B：打开节点 A 的 remote segment，验证能拿到 tseg
./tent/tests/tent_ub_transfer_test --role=client \
  --remote_segment=node_a_seg \
  --metastore=etcd://ETCD_IP:2379
```

**预期结果（节点 B 日志）：**

```
I UbTentMetadataBridge: getSegmentDescByID(X) — found in TENT SegmentManager
I convertFromTent: extracted N buffers, M devices
I BufferDesc tseg[0]: <tseg handle string>
I DeviceDesc eid: <eid string matching node A>
```

**失败排查：**

| 现象 | 可能原因 |
|---|---|
| `getSegmentDescByID` 返回 nullptr | TENT segment 未同步；检查 metastore 连通性 |
| tseg 列表为空 | `addMemoryBuffer()` 没有写入 `transport_attrs[UB]`；检查 bridge 的 local segment 读取 |
| eid 字段为空 | `setupUbLocalSegment()` 未能从 `context_list_` 拿到 EID；检查 URMA 初始化 |

---

### 4.5 测试三：UB Bootstrap / Handshake（双节点）

**目的：** 验证 TENT BootstrapUb RPC 替代旧 TCP handshake daemon 正常完成 URMA jetty 交换。

**步骤：**

1. 节点 A 启动，`UbTentTransport::install()` 后 `setBootstrapUbCallback` 已注册
2. 节点 B 发起 `submitTransfer` 到节点 A 的某地址
3. `UbEndPoint::setupConnections()` 触发 `sendHandshake`
4. Bridge 的 `sendHandshake` 调用 `ControlClient::bootstrapUb(node_a_rpc_addr, ...)`
5. 节点 A 的 `ControlService::onBootstrapUb` 被触发，调用注册的回调（`UbTransport::onSetupConnections`）
6. URMA jetty 交换成功

**验证方法（查看 glog）：**

节点 A：
```
I ControlService::onBootstrapUb: received from <node_B_nic_path>
I UbTransport::onSetupConnections: setting up jetty for <node_B_nic_path>
```

节点 B：
```
I UbTentMetadataBridge::sendHandshake: RPC to <node_A_rpc_addr>
I UbEndPoint: setupConnectionsByPassive succeeded
```

**失败排查：**

| 现象 | 可能原因 |
|---|---|
| RPC call 超时 | `rpc_server_addr` 解析错误；检查 TENT segment 中的 `rpc_server_addr` 字段 |
| `onSetupConnections` 未被调用 | `setBootstrapUbCallback` 注册时机在 install 成功后，检查时序 |
| jetty mismatch | `UbBootstrapDesc.jetty_num` 和 `HandShakeDesc.jetty_num` 的转换逻辑，检查 `#ifdef USE_UB` 宏 |

---

### 4.6 测试四：端到端 DRAM→DRAM 传输

**目的：** 完整验证从 `submitTransfer` 到 URMA 数据面写完成的全链路。

**步骤（参考已有 RDMA loopback 测试改写为 UB 版本）：**

```bash
# 节点 A（receiver）
./tent/tests/tent_ub_e2e_test --role=receiver \
  --segment=node_a --transport=ub \
  --metastore=etcd://ETCD_IP:2379

# 节点 B（sender）
./tent/tests/tent_ub_e2e_test --role=sender \
  --remote_segment=node_a --transport=ub \
  --size=1048576 \                   # 1MB
  --metastore=etcd://ETCD_IP:2379
```

**预期：**

```
Sender: transfer completed, 1048576 bytes, status=COMPLETED
Receiver: data verified OK
```

**Fallback 验证（测试 submit-fallback 改动）：**

1. 在节点 B 上用 `--transport=ub,tcp` 同时启用 UB 和 TCP
2. 断开 URMA 链路（拔网线或 `ip link set dev urma0 down`）
3. 触发 `submitTransfer`
4. 观察日志：应看到 UB submit 失败 → `failover_count=0` 重置 → 下一次 poll 触发 `resubmitTransferTask` → 切换到 TCP

```
W UbTentTransport: submitTransferTask failed: ...
I TransferEngineImpl: Transport failover: UB -> TCP (attempt 1/3)
I Transfer completed via TCP
```

---

## 五、回归测试（确保老 TE 接口不受影响）

Phase 3 在 `transfer_metadata.h` 中对几个方法加了 `virtual`，需要确认旧 TE 功能正常。

```bash
# 编译并运行老 TE 的单元测试
cmake --build . --target transfer_engine -j4
ctest -R transport_uint_test -V
ctest -R rdma_transport_test -V
```

**预期：** 所有旧测试 PASS，无回归。

---

## 六、测试矩阵汇总

| 测试 | 需要硬件 | 可在 CI 中跑 | 对应 todo |
|---|---|---|---|
| Mock install/uninstall | 否（MOCK_URMA） | ✅ | `install-refactor` |
| tseg 写入 TENT BufferDesc | 否（MOCK_URMA） | ✅ | `add-buffer-tseg` |
| Local segment EID 发布 | 是（URMA 网卡） | ❌ | `setup-local-segment` |
| 双节点 metadata 同步 | 是 | ❌ | `bridge-class` |
| BootstrapUb RPC handshake | 是 | ❌ | `ub-bootstrap` |
| getTESegmentID 无 fallback | 是 | ❌ | `segment-id-fix` |
| UB→TCP fallback | 是（或 Mock 注入错误） | ⚠️ 部分 | `submit-fallback` |
| 老 TE 回归 | 否 | ✅ | — |

---

## 七、常用调试命令

```bash
# 打开所有 glog 日志
GLOG_logtostderr=1 GLOG_v=2 ./your_test_binary

# 检查 TENT segment JSON（发布后）
# 在 etcd 中查询（若使用 etcd metastore）
etcdctl get /mooncake/segment/ --prefix

# 验证 UB transport_attrs 字段存在
etcdctl get /mooncake/segment/node_a_seg | python3 -m json.tool | grep -A5 transport_attrs

# 查看 URMA 设备状态
urma_cmd -q all -f json

# 抓取 TENT RPC 通信（BootstrapUb）
tcpdump -i any port <TENT_RPC_PORT> -A -s0 | grep BootstrapUb
```

---

## 八、已知限制

1. **`addLocalMemoryBuffer` 和 `updateLocalSegmentDesc` 未 virtual**：这两个方法在 bridge 中走基类的 P2P 本地缓存实现，P2P 模式不向外发布，这是有意设计（TENT 的 `synchronizeLocal` 承担发布职责）。

2. **`#ifdef USE_UB` 宏依赖**：`HandShakeDesc.jetty_num` 字段只在 `USE_UB=ON` 时编译，`UbBootstrapDesc` ↔ `HandShakeDesc` 的转换代码在 bridge 和 transport 中均有对应 `#ifdef` 保护，真机测试必须以 `USE_UB=ON` 编译。

3. **Fallback 边界**：`submit-fallback` 改动移除了 `updateTaskStatusAfterPoll` 中对 UNSPEC 任务的豁免，现在所有 UNSPEC 任务在 poll 时都会触发 `resubmitTransferTask`。如果没有可用的 fallback transport，`resubmitTransferTask` 会返回错误，任务最终标为 FAILED，行为与之前一致。
