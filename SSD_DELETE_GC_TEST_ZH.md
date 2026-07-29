# SSD 对象删除与 Bucket GC 测试说明

## 1. 测试范围

本分支只验证以下链路：

1. `Remove` 和 `BatchRemove` 为已完成的 `LOCAL_DISK` 副本生成删除任务；
2. HA 模式下，删除任务和 ACK 通过 OpLog 持久化并复制到 standby；
3. holder 按对象 incarnation 校验后，将 bucket 中的对象持久化为 tombstone；
4. tombstone 对象立即不可读，重启后也不会重新注册；
5. 后台单 worker GC：
   - 直接删除全死 bucket；
   - 普通情况下按 tombstone 比例触发；
   - SSD 使用量达到高水位后忽略比例门槛，持续回收到低水位或无可回收对象；
   - 单次最多合并 8 个部分失效 bucket，且目标 bucket 不超过现有的大小和 key 数限制；
6. GC 使用两阶段本地 intent 恢复 holder 进程崩溃，不依赖 Master OpLog 猜测本地文件状态；
7. 延迟到达的旧删除任务不会删除同名的新 incarnation。

不在本次范围内：`RemoveAll`、`RemoveByRegex`、其他存储后端、通用任务调度器和多 worker GC。

## 2. 建议测试环境

- Linux x86_64 或 ARM64；
- 一块独立测试目录或测试盘，不要指向生产数据；
- Ninja、CMake 和仓库 `dependencies.sh` 所需依赖；
- 如需运行 HA 进程终止测试，配置时打开
  `MOONCAKE_ENABLE_TEST_FAILPOINTS`。

记录待测版本：

```bash
git checkout ssd_delete_new
git rev-parse HEAD
git status --short
```

## 3. 构建

以下配置不要求 CUDA、RDMA 或 UB 硬件：

```bash
cmake -S . -B build-ssd-delete -G Ninja \
  -DWITH_STORE=ON \
  -DWITH_EP=OFF \
  -DUSE_CUDA=OFF \
  -DBUILD_UNIT_TESTS=ON \
  -DBUILD_EXAMPLES=OFF \
  -DMOONCAKE_ENABLE_TEST_FAILPOINTS=ON

cmake --build build-ssd-delete --target \
  storage_backend_bucket_delete_test \
  local_delete_test \
  oplog_applier_test \
  local_delete_process_kill_test
```

如果测试机的标准构建参数与上面不同，以机器上已经验证过的 Mooncake
构建参数为准，但必须保留 `BUILD_UNIT_TESTS=ON`。

上述四个定向测试不要求真实 etcd。若还要运行
`hot_standby_service_test` 等完整 HA/standby 回归，请使用独立构建目录并开启
Mooncake Store 的 HA etcd 支持：

```bash
cmake -S . -B build-ssd-delete-ha -G Ninja \
  -DWITH_STORE=ON \
  -DWITH_EP=OFF \
  -DUSE_CUDA=OFF \
  -DBUILD_UNIT_TESTS=ON \
  -DBUILD_EXAMPLES=OFF \
  -DSTORE_USE_ETCD=ON \
  -DMOONCAKE_ENABLE_TEST_FAILPOINTS=ON
```

这里必须是 `STORE_USE_ETCD`；`USE_ETCD` 是 Transfer Engine 的独立选项，
不能替代前者。未开启 `STORE_USE_ETCD` 时，HotStandby 的 OpLog following
按上游设计返回 `INTERNAL_ERROR`，因此不能据此判断本特性产生了 HA 回归。

## 4. 必跑自动化测试

```bash
ctest --test-dir build-ssd-delete --output-on-failure -R \
  '^(storage_backend_bucket_delete_test|local_delete_test|oplog_applier_test|local_delete_process_kill_test)$'
```

也可以单独运行 GC 用例：

```bash
build-ssd-delete/mooncake-store/tests/storage_backend_bucket_delete_test \
  --gtest_filter='*GarbageCollection*:*DiskHighWatermark*'
```

重点用例与预期结果：

| 用例 | 预期结果 |
| --- | --- |
| `DeleteIsDurableAndCannotDeleteRecreatedKey` | tombstone 重启后仍有效；旧任务不能删除同名新对象 |
| `GarbageCollectionMergesPartiallyDeadBuckets` | 3 个部分失效 bucket 合并成 1 个；live value 字节完全一致；重启后仍可读 |
| `DiskHighWatermarkOverridesDeletedRatioThreshold` | 删除比例低于普通门槛时，只要超过高水位仍触发 GC，并回落到低水位 |
| `GarbageCollectionUnlinksFullyDeadBucketWithoutReplacement` | `.meta` 和 `.bucket` 都被删除，不创建替代 bucket |
| `GarbageCollectionIntentRecoversPreparedAndCommittedStates` | PREPARED 保留 source；COMMITTED 保留 target；intent 被清理 |
| `RemoveAndAckReplicateLocalDeleteIntent` | standby 同步应用 REMOVE 和 ACK |
| `DurableRemoveSurvivesPrimaryKillBeforeCallback` | primary 在 durable callback 前退出，standby 仍恢复删除任务 |
| `AckKilledBeforeDurabilityLeavesTaskPending` | ACK 未持久化时任务不会丢失，可再次下发 |

## 5. 运行参数

新增 GC 参数只有三个：

```bash
export MOONCAKE_OFFLOAD_BUCKET_GC_ENABLE=1
export MOONCAKE_OFFLOAD_BUCKET_GC_INTERVAL_SECONDS=10
export MOONCAKE_OFFLOAD_BUCKET_GC_DELETED_RATIO=0.25
```

GC 复用现有容量和磁盘水位参数：

```bash
export MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE=$((20 * 1024 * 1024 * 1024))
export MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION=1
export MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO=0.90
export MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO=0.80
```

建议先把 bucket 大小、每 bucket key 数和总容量调小，在测试盘上快速制造多个
bucket 和水位压力。

## 6. 端到端功能测试

### 6.1 Remove

1. 写入至少两个对象，使它们进入同一个 bucket；
2. 等待 offload 完成；
3. 调用 `Remove` 删除其中一个对象；
4. 等待一个 holder heartbeat 周期；
5. 验证：
   - Master 查询不到已删除对象；
   - holder 本地读取不到已删除对象；
   - 同 bucket 的 live 对象仍能字节精确读取；
   - GC 前 bucket 数据文件仍存在；
   - GC 后旧 bucket 被删除或替换，物理占用下降。

### 6.2 BatchRemove 和多 bucket 合并

1. 创建至少 3 个 bucket，每个 bucket 放 2 个对象；
2. 用一次 `BatchRemove` 从每个 bucket 删除 1 个对象；
3. 等待 tombstone ACK；
4. 验证所有已删除对象不可读，所有保留对象可读；
5. 等待 GC；
6. 验证多个 source bucket 被合并，且合并后的 bucket 数减少；
7. 重启 holder，再次验证 deleted 不可读、live 可读。

可用下面的方式观察测试目录，文件名按实际存储路径替换：

```bash
find "$SSD_TEST_PATH" -maxdepth 1 -type f \
  \( -name '*.meta' -o -name '*.bucket' -o -name '.bucket_gc_intent' \) \
  -printf '%f %s\n' | sort
du -sb "$SSD_TEST_PATH"
```

### 6.3 全死 bucket

1. 删除某个 bucket 中的全部对象；
2. 等待 GC；
3. 验证对应 `.meta` 和 `.bucket` 均消失；
4. 验证没有为这个 bucket 创建空 replacement；
5. 重启 holder，确认对象不会重新出现。

### 6.4 高低水位

1. 把总容量设小，高/低水位分别设为 `0.90/0.80`；
2. 把普通 GC 比例设为 `0.90`；
3. 写入数据使物理占用超过高水位；
4. 只删除约 30% 数据；
5. 验证虽然 30% 低于普通 90% 门槛，GC 仍被水位触发；
6. 验证有足够 dead bytes 时占用回落到低水位；
7. 验证 GC I/O 不在 heartbeat 线程内同步执行。

## 7. 崩溃恢复测试

### 7.1 Master/standby

必须运行 `local_delete_process_kill_test`。另外建议在真实 HA 部署中验证：

1. REMOVE OpLog 已持久化、primary callback 尚未执行时终止 primary；
2. 提升 standby；
3. 验证 holder 仍能获取同一个删除任务；
4. 在 tombstone 已落盘、ACK 尚未持久化时再次终止 primary；
5. 验证任务会重复下发，但重复 tombstone 是幂等的。

### 7.2 Holder tombstone

1. holder 写完 tombstone 后、发送 ACK 前终止进程；
2. 重启 holder；
3. 验证对象不会由 `.meta` 恢复为 live；
4. 验证重复任务返回 already-removed，并最终 ACK。

### 7.3 Holder GC

在出现 `.bucket_gc_intent` 时终止 holder，分别覆盖：

- PREPARED：重启后保留所有 source，删除未提交 target；
- COMMITTED：重启后保留 target，删除所有 source；
- 全死 bucket：重启后 source 文件最终被清理。

每次恢复后都必须满足：

- 每个 live key 只有一个权威副本；
- deleted key 不可见；
- live value 字节不变；
- `.bucket_gc_intent` 最终消失；
- 不存在同时被索引的 source 和 target。

## 8. 性能与资源检查

建议用同一工作负载分别关闭和打开 GC，记录：

- holder heartbeat p50/p99；
- GC 期间 Get p50/p99；
- GC 前后物理字节数；
- GC 读取、写入和回收字节；
- holder RSS 峰值；
- GC 期间 CPU 和磁盘带宽。

实现使用固定 1 MiB copy buffer；RSS 不应随单个 bucket 的 live data
体积线性增长。GC 只有一个 worker，不应出现多个 compaction 并发占满磁盘。

## 9. 通过标准

- 上述四个测试目标全部构建成功；
- 定向 CTest 全部通过；
- Remove、BatchRemove、全死 bucket、多 bucket 合并和水位回收均通过；
- primary、standby、holder 三类重启后都不丢删除意图；
- 同名对象重建不会被旧任务误删；
- GC 前后所有 live value 字节一致；
- 没有遗留 intent、空 replacement 或被重复索引的 bucket；
- 测试结果附带 commit SHA、构建参数、测试命令和完整失败日志。

> 当前文档由实现方提供测试步骤；按约定，本分支未在本地执行编译或测试，
> 最终可提交结论应以 Linux 测试机结果为准。
