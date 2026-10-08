# GetSegmentDesc 重启恢复测试

## 契约

`ControlClient::getSegmentDesc` 首次遇到 `RpcServiceError` 时额外尝试一次，
成功及其他错误直接返回。重试只限于读取本端描述的只读 RPC，不改变通用
`CoroRpcAgent` 和其他 ControlClient 操作的重放语义。

RPC 连接池按线程隔离。同地址服务重启后，某线程成功刷新不代表其他线程的
旧连接已更新。底层 RPC 在旧连接失败后丢弃该连接；只读调用的第二次尝试可
建立新连接。若对端仍不可用，最终返回第二次错误，不伪造成功。

“有界”指最多两次 RPC 尝试，仍沿用每次 RPC 的既有超时，不新增总 deadline、
循环退避或后台重试。健康路径不增加 RPC、锁或 sleep；这不等于承诺零 CPU
开销。WR/CQ、bootstrap、notify 等路径不受此重试逻辑影响。

## 运行

在 Linux 上按仓库方式启用 `USE_TENT=ON`、`BUILD_UNIT_TESTS=ON` 后：

```bash
cmake --build build --target tent_segment_desc_retry_test tent_rpc_reconnect_test
ctest --test-dir build -R 'tent_(segment_desc_retry|rpc_reconnect)_test' --output-on-failure
build/mooncake-transfer-engine/tent/tests/tent_segment_desc_retry_test --gtest_repeat=20 --gtest_shuffle
```

新增测试通过真实 ControlClient、CoroRpcAgent 和 loopback socket 覆盖：

- 同端口重启后第一次读取获得新代次描述。
- 前台已恢复后，长驻 worker 的独立旧池也在第一次读取内恢复。
- 健康 handler 仅执行一次；持续报错 handler 恰好执行两次。
- 对端持续失联返回失败；通知副作用仍只执行一次。

每项在独立调用线程运行以隔离 thread-local 池；重启必须保持原端口。
100ms FIN 前置等待沿用 `rpc_reconnect_test`，不是生产重试策略。
这些测试不依赖 RDMA 硬件，也不单独证明 RDMA 数据面重启恢复或性能无回退；
后两项需使用长驻 engine 的双机恢复和独立性能测试验证。

## 兼容与回滚

无需 API、wire format 或配置迁移。调用方可能少看到一次可恢复错误，持续失效
路径最多多耗费一次 RPC 尝试。回退此独立修复即可恢复原单次调用行为；不要通过
给通用 RPC 加自动重试来替代它，有副作用的操作可能被执行两次。
