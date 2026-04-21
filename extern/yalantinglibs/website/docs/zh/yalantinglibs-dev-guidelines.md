# 目标与原则
- 单一职责：每个模块/函数只做一件事。
- 向后兼容：主版本（major）外不破坏 API。
- 文档即契约：公开接口应有清晰文档。
- 零依赖或最小依赖：避免引入不必要的第三方库。
- 性能可预测：关键路径需有基准测试（benchmark）。
- 安全优先：防止内存泄漏、越界访问、注入等漏洞。
# 项目结构
```
yalantinglibs/ylt
├── include/ # C++ 库实现文件
├── thirdparty/ # 第三方依赖库代码
├── standalone/ # 在yalantinglibs中维护的第三方库代码
├── src/module/tests # 单元测试
├── src/module/benchmark/ # 性能测试
├── src/module/examples/ # 使用示例
├── websites/docs/ # 文档
├── cmake/ # 构建脚本
├── README.md
├── LICENSE
├── .gitignore
└── CMakeLists.txt # 构建脚本
```

# 编码规范
## 命名约定
| 类型       | 命名规则                                      |
|------------|---------------------------------------------|
| 函数/变量  | `snake_case`                                |
| 类型/类    | `snake_case`，如果类名较短可以加 `_t` 后缀    |
| 宏/常量    | `UPPER_SNAKE_CASE`                          |
| 私有成员   | 变量名加后缀 `_`（例如：`id_`）              |

## 注释与文档
- 所有 public 接口 必须有文档注释（Doxygen）。
- 示例： 

```cpp
/*!
* client发起rpc调用，返回rpc_result，内部调用了call_for接口，超时时间为5秒。
*
* @tparam func 服务端注册的rpc函数
* @tparam Args
* @param args 服务端rpc函数需要的参数
* @return rpc调用结果
*/
template <auto func, typename... Args>
async_simple::coro::Lazy<
    rpc_result<util::function_return_type_t<decltype(func)>>>
call(Args &&...args);
```

## 代码格式化
使用clang-format-13 格式化代码

## 错误处理
- 不要静默失败：要么抛异常，要么返回错误码 + 日志。
- 提供明确的错误类型（coro_rpc::errc）。
- 避免使用 assert 做输入校验（发布版可能被关闭）。
版本管理（SemVer）
- 严格遵循 Semantic Versioning 2.0.0
  ○ MAJOR.MINOR.PATCH
  ○ MAJOR：较大变更（特性、API 增加/删除/修改）
  ○ MINOR：新增功能
  ○ PATCH：bug 修复
- 每次发布必须更新 CHANGELOG.md，格式如下：
```
What's Changed
- feat: make appenders useful by @std-microblock in #988
- Revert "feat: make appenders useful" by @qicosmos in #1045
- [coro_rpc_client]fix for rpc const ref arg by @qicosmos in #1050
- [coro_http]Fix chat room by @qicosmos in #1051
- [easylog][feat]add appenders by @qicosmos in #1049
- Merge Miss Code from LTS 1.2.0 by @poor-circle in #1048
```
# 测试要求
- CI 必须包含：编译检查、单元测试、格式检查、内存检测（ASan）。
# 构建与分发
- 提供标准构建方式（CMake）
- 支持静态库 & 动态库（C++）
- 提供预编译二进制（如 GitHub Releases）
# 依赖管理
- 依赖必须锁定版本
- 评估依赖的安全性
# 安全与合规
- 所有输入必须校验（路径、大小、格式）
- 禁止硬编码密钥、路径
- 遵守许可证兼容性（MIT）
- 敏感操作需提供日志开关（默认关闭）
# 文档要求
- README.md 必须包含： 
  ○ 功能简介
  ○ 安装方法
  ○ 快速开始示例
  ○ API 文档链接
- 提供完整 API 参考文档（自动生成）
- 提供常见问题（FAQ）和故障排查指南
# 代码审查（Code Review）
- 所有 PR 必须经过至少 1 人 review
- 检查点： 
  ○ 是否破坏兼容性？
  ○ 是否有未覆盖的边界情况？
  ○ 文档是否同步更新？
  ○ 是否引入新依赖？