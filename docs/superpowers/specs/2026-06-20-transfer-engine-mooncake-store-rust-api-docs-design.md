---
title: Transfer Engine & Mooncake Store Rust API docs completion
date: 2026-06-20
status: accepted
---

## Goal

补充并完善 Mooncake 的 **Transfer Engine** 与 **Mooncake Store** 的 **Rust 接口文档**，使读者无需阅读源码即可：

- 知道每个 Rust crate 的定位、依赖（CMake/动态链接）与运行时前置条件；
- 了解对外暴露的 Rust API（公开类型、方法、参数语义、返回值/错误语义、安全约束）；
- 复制示例即可完成最小可用的初始化与读写/传输。

## Non-goals

- 不改动 Rust/C++ 实现与 ABI。
- 不为 Rust API 追加新的接口；仅补文档与必要的使用示例（以现有接口为准）。
- 不把 Rust API 变成自动生成的 rustdoc（本次以 Sphinx markdown 页面为主，保持与现有 Python/C++ 文档风格一致）。

## Current state (found in repo)

- Docs 目前包含 `api-reference/python|cpp|http`，缺少 `api-reference/rust`。
- Mooncake Store Rust crate: `mooncake-store/rust`，库名 `mooncake_store`，提供安全封装：
  - `MooncakeStore`
  - `ReplicateConfig`
  - `StoreError`
- Transfer Engine Rust bindings: `mooncake-transfer-engine/rust`，crate 名 `transfer_engine_rust`（当前以二进制示例为主），其 `src/transfer_engine.rs` 提供绑定封装类型：
  - `TransferEngine`
  - `TransferRequest`
  - `OpcodeEnum`
  - `TransferStatusEnum`
  - `BufferEntry`

## Approach

新增 Rust API Reference 目录，并在顶层 `docs/source/index.md` 的 toctree 中挂载：

- `docs/source/api-reference/rust/index.md`
- `docs/source/api-reference/rust/mooncake-store.md`
- `docs/source/api-reference/rust/transfer-engine.md`

文档内容采用“与 Python API reference 类似的结构”：

- Overview / Installation (build & runtime prerequisites)
- Quick start（最小可运行示例）
- API Reference（按类型分节）
- Safety / Thread-safety（对 `unsafe`、FFI 指针、注册内存等关键点做显式说明）
- Error handling（Rust error types + C 层错误码映射方式）
- Environment variables / runtime dependencies（与现有部署文档对齐，强调 metadata server / mooncake_master 依赖）

## Success criteria

- `docs/source/index.md` 中出现 Rust API Reference 入口。
- Rust API Reference 页面能完整覆盖上述公开类型/方法，并含可复制的示例。
- `docs/Makefile` 的 HTML 构建流程（如可在 CI/本地运行）不因新增页面失败。

