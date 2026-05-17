# GDS 重构项目 - 产品需求文档

## 概述
- **摘要**：本项目旨在重构 GDS (现在已更名为 NDS) 存储接口的实现，将 Mock 实现从 NDSMock 类中移除，直接实现 gds_interface.h 暴露的接口。
- **目的**：简化代码结构，减少不必要的封装，直接使用接口进行操作，提高代码的可维护性和清晰度。
- **目标用户**：Mooncake 项目的开发人员和维护人员。

## 目标
- 重构 gds_mock.cpp，移除 NDSMock 类，直接实现 gds_interface.h 暴露的接口。
- 修改所有使用 NDSMock::instance() 的代码，改为使用 gds_interface.h 暴露的接口。
- 确保重构后的代码能够正常编译和运行。
- 确保所有测试用例通过。

## 非目标（超出范围）
- 不修改 gds_interface.h 的接口定义。
- 不修改 NDS 命名空间的名称。
- 不改变存储的实现逻辑，只改变代码结构。

## 背景和上下文
- 当前的实现中，gds_mock.cpp 中定义了 NDSMock 类，并且通过 C 接口函数调用 NDSMock::instance() 来实现 gds_interface.h 暴露的接口。
- 同时，client.cpp 中直接使用 NDSMock::instance() 来调用相关方法，而不是使用 gds_interface.h 暴露的接口。
- 这种实现方式导致代码结构复杂，维护困难，需要进行重构。

## 功能需求
- **FR-1**：重构 gds_mock.cpp，移除 NDSMock 类，直接实现 gds_interface.h 暴露的接口。
- **FR-2**：修改 client.cpp 中所有使用 NDSMock::instance() 的代码，改为使用 gds_interface.h 暴露的接口。
- **FR-3**：修改测试代码中使用 NDSMock::instance() 的代码，改为使用 gds_interface.h 暴露的接口。

## 非功能需求
- **NFR-1**：重构后的代码应该保持与原来相同的功能和行为。
- **NFR-2**：重构后的代码应该具有良好的可读性和可维护性。
- **NFR-3**：重构后的代码应该能够正常编译和运行。
- **NFR-4**：所有测试用例应该通过。

## 约束
- **技术**：使用 C++17 标准，不引入新的依赖。
- **依赖**：依赖于 gds_interface.h 中定义的接口。

## 假设
- 重构不会改变存储的实现逻辑，只是改变代码结构。
- 所有使用 NDSMock::instance() 的代码都可以直接替换为使用 gds_interface.h 暴露的接口。

## 验收标准

### AC-1：gds_mock.cpp 重构完成
- **Given**：gds_mock.cpp 文件存在。
- **When**：重构 gds_mock.cpp，移除 NDSMock 类，直接实现 gds_interface.h 暴露的接口。
- **Then**：gds_mock.cpp 中不再包含 NDSMock 类的定义，直接实现了 gds_interface.h 暴露的接口。
- **验证**：`human-judgment`

### AC-2：client.cpp 修改完成
- **Given**：client.cpp 文件存在，其中使用了 NDSMock::instance()。
- **When**：修改 client.cpp，将所有使用 NDSMock::instance() 的代码改为使用 gds_interface.h 暴露的接口。
- **Then**：client.cpp 中不再包含 NDSMock::instance() 的调用，改为使用 gds_interface.h 暴露的接口。
- **验证**：`human-judgment`

### AC-3：测试代码修改完成
- **Given**：测试代码文件存在，其中使用了 NDSMock::instance()。
- **When**：修改测试代码，将所有使用 NDSMock::instance() 的代码改为使用 gds_interface.h 暴露的接口。
- **Then**：测试代码中不再包含 NDSMock::instance() 的调用，改为使用 gds_interface.h 暴露的接口。
- **验证**：`human-judgment`

### AC-4：代码编译成功
- **Given**：重构后的代码。
- **When**：编译项目。
- **Then**：编译成功，没有错误。
- **验证**：`programmatic`

### AC-5：测试用例通过
- **Given**：编译成功的项目。
- **When**：运行测试用例。
- **Then**：所有测试用例通过。
- **验证**：`programmatic`

## 未解决的问题
- [ ] 是否需要保留 NDSMock 类的辅助方法（如 clear()、size()、hasBlock()）？如果需要，如何处理这些方法？