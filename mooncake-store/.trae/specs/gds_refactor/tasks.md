# GDS 重构项目 - 实现计划（分解和优先排序的任务列表）

## [x] 任务 1：重构 gds_mock.cpp，移除 NDSMock 类，直接实现 gds_interface.h 暴露的接口
- **优先级**：P0
- **依赖**：无
- **描述**：
  - 移除 gds_mock.cpp 中的 NDSMock 类定义
  - 将 NDSMock 类的方法实现直接作为 gds_interface.h 暴露的接口的实现
  - 保留必要的全局变量和辅助函数
- **验收标准**：AC-1
- **测试要求**：
  - `human-judgment` TR-1.1：gds_mock.cpp 中不再包含 NDSMock 类的定义
  - `human-judgment` TR-1.2：gds_mock.cpp 直接实现了 gds_interface.h 暴露的接口
  - `programmatic` TR-1.3：gds_mock.cpp 能够正常编译

## [x] 任务 2：修改 client.cpp 中使用 NDSMock::instance() 的代码
- **优先级**：P0
- **依赖**：任务 1
- **描述**：
  - 将 client.cpp 中所有使用 NDSMock::instance() 的代码改为使用 gds_interface.h 暴露的接口
  - 确保修改后的代码逻辑与原来相同
- **验收标准**：AC-2
- **测试要求**：
  - `human-judgment` TR-2.1：client.cpp 中不再包含 NDSMock::instance() 的调用
  - `programmatic` TR-2.2：client.cpp 能够正常编译

## [x] 任务 3：修改测试代码中使用 NDSMock::instance() 的代码
- **优先级**：P1
- **依赖**：任务 1
- **描述**：
  - 将测试代码中所有使用 NDSMock::instance() 的代码改为使用 gds_interface.h 暴露的接口
  - 确保修改后的测试代码逻辑与原来相同
- **验收标准**：AC-3
- **测试要求**：
  - `human-judgment` TR-3.1：测试代码中不再包含 NDSMock::instance() 的调用
  - `programmatic` TR-3.2：测试代码能够正常编译

## [x] 任务 4：编译项目，确保无错误
- **优先级**：P0
- **依赖**：任务 1、任务 2、任务 3
- **描述**：
  - 编译整个项目，确保没有编译错误
- **验收标准**：AC-4
- **测试要求**：
  - `programmatic` TR-4.1：项目编译成功，没有错误

## [x] 任务 5：运行测试用例，确保所有测试通过
- **优先级**：P0
- **依赖**：任务 4
- **描述**：
  - 运行所有测试用例，确保所有测试通过
- **验收标准**：AC-5
- **测试要求**：
  - `programmatic` TR-5.1：所有测试用例通过

## [x] 任务 6：处理 NDSMock 类的辅助方法
- **优先级**：P2
- **依赖**：任务 1
- **描述**：
  - 评估是否需要保留 NDSMock 类的辅助方法（如 clear()、size()、hasBlock()）
  - 如果需要，将这些方法改为全局函数或其他适当的形式
- **验收标准**：AC-1
- **测试要求**：
  - `human-judgment` TR-6.1：辅助方法的处理方式合理
  - `programmatic` TR-6.2：使用辅助方法的代码能够正常编译和运行