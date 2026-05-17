# GDS KV 存储集成分析与测试方案

## 1. 任务概述

本任务旨在将 Mooncake 项目中的文件存储机制替换为 GDS KV 存储，主要修改 Client::Put 和 Client::TransferRead 方法，使其使用 GDS KV 接口进行数据存储和读取。

## 2. 核心修改点

### 2.1 接口转换
- **ObjectKey 到 uint64_t 转换**：使用 `objectKeyToUint64` 函数将字符串类型的 ObjectKey 转换为 uint64_t 类型的 blockId
- **GDS 接口调用**：将文件操作替换为 GDS::put 和 GDS::get 调用

### 2.2 代码修改

#### Client::Put 方法
- **修改位置**：`src/client.cpp` 第 596-695 行
- **修改内容**：
  1. 当处理磁盘副本时，将数据写入 GDS KV 存储
  2. 计算总数据大小并合并切片数据
  3. 调用 GDSMock::instance().put 写入数据
  4. 处理错误情况并返回适当的错误码

#### Client::TransferRead 方法
- **修改位置**：`src/client.cpp` 第 1376-1406 行
- **修改内容**：
  1. 当处理磁盘副本时，从 GDS KV 存储读取数据
  2. 将 ObjectKey 转换为 blockId
  3. 调用 GDSMock::instance().get 读取数据
  4. 处理错误情况并返回适当的错误码

#### Client::BatchPut 方法
- **修改位置**：`src/client.cpp` 第 1103-1122 行
- **修改内容**：
  1. 在处理磁盘副本时，使用 GDS KV 存储
  2. 对每个键值对调用 GDSMock::instance().put

#### Client::BatchGet 方法
- **修改位置**：`src/client.cpp` 第 472-594 行
- **修改内容**：
  1. 在处理磁盘副本时，从 GDS KV 存储读取数据
  2. 对每个键值对调用 GDSMock::instance().get

### 2.3 GDS Mock 实现
- **修改位置**：`include/gds/gds_mock.cpp`
- **修改内容**：
  1. 将文件写入目录从当前目录修改为 `kv_data` 子目录
  2. 自动创建不存在的 `kv_data` 目录
  3. 输出详细的调试信息，包括文件路径

## 3. 测试方案

### 3.1 测试用例设计

#### 基本 Put/Get 测试
- **测试目标**：验证 Client::Put 能正确将数据写入 GDS KV 存储，Client::Get 能正确从 GDS KV 存储读取数据
- **测试步骤**：
  1. 创建 Client 实例
  2. 准备测试数据和缓冲区
  3. 调用 Client::Put 存储数据
  4. 调用 Client::Get 读取数据
  5. 验证读取的数据与写入的数据一致
  6. 清理测试数据

#### 批量操作测试
- **测试目标**：验证 Client::BatchPut 和 Client::BatchGet 能正确处理多个键值对
- **测试步骤**：
  1. 创建 Client 实例
  2. 准备多个测试键值对
  3. 调用 Client::BatchPut 批量存储数据
  4. 调用 Client::BatchGet 批量读取数据
  5. 验证所有键值对都能正确读取
  6. 清理测试数据

#### 异常情况测试
- **测试目标**：验证系统能正确处理异常情况，如读取不存在的数据
- **测试步骤**：
  1. 创建 Client 实例
  2. 尝试读取一个不存在的键
  3. 验证系统返回适当的错误码

#### 边界情况测试
- **测试目标**：验证系统能正确处理边界情况，如空数据、大数据等
- **测试步骤**：
  1. 创建 Client 实例
  2. 测试空数据的存储和读取
  3. 测试大数据的存储和读取
  4. 验证操作结果

### 3.2 测试代码

测试代码文件：`tests/client_gds_integration_test.cpp`

## 4. 编译改动

### 4.1 CMake 配置
- **修改文件**：`CMakeLists.txt`
- **修改内容**：
  1. 添加 GDS 接口头文件目录
  2. 链接 GDS Mock 库

### 4.2 编译命令

#### 编译 GDS Mock 库
```bash
cd include/gds
g++ -std=c++17 -Wall -Wextra -O2 -fPIC -I. -c gds_mock.cpp -o gds_mock.o
ar rcs libgdsmock.a gds_mock.o
```

#### 编译测试代码
```bash
g++ -std=c++17 -Wall -Wextra -O2 -Iinclude -c tests/client_gds_integration_test.cpp -o client_gds_integration_test.o
g++ -o client_gds_integration_test client_gds_integration_test.o -Linclude/gds -lgdsmock
```

## 5. 验证方法

1. **运行测试**：执行测试代码，确保所有测试用例通过
2. **检查文件**：验证 `kv_data` 目录是否创建，以及是否生成了相应的文件
3. **检查日志**：查看调试日志，确认数据是否正确写入和读取
4. **性能测试**：测试不同大小数据的存储和读取性能

## 6. 依赖项

- GDS KV 接口头文件：`gds_interface.h`
- GDS Mock 库：`libgdsmock.a`
- C++17 或更高版本

## 7. 注意事项

1. **错误处理**：确保正确处理 GDS 接口返回的错误码
2. **内存管理**：确保所有分配的内存都能正确释放
3. **数据完整性**：确保数据在存储和读取过程中保持完整
4. **性能优化**：考虑批量操作和数据压缩等优化策略

## 8. 结论

通过本次修改，Mooncake 项目成功集成了 GDS KV 存储，替换了原有的文件存储机制。这将提高数据存储和读取的效率，同时提供更可靠的数据持久化方案。
