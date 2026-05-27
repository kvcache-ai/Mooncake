# Transfer Engine Python API 文档

## 概述

Transfer Engine Python API 提供了一个高级接口，用于在分布式系统之间使用RDMA（远程直接内存访问）和其他传输协议进行高效的数据传输。它能够在集群节点之间实现快速、低延迟的数据移动。

## 安装

```python
from mooncake.engine import TransferEngine
```

## 类：TransferEngine

提供所有传输引擎功能的主类。

### 构造函数

```python
TransferEngine()
```

使用默认设置创建新的TransferEngine实例。

### 初始化方法

#### initialize()

```python
initialize(local_hostname, metadata_server, protocol, device_name)
```

使用基本配置初始化传输引擎。

**参数：**
- `local_hostname` (str): 本地服务器的主机名和端口（例如："127.0.0.1:12345"）
- `metadata_server` (str): 元数据服务器连接字符串（例如："127.0.0.1:2379" 或 "etcd://127.0.0.1:2379"）
- `protocol` (str): 要使用的传输协议（"rdma"、"tcp" 等）
- `device_name` (str): 要过滤的设备名称的逗号分隔列表，或空字符串表示所有设备

**返回值：**
- `int`: 成功时返回0，失败时返回负值

#### initialize_ext()

```python
initialize_ext(local_hostname, metadata_server, protocol, device_name, metadata_type)
```

使用扩展配置初始化传输引擎，包括元数据类型规范。

**参数：**
- `local_hostname` (str): 本地服务器的主机名和端口
- `metadata_server` (str): 元数据服务器连接字符串
- `protocol` (str): 要使用的传输协议
- `device_name` (str): 要过滤的设备名称的逗号分隔列表
- `metadata_type` (str): 元数据服务器的类型（"etcd"、"p2p" 等）

**返回值：**
- `int`: 成功时返回0，失败时返回负值

### 网络信息

#### get_rpc_port()

```python
get_rpc_port()
```

获取传输引擎正在监听的RPC端口。

**返回值：**
- `int`: RPC端口号

### 缓冲区管理

#### allocate_managed_buffer()

```python
allocate_managed_buffer(length)
```

使用伙伴分配系统分配指定大小的托管缓冲区，以实现高效的内存管理。

**参数：**
- `length` (int): 要分配的缓冲区大小（字节）

**返回值：**
- `int`: 分配的缓冲区的内存地址（整数），失败时返回0

#### free_managed_buffer()

```python
free_managed_buffer(buffer_addr, length)
```

释放之前分配的托管缓冲区。

**参数：**
- `buffer_addr` (int): 要释放的缓冲区的内存地址
- `length` (int): 缓冲区的大小（字节）

**返回值：**
- `int`: 成功时返回0，失败时返回负值

#### get_first_buffer_address()

```python
get_first_buffer_address(segment_name)
```

获取指定段中第一个缓冲区的地址。

**参数：**
- `segment_name` (str): 段的名称

**返回值：**
- `int`: 段中第一个缓冲区的内存地址，如果段不存在或没有已注册的缓冲区则返回 0

### 数据传输操作

#### transfer_sync_write()

```python
transfer_sync_write(target_hostname, buffer, peer_buffer_address, length)
```

执行同步写操作，将数据从本地缓冲区传输到远程缓冲区。

**参数：**
- `target_hostname` (str): 目标服务器的主机名
- `buffer` (int): 本地缓冲区地址
- `peer_buffer_address` (int): 远程缓冲区地址
- `length` (int): 要传输的字节数

**返回值：**
- `int`: 成功时返回0，失败时返回负值

#### transfer_sync_read()

```python
transfer_sync_read(target_hostname, buffer, peer_buffer_address, length)
```

执行同步读操作，将数据从远程缓冲区传输到本地缓冲区。

**参数：**
- `target_hostname` (str): 目标服务器的主机名
- `buffer` (int): 本地缓冲区地址
- `peer_buffer_address` (int): 远程缓冲区地址
- `length` (int): 要传输的字节数

**返回值：**
- `int`: 成功时返回0，失败时返回负值

#### transfer_sync()

```python
transfer_sync(target_hostname, buffer, peer_buffer_address, length, opcode)
```

使用指定的操作码执行同步传输操作。

**参数：**
- `target_hostname` (str): 目标服务器的主机名
- `buffer` (int): 本地缓冲区地址
- `peer_buffer_address` (int): 远程缓冲区地址
- `length` (int): 要传输的字节数
- `opcode` (TransferOpcode): 传输操作类型（READ 或 WRITE）

**返回值：**
- `int`: 成功时返回0，失败时返回负值

#### transfer_submit_write()

```python
transfer_submit_write(target_hostname, buffer, peer_buffer_address, length)
```

提交异步写操作并立即返回。

**参数：**
- `target_hostname` (str): 目标服务器的主机名
- `buffer` (int): 本地缓冲区地址
- `peer_buffer_address` (int): 远程缓冲区地址
- `length` (int): 要传输的字节数

**返回值：**
- `int`: 用于跟踪操作的批次ID，失败时返回负值

#### transfer_check_status()

```python
transfer_check_status(batch_id)
```

检查异步传输操作的状态。

**参数：**
- `batch_id` (int): 从transfer_submit_write()返回的批次ID

**返回值：**
- `int`:
  - 1: 传输成功完成
  - 0: 传输仍在进行中
  - -1: 传输失败
  - -2: 传输超时

### 缓冲区I/O操作

#### write_bytes_to_buffer()

```python
write_bytes_to_buffer(dest_address, src_ptr, length)
```

将Python字节对象中的字节写入指定地址的缓冲区。

**参数：**
- `dest_address` (int): 目标缓冲区地址
- `src_ptr` (bytes): 要写入的源字节
- `length` (int): 要写入的字节数

**返回值：**
- `int`: 成功时返回0，失败时返回负值

#### read_bytes_from_buffer()

```python
read_bytes_from_buffer(source_address, length)
```

从指定地址的缓冲区读取字节，并将其作为Python字节对象返回。

**参数：**
- `source_address` (int): 源缓冲区地址
- `length` (int): 要读取的字节数

**返回值：**
- `bytes`: 从缓冲区读取的字节

### 内存注册（实验性）

#### register_memory()

```python
register_memory(buffer_addr, capacity)
```

注册内存区域以供RDMA访问（实验性功能）。

**参数：**
- `buffer_addr` (int): 要注册的内存地址
- `capacity` (int): 内存区域的大小（字节）

**返回值：**
- `int`: 成功时返回0，失败时返回负值

#### unregister_memory()

```python
unregister_memory(buffer_addr)
```

注销之前注册的内存区域。

**参数：**
- `buffer_addr` (int): 要注销的内存地址

**返回值：**
- `int`: 成功时返回0，失败时返回负值

### 枚举

#### TransferOpcode

```python
TransferOpcode.READ   # 读操作
TransferOpcode.WRITE  # 写操作
```

## 环境变量

传输引擎支持以下环境变量：

- `MC_TRANSFER_TIMEOUT`: 设置传输超时时间（秒）（默认：30）
- `MC_METADATA_SERVER`: 默认元数据服务器地址
- `MC_LEGACY_RPC_PORT_BINDING`: 启用传统RPC端口绑定行为
- `MC_TCP_BIND_ADDRESS`: 指定TCP绑定地址
- `MC_CUSTOM_TOPO_JSON`: 自定义拓扑JSON文件路径
- `MC_TE_METRIC`: 启用指标报告（设置为"1"、"true"、"yes"或"on"）
- `MC_TE_METRIC_INTERVAL_SECONDS`: 设置指标报告间隔（秒）

## 使用示例

### 基本设置和数据传输

```python
from mooncake.engine import TransferEngine
import os

# Create transfer engine instance
engine = TransferEngine()

# Initialize with basic configuration
ret = engine.initialize(
    local_hostname="127.0.0.1:12345",
    metadata_server="127.0.0.1:2379",
    protocol="rdma",
    device_name=""
)

if ret != 0:
    raise RuntimeError(f"Initialization failed with code {ret}")

# Allocate and initialize client buffer (1MB)
client_buffer = np.ones(1024 * 1024, dtype=np.uint8)  # Fill with ones
buffer_data = client_buffer.ctypes.data
buffer_data_len = client_buffer.nbytes

# Prepare data
data = b"Hello, Transfer Engine!"
data_len = len(data)

engine.register(buffer_data, buffer_data_len)

# Get Remote Addr from ZMQ or upper-layer inference framework
remote_addr = ??

# Transfer data to remote node
ret = engine.transfer_sync_write(
    target_hostname="127.0.0.1:12346",
    buffer=data,
    peer_buffer_address=remote_addr,
    length=data_len
)

if ret == 0:
    print("Data transfer completed successfully")
else:
    print(f"Data transfer failed with code {ret}")

engine.deregister(data)
```

### 异步传输

```python
# Submit asynchronous write
batch_id = engine.transfer_submit_write(
    target_hostname="127.0.0.1:12346",
    buffer=local_addr,
    peer_buffer_address=remote_addr,
    length=data_len
)

if batch_id < 0:
    print(f"Failed to submit transfer with code {batch_id}")
else:
    # Poll for completion
    while True:
        status = engine.transfer_check_status(batch_id)
        if status == 1:
            print("Transfer completed successfully")
            break
        elif status == -1:
            print("Transfer failed")
            break
        elif status == -2:
            print("Transfer timed out")
            break
        # Transfer still in progress, continue polling
        import time
        time.sleep(0.001)  # Small delay to avoid busy waiting
```


### 托管缓冲区分配

```python
# Allocate managed buffer
buffer_size = 1024 * 1024  # 1MB
buffer_addr = engine.allocate_managed_buffer(buffer_size)

if buffer_addr == 0:
    print("Failed to allocate buffer")
else:
    # Use the buffer
    test_data = b"Test data for managed buffer"
    engine.write_bytes_to_buffer(buffer_addr, test_data, len(test_data))

    # Read back
    read_data = engine.read_bytes_from_buffer(buffer_addr, len(test_data))
    print(f"Read data: {read_data}")

    # Free the buffer when done
    engine.free_managed_buffer(buffer_addr, buffer_size)
```

### 使用环境变量的配置示例

```python
import os

# 设置环境变量
os.environ["MC_TRANSFER_TIMEOUT"] = "60"  # 60秒超时
os.environ["MC_METADATA_SERVER"] = "etcd://192.168.1.100:2379"
os.environ["MC_TE_METRIC"] = "1"  # 启用指标报告

# 创建并初始化传输引擎
engine = TransferEngine()
ret = engine.initialize(
    local_hostname="192.168.1.101:12345",
    metadata_server="192.168.1.100:2379",
    protocol="rdma",
    device_name="mlx5_0,mlx5_1"  # 指定RDMA设备
)
```

### 错误处理和重试机制

## 错误处理

所有方法都返回整数状态码：
- `0`: 成功
- 负值: 表示各种失败条件的错误代码

常见错误场景：
- 网络连接问题
- 无效的缓冲区地址
- 内存分配失败
- 传输超时
- 元数据服务器连接问题

## 性能考虑

1. **缓冲区重用**: 尽可能重用分配的缓冲区，避免频繁的分配/释放开销
2. **批处理操作**: 当需要多个传输时，使用`transfer_submit_write()`和`transfer_check_status()`以获得更好的吞吐量
3. **内存对齐**: 确保缓冲区正确对齐以获得最佳RDMA性能
4. **超时配置**: 根据网络特性和数据大小调整`MC_TRANSFER_TIMEOUT`

## 线程安全

Transfer Engine Python API在大多数操作中都是线程安全的。但是，建议：
- 尽可能为不同线程使用单独的TransferEngine实例
- 避免对同一缓冲区地址进行并发修改
- 在线程间共享缓冲区地址时使用适当的同步机制

## 故障排除

1. **初始化失败**: 检查元数据服务器连接性和网络配置
2. **传输失败**: 验证目标主机名是否正确以及网络连接是否建立
3. **内存问题**: 确保系统内存充足且缓冲区对齐正确
4. **性能问题**: 检查RDMA设备配置和网络拓扑

## 最佳实践

1. **资源管理**: 始终在完成后释放分配的缓冲区
2. **错误检查**: 检查所有API调用的返回值
3. **配置优化**: 根据硬件环境调整环境变量
4. **监控**: 启用指标报告以监控传输性能
5. **测试**: 在生产环境中使用前进行充分的测试
