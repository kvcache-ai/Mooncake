# yaLanTingLibs 国密SSL（NTLS）支持

## 概述

yaLanTingLibs 现已全面支持国密SSL（NTLS，National TLS），这是中国国家密码管理局制定的密码算法标准。通过集成 Tongsuo（铜锁）密码库，yaLanTingLibs 的 `coro_rpc` 和 `coro_http` 组件现在可以支持两种国密通信协议：

- **GB/T 38636-2020 TLCP**：双证书国密通信协议（Transport Layer Cryptography Protocol）
- **RFC 8998**：TLS 1.3 + 国密单证书模式

这使得 yaLanTingLibs 成为首批支持国密SSL的现代C++20协程网络库之一，为需要符合国密标准的应用提供了高性能、易用的解决方案。

## 背景

### 什么是国密SSL？

国密SSL（NTLS）是基于中国国家密码算法标准（SM2/SM3/SM4）的传输层安全协议。与传统的TLS协议使用RSA、AES等国际算法不同，国密SSL使用：

- **SM2**：椭圆曲线公钥密码算法（用于数字签名和密钥交换）
- **SM3**：密码杂凑算法（用于消息摘要）
- **SM4**：分组密码算法（用于数据加密）

### 为什么需要国密SSL？

1. **合规要求**：在中国，涉及国家秘密、商业秘密和个人隐私的信息系统，需要采用国密算法
2. **自主可控**：国密算法由中国自主研发，符合国家信息安全战略
3. **标准化**：GB/T 38636-2020 和 RFC 8998 提供了标准化的国密通信协议规范

### 应用场景

- 政府信息系统
- 金融行业（银行、证券、保险）
- 关键基础设施
- 需要符合国密标准的商业应用

## 支持的协议模式

### 1. GB/T 38636-2020 TLCP（双证书模式）

TLCP（Transport Layer Cryptography Protocol）是中国的国家标准，采用双证书机制：

- **签名证书**：用于身份认证和数字签名
- **加密证书**：用于密钥交换和数据加密

**特点：**
- 符合GB/T 38636-2020标准
- 支持单向认证和双向认证
- 使用SM2/SM3/SM4国密算法套件
- 默认密码套件：`ECC-SM2-SM4-GCM-SM3` 和 `ECC-SM2-SM4-CBC-SM3`，支持设置ECDHE_SM4_CBC_SM3和ECDHE_SM4_GCM_SM3

### 2. RFC 8998 TLS 1.3 + 国密（单证书模式）

RFC 8998 是IETF标准，将国密算法集成到TLS 1.3协议中：

- 使用单一证书（简化证书管理）
- 基于TLS 1.3协议
- 支持国密算法套件：`TLS_SM4_GCM_SM3` 和 `TLS_SM4_CCM_SM3`

**特点：**
- 符合国际标准（RFC 8998）
- 证书管理更简单
- 性能优化（TLS 1.3的1-RTT握手）
- 向后兼容TLS 1.3生态

## 技术实现

### 依赖库

yaLanTingLibs 的NTLS功能基于 **Tongsuo（铜锁）** 密码库实现。Tongsuo（铜锁） 是由开放原子开源基金会（OpenAtom Foundation）孵化及运营的开源项目，基于OpenSSL 3.0.3分支，增加了对国密算法的完整支持。
铜锁获得了国家密码管理局商用密码检测中心颁发的商用密码产品认证证书，助力用户在国密改造、密评、等保等过程中，更加严谨地满足我国商用密码技术合规的要求。

**Tongsuo项目地址：** https://github.com/Tongsuo-Project/Tongsuo

### 架构设计

NTLS功能完全集成到 yaLanTingLibs 的现有架构中：

- **条件编译**：通过 `YLT_ENABLE_NTLS` 宏控制，默认关闭，不影响现有代码
- **统一API**：与标准SSL API保持一致的使用方式
- **协程友好**：完全支持C++20协程，异步非阻塞
- **零拷贝**：充分利用协程特性，实现高性能网络通信

## 编译配置

### 前置要求

1. **安装Tongsuo库**
   ```bash
   # 从源码编译安装Tongsuo
   git clone https://github.com/Tongsuo-Project/Tongsuo.git
   cd Tongsuo
   ./config --prefix=/usr/local/tongsuo \
            -Wl,-rpath,/usr/local/tongsuo/lib64 \
            enable-ntls
   make -j$(nproc)
   sudo make install
   sudo ldconfig
   ```

2. **配置环境变量**
   ```bash
   export PATH=/usr/local/tongsuo/bin:$PATH
   export LD_LIBRARY_PATH=/usr/local/tongsuo/lib64:$LD_LIBRARY_PATH
   export PKG_CONFIG_PATH=/usr/local/tongsuo/lib64/pkgconfig:$PKG_CONFIG_PATH
   ```

### CMake配置

在编译 yaLanTingLibs 时，需要启用SSL和NTLS支持：

```bash
cmake -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DYLT_ENABLE_SSL=ON \
    -DYLT_ENABLE_NTLS=ON \
    -DCMAKE_PREFIX_PATH=/usr/local/tongsuo \
    -DOPENSSL_ROOT_DIR=/usr/local/tongsuo \
    -DOPENSSL_INCLUDE_DIR=/usr/local/tongsuo/include

cmake --build build
```

**关键配置选项：**
- `YLT_ENABLE_SSL=ON`：启用SSL支持（NTLS的前置条件）
- `YLT_ENABLE_NTLS=ON`：启用NTLS支持
- `CMAKE_PREFIX_PATH`：指定Tongsuo的安装路径
- `OPENSSL_ROOT_DIR`：CMake查找OpenSSL/Tongsuo的根目录

## 使用方法

### coro_rpc 使用示例

#### TLCP双证书模式（GB/T 38636-2020）

**服务器端（单向认证）：**

```cpp
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <iostream>
#include <thread>

using namespace coro_rpc;

// 证书路径 - 使用Tongsuo安装的SM2证书
const std::string CERT_PATH = "/usr/local/tongsuo/ssl/certs/sm2/";
const std::string SERVER_SIGN_CERT = CERT_PATH + "server_sign.crt";
const std::string SERVER_SIGN_KEY = CERT_PATH + "server_sign.key";
const std::string SERVER_ENC_CERT = CERT_PATH + "server_enc.crt";
const std::string SERVER_ENC_KEY = CERT_PATH + "server_enc.key";
const std::string CA_CERT = CERT_PATH + "chain-ca.crt";

// RPC服务函数
std::string echo(std::string_view data) {
    std::cout << "Server received: " << data << std::endl;
    return "Hello World";
}

int main() {
    // 创建RPC服务器
    coro_rpc_server server(std::thread::hardware_concurrency(), 8801);
    
    // 配置NTLS（TLCP双证书模式，单向认证）
    ssl_ntls_configure ntls_conf;
    ntls_conf.base_path = "";                     // 使用完整路径
    ntls_conf.sign_cert_file = SERVER_SIGN_CERT;  // SM2签名证书
    ntls_conf.sign_key_file = SERVER_SIGN_KEY;    // SM2签名私钥
    ntls_conf.enc_cert_file = SERVER_ENC_CERT;    // SM2加密证书
    ntls_conf.enc_key_file = SERVER_ENC_KEY;      // SM2加密私钥
    ntls_conf.ca_cert_file = CA_CERT;             // CA证书
    ntls_conf.enable_client_verify = false;       // 禁用客户端证书验证
    ntls_conf.enable_ntls = true;                // 启用NTLS模式
    
    // 初始化NTLS
    server.init_ntls(ntls_conf);
    
    // 注册RPC服务
    server.register_handler<echo>();
    
    std::cout << "NTLS RPC Server (one-way auth) starting on port 8801..."
              << std::endl;
    
    // 启动服务器（阻塞）
    auto result = server.start();
    if (result) {
        std::cout << "Server start failed: " << result.message() << std::endl;
        return 1;
    }
    
    return 0;
}
```

**客户端（单向认证）：**

```cpp
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/SyncAwait.h"

using namespace coro_rpc;
using namespace async_simple::coro;

// 证书路径
const std::string CERT_PATH = "/usr/local/tongsuo/ssl/certs/sm2/";
const std::string CA_CERT = CERT_PATH + "chain-ca.crt";

// RPC函数声明
std::string echo(std::string_view data);

Lazy<void> test_one_way_auth() {
    coro_rpc_client client;
    
    // 初始化NTLS客户端（单向认证）
    // 单向认证模式下，只需要CA证书来验证服务器
    bool ok = client.init_ntls(
        CERT_PATH,      // 证书基础路径
        "",             // 客户端签名证书（单向认证不需要）
        "",             // 客户端签名私钥
        "",             // 客户端加密证书
        "",             // 客户端加密私钥
        CA_CERT,        // CA证书
        "localhost",    // 服务器域名
        false           // 不验证客户端证书
    );
    
    if (!ok) {
        std::cout << "Failed to initialize one-way NTLS client" << std::endl;
        co_return;
    }
    
    // 连接到服务器
    auto ec = co_await client.connect("127.0.0.1", "8801");
    if (ec) {
        std::cout << "Failed to connect: " << ec.message() << std::endl;
        co_return;
    }
    
    // 调用RPC服务
    auto result = co_await client.call<echo>("Test message to one-way server");
    if (result) {
        std::cout << "Echo result: " << result.value() << std::endl;
    }
}

int main() {
    syncAwait(test_one_way_auth());
    return 0;
}
```

**双向认证示例：**

```cpp
// 服务器端：设置 enable_client_verify = true
ntls_conf.enable_client_verify = true;  // 启用客户端证书验证

// 客户端：提供客户端证书
const std::string CLIENT_SIGN_CERT = CERT_PATH + "client_sign.crt";
const std::string CLIENT_SIGN_KEY = CERT_PATH + "client_sign.key";
const std::string CLIENT_ENC_CERT = CERT_PATH + "client_enc.crt";
const std::string CLIENT_ENC_KEY = CERT_PATH + "client_enc.key";

bool ok = client.init_ntls(
    CERT_PATH,
    CLIENT_SIGN_CERT,  // 客户端签名证书
    CLIENT_SIGN_KEY,   // 客户端签名私钥
    CLIENT_ENC_CERT,   // 客户端加密证书
    CLIENT_ENC_KEY,    // 客户端加密私钥
    CA_CERT,
    "localhost",
    true               // 验证客户端证书
);
```

#### RFC 8998 TLS 1.3 + 国密模式

**服务器端：**

```cpp
// 配置TLS 1.3 + 国密单证书模式（RFC 8998）
ssl_ntls_configure ntls_conf;
ntls_conf.base_path = "";
ntls_conf.mode = ntls_mode::tls13_single_cert;  // TLS 1.3 + 国密模式
ntls_conf.gm_cert_file = SERVER_GM_CERT;        // 国密单证书
ntls_conf.gm_key_file = SERVER_GM_KEY;          // 国密私钥
ntls_conf.ca_cert_file = CA_CERT;
ntls_conf.enable_client_verify = false;         // 单向认证
ntls_conf.cipher_suites = "TLS_SM4_GCM_SM3:TLS_SM4_CCM_SM3";  // TLS 1.3国密密码套件

server.init_ntls(ntls_conf);
```

**客户端：**

```cpp
// 初始化TLS 1.3 + 国密客户端（单向认证）
ssl_ntls_configure config;
config.base_path = CERT_PATH;
config.mode = ntls_mode::tls13_single_cert;  // TLS 1.3 + 国密模式
config.ca_cert_file = CA_CERT;
config.server_name = "localhost";
config.cipher_suites = "TLS_SM4_GCM_SM3:TLS_SM4_CCM_SM3";  // TLS 1.3国密密码套件
// 单向认证不需要客户端证书

bool ok = client.init_ntls(config);
```

### coro_http 使用示例

#### HTTP服务器（TLCP模式）

```cpp
#include <ylt/coro_http/coro_http_server.hpp>
#include <iostream>

using namespace cinatra;

// 证书路径
const std::string CERT_PATH = "/usr/local/tongsuo/ssl/certs/sm2/";
const std::string SERVER_SIGN_CERT = CERT_PATH + "server_sign.crt";
const std::string SERVER_SIGN_KEY = CERT_PATH + "server_sign.key";
const std::string SERVER_ENC_CERT = CERT_PATH + "server_enc.crt";
const std::string SERVER_ENC_KEY = CERT_PATH + "server_enc.key";
const std::string CA_CERT = CERT_PATH + "chain-ca.crt";

void start_one_way_server() {
    coro_http_server server(1, 8801);
    
    // 初始化NTLS（TLCP双证书模式，单向认证）
    server.init_ntls(
        SERVER_SIGN_CERT,    // 签名证书
        SERVER_SIGN_KEY,     // 签名私钥
        SERVER_ENC_CERT,     // 加密证书
        SERVER_ENC_KEY,      // 加密私钥
        CA_CERT,             // CA证书
        false                // 不验证客户端证书
    );
    
    // 设置NTLS密码套件
    server.set_ntls_cipher_suites("ECC-SM2-SM4-GCM-SM3:ECC-SM2-SM4-CBC-SM3");
    
    // 注册HTTP处理器
    server.set_http_handler<GET>(
        "/",
        [](coro_http_request& req, coro_http_response& resp) 
            -> async_simple::coro::Lazy<void> {
            resp.set_status_and_content(status_type::ok, "Hello World");
            co_return;
        });
    
    server.set_http_handler<POST>(
        "/echo",
        [](coro_http_request& req, coro_http_response& resp) 
            -> async_simple::coro::Lazy<void> {
            resp.set_status_and_content(status_type::ok, "Hello World");
            co_return;
        });
    
    std::cout << "One-way NTLS server ready on port 8801" << std::endl;
    server.sync_start();
}

int main() {
    start_one_way_server();
    return 0;
}
```

#### HTTP客户端

**TLCP模式（单向认证）：**

```cpp
#include <ylt/coro_http/coro_http_client.hpp>
#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/SyncAwait.h"

using namespace cinatra;

async_simple::coro::Lazy<void> test_ntls_http_client_one_way() {
    coro_http_client client{};
    
    // 启用NTLS协议（简单模式，仅服务器认证）
    client.set_ntls_schema(true);
    
    // 发送GET请求
    auto response = co_await client.async_get("https://localhost:8801/");
    if (response.net_err) {
        std::cout << "GET / request failed: " << response.net_err.message()
                  << std::endl;
    } else {
        std::cout << "Response status: " << response.status << std::endl;
        std::cout << "Response body: " << response.resp_body << std::endl;
    }
    
    // 发送POST请求
    auto post_response = co_await client.async_post(
        "https://localhost:8801/echo", "Test message", req_content_type::text);
    if (!post_response.net_err) {
        std::cout << "POST Response: " << post_response.resp_body << std::endl;
    }
}

int main() {
    async_simple::coro::syncAwait(test_ntls_http_client_one_way());
    return 0;
}
```

**TLCP模式（双向认证）：**

```cpp
const std::string CLIENT_SIGN_CERT = CERT_PATH + "client_sign.crt";
const std::string CLIENT_SIGN_KEY = CERT_PATH + "client_sign.key";
const std::string CLIENT_ENC_CERT = CERT_PATH + "client_enc.crt";
const std::string CLIENT_ENC_KEY = CERT_PATH + "client_enc.key";

async_simple::coro::Lazy<void> test_ntls_http_client_mutual_auth() {
    coro_http_client client{};
    
    // 初始化NTLS客户端（双向认证）
    bool init_ok = client.init_ntls_client(
        CLIENT_SIGN_CERT,       // 客户端签名证书
        CLIENT_SIGN_KEY,        // 客户端签名私钥
        CLIENT_ENC_CERT,        // 客户端加密证书
        CLIENT_ENC_KEY,         // 客户端加密私钥
        CA_CERT,                // CA证书
        asio::ssl::verify_peer  // 验证服务器证书
    );
    
    if (!init_ok) {
        std::cout << "Failed to initialize NTLS client" << std::endl;
        co_return;
    }
    
    // 发送请求（需要客户端证书）
    auto response = co_await client.async_get("https://localhost:8802/");
    // ...
}
```

**TLS 1.3 + 国密模式：**

```cpp
// 服务器端
const std::string SERVER_GM_CERT = CERT_PATH + "server_sign.crt";
const std::string SERVER_GM_KEY = CERT_PATH + "server_sign.key";

server.init_ntls(
    "", SERVER_GM_CERT, SERVER_GM_KEY, "", "", CA_CERT,
    false,  // 单向认证
    "TLS_SM4_GCM_SM3:TLS_SM4_CCM_SM3",  // TLS 1.3国密密码套件
    coro_http_connection::ntls_mode::tls13_single_cert
);

// 客户端
bool init_ok = client.init_ntls_tls13_gm_client(
    "",                      // 单向认证不需要客户端证书
    "",                      // 单向认证不需要客户端私钥
    CA_CERT,                 // CA证书
    asio::ssl::verify_peer,  // 验证服务器证书
    "TLS_SM4_GCM_SM3:TLS_SM4_CCM_SM3"  // TLS 1.3国密密码套件
);
```

## 证书管理

### 使用Tongsuo测试证书（推荐用于开发测试）

最简单的方式是直接使用Tongsuo源码中自带的测试证书：

```bash
# Tongsuo源码目录中的测试证书位置
TONGSUO_SRC/test/certs/sm2/

# 将测试证书复制到安装目录（CI中会自动完成）
cp -r /path/to/Tongsuo/test/certs/sm2 /usr/local/tongsuo/ssl/certs/
```

这些证书已经包含了：
- CA证书（`chain-ca.crt`）
- 服务器签名证书和私钥（`server_sign.crt`, `server_sign.key`）
- 服务器加密证书和私钥（`server_enc.crt`, `server_enc.key`）
- 客户端签名证书和私钥（`client_sign.crt`, `client_sign.key`）
- 客户端加密证书和私钥（`client_enc.crt`, `client_enc.key`）

### 生成SM2证书

如果需要生成自己的SM2证书，可以参考 [Tongsuo官方文档：使用 Tongsuo 签发 SM2 双证书](https://www.yuque.com/tsdoc/ts/sulazb)。

以下是基本的生成步骤：

#### 1. 生成CA私钥和证书

```bash
# 生成CA私钥
tongsuo genpkey -algorithm SM2 -out ca.key

# 生成CA证书
tongsuo req -new -x509 -key ca.key -out ca.crt -days 365 \
    -subj "/C=CN/ST=Beijing/L=Beijing/O=Test/OU=Test/CN=Test CA"
```

#### 2. 生成服务器签名证书（TLCP模式需要）

```bash
# 生成服务器签名私钥
tongsuo genpkey -algorithm SM2 -out server_sign.key

# 生成服务器签名证书请求
tongsuo req -new -key server_sign.key -out server_sign.csr \
    -subj "/C=CN/ST=Beijing/L=Beijing/O=Test/OU=Test/CN=localhost"

# 使用CA签名服务器签名证书
tongsuo x509 -req -in server_sign.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server_sign.crt -days 365
```

#### 3. 生成服务器加密证书（TLCP模式需要）

```bash
# 生成服务器加密私钥
tongsuo genpkey -algorithm SM2 -out server_enc.key

# 生成服务器加密证书请求
tongsuo req -new -key server_enc.key -out server_enc.csr \
    -subj "/C=CN/ST=Beijing/L=Beijing/O=Test/OU=Test/CN=localhost"

# 使用CA签名服务器加密证书
tongsuo x509 -req -in server_enc.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server_enc.crt -days 365
```

#### 4. 生成客户端证书（双向认证需要）

```bash
# 生成客户端签名私钥
tongsuo genpkey -algorithm SM2 -out client_sign.key

# 生成客户端签名证书请求
tongsuo req -new -key client_sign.key -out client_sign.csr \
    -subj "/C=CN/ST=Beijing/L=Beijing/O=Test/OU=Test/CN=client"

# 使用CA签名客户端签名证书
tongsuo x509 -req -in client_sign.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out client_sign.crt -days 365

# 生成客户端加密私钥和证书（类似步骤）
tongsuo genpkey -algorithm SM2 -out client_enc.key
tongsuo req -new -key client_enc.key -out client_enc.csr \
    -subj "/C=CN/ST=Beijing/L=Beijing/O=Test/OU=Test/CN=client"
tongsuo x509 -req -in client_enc.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out client_enc.crt -days 365
```

#### 5. 清理临时文件

```bash
rm *.csr *.srl
```

**注意：** 生产环境请使用正式的CA机构签发的证书，并妥善保管私钥文件。

### 证书文件组织

建议的证书目录结构：

```
/usr/local/tongsuo/ssl/certs/sm2/
├── ca.crt              # CA根证书
├── server_sign.crt     # 服务器签名证书（TLCP）
├── server_sign.key     # 服务器签名私钥（TLCP）
├── server_enc.crt      # 服务器加密证书（TLCP）
├── server_enc.key      # 服务器加密私钥（TLCP）
├── client_sign.crt     # 客户端签名证书（双向认证）
├── client_sign.key     # 客户端签名私钥（双向认证）
└── chain-ca.crt        # CA证书链
```

## 认证模式

### 单向认证

服务器提供证书，客户端验证服务器身份：

- **适用场景**：大多数Web应用、API服务
- **配置**：`enable_client_verify = false`
- **客户端**：只需要CA证书验证服务器

### 双向认证（Mutual Authentication）

服务器和客户端都需要提供证书：

- **适用场景**：高安全要求的系统、内网服务
- **配置**：`enable_client_verify = true`
- **客户端**：需要提供客户端证书和私钥


## 示例代码

完整的示例代码位于：

- **RPC示例**：`src/coro_rpc/examples/base_examples/ntls_server.cpp` 和 `ntls_client.cpp`
- **HTTP示例**：`src/coro_http/examples/ntls_http_server.cpp` 和 `ntls_http_client.cpp`

运行示例：

```bash
# 编译示例
cmake --build build --target ntls_server ntls_client

# 运行服务器（后台）
./build/output/examples/coro_rpc/ntls_server &

# 运行客户端
./build/output/examples/coro_rpc/ntls_client
```

## 兼容性说明

### 默认行为

- **NTLS功能默认关闭**：不影响现有代码，需要显式启用
- **条件编译**：通过 `YLT_ENABLE_NTLS` 宏控制，未启用NTLS时相关代码不会编译

### 与标准SSL共存

yaLanTingLibs 支持同时使用标准SSL和NTLS：

- 标准SSL：使用OpenSSL，支持RSA/AES等国际算法
- NTLS：使用Tongsuo，支持SM2/SM3/SM4国密算法

两者可以共存，通过不同的配置选择使用。

## 测试

yaLanTingLibs 提供了完整的NTLS测试：

- **CI测试**：`.github/workflows/ntls_test.yml` 自动测试NTLS功能
- **端到端测试**：`scripts/run_ntls_e2e_tests.sh` 验证完整的通信流程
- **协议验证**：可以使用Tongsuo自带的命名行与ylt的rpc或者http进行测试，具体参考：https://www.yuque.com/tsdoc/ts/hedgqf 使用方法
 
## 相关资源

- **Tongsuo项目**：https://github.com/Tongsuo-Project/Tongsuo
- **GB/T 38636-2020标准**：国家密码管理局
- **RFC 8998**：https://www.rfc-editor.org/rfc/rfc8998.html
- **yaLanTingLibs文档**：https://github.com/alibaba/yalantinglibs
- **国密TLCP使用手册**：https://www.yuque.com/tsdoc/ts/hedgqf
- **使用 Tongsuo 签发 SM2 双证书**：https://www.yuque.com/tsdoc/ts/sulazb 
- **在 TLS1.3 中使用商用密码算法**：https://www.yuque.com/tsdoc/ts/grur3x
## 总结

yaLanTingLibs 的NTLS支持为需要符合国密标准的应用提供了：

✅ **完整的国密协议支持**：GB/T 38636-2020 TLCP 和 RFC 8998  
✅ **简单易用的API**：与标准SSL API保持一致  
✅ **生产就绪**：完整的测试、文档和示例  

这使得 yaLanTingLibs 成为构建符合国密标准的高性能网络应用的理想选择。
