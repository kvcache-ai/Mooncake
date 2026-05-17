我现在有一个头文件

gds_interface.h 和对应的

内容如下：这是一个kv存储的接口

namespace GDS {

int32_t init(void* addr, uint64_t len);

int32_t isExists(std::vector<uint64_t> blockIds);

int32_t get(uint64_t blockId, uint8_t *blockAddr, size_t offset, size_t len);

int32_t put(uint64_t blockId, uint8_t *blockAddr, size_t offset, size_t len);

}

# 需求如下：

- Client::Put 调用 put 函数，objectKeyToUint64将ObjectKey转换为uint64_t，调用GDS::put把数据写入kv存储中，而不是写入到原来的file中。 Client::TransferRead 调用GDS::get接口 从kv存储中读取， 对应的batch操作同时进行替换

- 

- 生成对应的CMakefile 添加gds_interface.h 和 libgdsclient.so 到项目的编译中，保证项目可以正常编译，链接到 libgdsclient.so

- 生成对应的测试用例，测试 put 函数的正确性，测试用例中包含以下场景：
    - 正常情况，写入数据后，通过 TransferRead 函数可以正确读取到数据
    - 异常情况，写入数据时，指定的 blockId 不存在，返回错误码
