#include <gflags/gflags.h>
#include <glog/logging.h>
#include <algorithm>
#include <cctype> 
#include "common.h"
#include "transfer_engine.h"
#include "transport/transport.h"
#include "acl/acl.h"
#include "hccl.h"

DEFINE_int32(batch_size, 64, "Batch size");
DEFINE_uint64(block_size, 131072, "Block size for each transfer request");
DEFINE_uint64(device_id, 0, "The device logic and phy ID of this machine");
DEFINE_string(mode, "dtod", "device to device or device to host");

using namespace mooncake;

std::string toLowerCase(const std::string& str) {
    std::string lowerStr = str;
    std::transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return lowerStr;
}

int aclDeviceToHost() {
    void* devAddr;
    void* hostAddr;
    uint64_t totalSize = FLAGS_block_size * FLAGS_batch_size;
    aclError ret = aclrtMalloc(&devAddr, totalSize, ACL_MEM_MALLOC_NORMAL_ONLY);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to allocate device memory, ret:" << ret;
        return ret;
    }

    ret = aclrtMallocHost(&hostAddr, totalSize);
    if (ret != ACL_ERROR_NONE || hostAddr == nullptr) {
        LOG(ERROR) << "Failed to allocate host memory, ret:" << ret;
        return ret;
    }

    for (size_t i = 0; i < totalSize; i += sizeof(uint32_t)) {
        *(uint32_t*)((char *)hostAddr + i) = 0x12345678;
    }

    ret = aclrtMemcpy(devAddr, totalSize, hostAddr, totalSize, ACL_MEMCPY_HOST_TO_DEVICE);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to copy data from host to device, ret: " << ret;
        return ret;
    }

    constexpr std::uint64_t kBatch = 10000;
    std::uint64_t cnt = 0;
    std::chrono::microseconds total_us{0};

    while (1) {
        auto start = std::chrono::high_resolution_clock::now();

        ret = aclrtMemcpy(devAddr, totalSize, hostAddr, totalSize, ACL_MEMCPY_HOST_TO_DEVICE);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to copy data from host to device, ret: " << ret;
            return ret;
        }

        ret = aclrtMemcpy(hostAddr, totalSize, devAddr, totalSize, ACL_MEMCPY_DEVICE_TO_HOST);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to copy data from device to host, ret: " << ret;
            return ret;
        }

        auto stop = std::chrono::high_resolution_clock::now();
        total_us += std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        cnt++;

        if (cnt >= kBatch) {
            double avg_us = static_cast<double>(total_us.count()) / kBatch;
            LOG(INFO) << ", total_size: " << totalSize * 2 / 1024 << "KB";
            LOG(INFO) << "avg_memcpy: " << avg_us << " us, avg_bw: "
                    << (static_cast<double>(totalSize) / avg_us / 1e3) << " GB/s";
            cnt = 0;
            total_us = std::chrono::microseconds{0};
        }

        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    
    ret = aclrtFree(devAddr);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to aclrtFree, ret: " << ret;
        return ret;
    }

    ret = aclrtFreeHost(hostAddr);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to aclrtFreeHost, ret: " << ret;
        return ret;
    }
    
    return 0;
}

int aclDeviceToDevice() {
    void* hugeBlock;          // 单个大块内存
    std::vector<void*> smallBlocks; // 多个小块内存
    uint64_t block_size = FLAGS_block_size;
    uint64_t block_num = FLAGS_batch_size;
    uint64_t hugeSize = block_size * block_num;
    aclrtStream stream;

    aclError ret = aclrtCreateStream(&stream);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtCreateStream error, ret: " << ret;
    }

    smallBlocks.resize(block_num);
    ret = aclrtMalloc(&hugeBlock, hugeSize, ACL_MEM_MALLOC_NORMAL_ONLY);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to allocate device memory, ret:" << ret;
        return ret;
    }

    for (uint64_t i = 0; i < block_num; ++i) {
        ret = aclrtMalloc(&smallBlocks[i], block_size, ACL_MEM_MALLOC_NORMAL_ONLY);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to allocate device memory, ret:" << ret;
            return ret;
        }
    }

    void* tmpAddr = hugeBlock;
    for (auto addr : smallBlocks) {
        ret = aclrtMemcpyAsync(tmpAddr, block_size, addr, block_size, ACL_MEMCPY_DEVICE_TO_DEVICE, stream);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to copy data from device to device, ret: " << ret;
            return ret;
        }
        tmpAddr = static_cast<char*>(tmpAddr) + block_size;
    }

    constexpr std::uint64_t kBatch = 10000;
    std::uint64_t cnt = 0;
    std::chrono::microseconds total_us{0};

    while (1) {
        auto start = std::chrono::high_resolution_clock::now();
        tmpAddr = hugeBlock;
        for (auto addr : smallBlocks) {
            ret = aclrtMemcpyAsync(tmpAddr, block_size, addr, block_size, ACL_MEMCPY_DEVICE_TO_DEVICE, stream);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to copy data from device to device, ret: " << ret;
                return ret;
            }
            tmpAddr = static_cast<char*>(tmpAddr) + block_size;
        }

        ret = aclrtSynchronizeStream(stream);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to aclrtSynchronizeStream, ret: " << ret;
            return ret;
        }

        auto stop = std::chrono::high_resolution_clock::now();
        total_us += std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        cnt++;

        if (cnt >= kBatch) {
            double avg_us = static_cast<double>(total_us.count()) / kBatch;
            LOG(INFO) << " block_size: " << block_size / 1024
                  << "KB, block_num: " << block_num
                  << ", total_size: " << hugeSize / 1024 << "KB";
            LOG(INFO) << "avg_memcpy: " << avg_us << " us, avg_bw: "
                    << (static_cast<double>(hugeSize) / avg_us / 1e3) << " GB/s";
            cnt = 0;
            total_us = std::chrono::microseconds{0};
        }

        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    
    for (auto addr : smallBlocks) {
        ret = aclrtFree(addr);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to aclrtFree, ret: " << ret;
            return ret;
        }
    }

    ret = aclrtFree(hugeBlock);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to aclrtFree, ret: " << ret;
        return ret;
    }

    return 0;
}


int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    int deviceLogicid = FLAGS_device_id;
    const char *aclConfigPath = nullptr;
    aclError ret = aclInit(aclConfigPath);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to initialize ACL, ret: " << ret;
        return -1;
    }

    ret = aclrtSetDevice(deviceLogicid);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to set device, ret: " << ret;
        return -1;
    }

    aclrtContext context = nullptr;
    ret = aclrtCreateContext(&context, deviceLogicid);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to create context, ret: " << ret;
        return ret;
    }

    std::string mode = toLowerCase(FLAGS_mode);

    if (mode == "dtoh" || mode == "htod") {
        LOG(INFO) << "Detected mode: " << mode;
        return aclDeviceToHost();
    } else if (mode == "dtod") {
        LOG(INFO) << "Detected mode: " << mode;
        return aclDeviceToDevice();
    }

    LOG(ERROR) << "Unsupported mode: must be 'dtod' or 'dtoh' or 'htod'";
    exit(EXIT_FAILURE);
}