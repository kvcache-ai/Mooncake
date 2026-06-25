#include <gflags/gflags.h>

#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include "cuda_alike.h"
#include "transport/device/device_transport.h"

DEFINE_string(device, "", "RDMA device name, e.g. mlx5_10. Empty means auto");
DEFINE_int32(num_ranks, 1, "Number of ranks for the DeviceTransport");
DEFINE_int32(num_qps, 1, "Number of QPs to create");
DEFINE_int32(gpu_id, 0, "CUDA-like device id");

namespace {

void checkCuda(cudaError_t err, const char* what) {
    if (err != cudaSuccess) {
        throw std::runtime_error(std::string(what) + ": " +
                                 cudaGetErrorString(err));
    }
}

void check(bool cond, const char* what) {
    if (!cond) throw std::runtime_error(what);
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    try {
        int device_count = 0;
        checkCuda(cudaGetDeviceCount(&device_count), "cudaGetDeviceCount");
        if (device_count <= 0) {
            std::cout << "No CUDA-like devices found; skipping\n";
            return 0;
        }
        check(FLAGS_gpu_id >= 0 && FLAGS_gpu_id < device_count,
              "invalid --gpu_id");
        checkCuda(cudaSetDevice(FLAGS_gpu_id), "cudaSetDevice");

        auto rdma = mooncake::device::createIbgdaDeviceTransport();
        check(static_cast<bool>(rdma),
              "createIbgdaDeviceTransport returned null");

        int ret =
            rdma->initialize(FLAGS_device, FLAGS_num_ranks, FLAGS_num_qps);
        check(ret == 0, "RdmaTransport::initialize failed");

        ret = rdma->allocateControlBuffer();
        check(ret == 0, "RdmaTransport::allocateControlBuffer failed");

        cudaStream_t stream = nullptr;
        checkCuda(cudaStreamCreate(&stream), "cudaStreamCreate");
        ret = rdma->createQueuePairs(reinterpret_cast<void*>(stream));
        check(ret == 0, "RdmaTransport::createQueuePairs failed");
        checkCuda(cudaStreamSynchronize(stream), "cudaStreamSynchronize");
        cudaStreamDestroy(stream);

        auto meta = rdma->localMetadata();
        check(static_cast<int>(meta.qpns.size()) == FLAGS_num_qps,
              "unexpected qpn count");
        check(static_cast<int>(meta.lids.size()) == FLAGS_num_qps,
              "unexpected lid count");
        check(rdma->qpDevCtxsPtr() != nullptr, "qpDevCtxsPtr is null");

        std::cout << "device_ibgda_transport_smoke_test passed"
                  << " device="
                  << (FLAGS_device.empty() ? "<auto>" : FLAGS_device)
                  << " gid_index=" << rdma->gidIndex()
                  << " roce=" << rdma->isRoce()
                  << " qpn0=" << meta.qpns.front() << "\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "device_ibgda_transport_smoke_test failed: " << e.what()
                  << "\n";
        return 1;
    } catch (...) {
        std::cerr << "device_ibgda_transport_smoke_test failed: unknown error\n";
        return 1;
    }
}
