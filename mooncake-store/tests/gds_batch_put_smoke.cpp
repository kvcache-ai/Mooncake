#include "real_client.h"
#include "test_server_helpers.h"
#include "gpu_staging_utils.h"

#include <gtest/gtest.h>
#include <cuda_runtime.h>
#include <filesystem>

namespace mooncake {
namespace testing {
namespace {

class GdsBatchPutSmoke : public ::testing::Test {
   protected:
    void SetUp() override {
        std::filesystem::create_directories("/tmp/gds_smoke_test");
        setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
               "offset_allocator_storage_backend", 1);
        setenv("MOONCAKE_ENABLE_GDS", "1", 1);
    }
    void TearDown() override {
        std::filesystem::remove_all("/tmp/gds_smoke_test");
    }
};

TEST_F(GdsBatchPutSmoke, PutWithGpuBuffer) {
    auto config = InProcMasterConfigBuilder()
                      .set_enable_offload(true)
                      .set_enable_disk_eviction(true)
                      .set_root_fs_dir("/tmp/gds_smoke_test")
                      .build();
    InProcMaster master;
    ASSERT_TRUE(master.Start(config));

    ConfigDict cfg;
    cfg["local_hostname"] = "127.0.0.1";
    cfg["metadata_server"] =
        "http://127.0.0.1:" + std::to_string(master.http_metadata_port()) +
        "/metadata";
    cfg["master_server_addr"] =
        "127.0.0.1:" + std::to_string(master.rpc_port());
    cfg["protocol"] = "tcp";

    RealClient client;
    ASSERT_TRUE(client.setup_internal(cfg).has_value());

    const size_t size = 64 * 1024;
    void* gpu = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu, size), cudaSuccess);
    cudaMemset(gpu, 0xAB, size);

    std::vector<std::string> keys = {"gds_smoke_key"};
    std::vector<void*> ptrs = {gpu};
    std::vector<size_t> sizes = {size};
    auto results = client.batch_put_from(keys, ptrs, sizes);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0], 0);

    cudaFree(gpu);
    master.Stop();
}

}  // namespace
}  // namespace testing
}  // namespace mooncake
