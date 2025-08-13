#include <gtest/gtest.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <torch/csrc/distributed/c10d/ProcessGroup.hpp>
#include <torch/csrc/distributed/c10d/FileStore.hpp>
#include <mooncake_backend.h>

namespace mooncake {

class MooncakeBackendTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto store = c10::make_intrusive<::c10d::FileStore>(
            "/tmp/mooncake_backend_test_store", 1);
        auto options =
            c10::make_intrusive<::c10d::Backend::Options>("mooncake");
        backend = std::make_shared<MooncakeBackend>(store, 0, 1, options);
    }

    void TearDown() override {}

    std::shared_ptr<MooncakeBackend> backend;
};

TEST_F(MooncakeBackendTest, AllreduceTest) {
    std::vector<torch::Tensor> tensors;
    tensors.push_back(torch::ones({2, 2}));

    ::c10d::AllreduceOptions opts;
    opts.reduceOp = ::c10d::ReduceOp::SUM;

    auto work = backend->allreduce(tensors, opts);
    work->wait();

    auto expected = torch::ones({2, 2}) * backend->getSize();
    ASSERT_TRUE(torch::allclose(tensors[0], expected))
        << "Allreduce result mismatch. Got: " << tensors[0]
        << " Expected: " << expected;
}

TEST_F(MooncakeBackendTest, BroadcastTest) {
    std::vector<torch::Tensor> tensors;
    tensors.push_back(torch::ones({2, 2}));

    ::c10d::BroadcastOptions opts;
    opts.rootRank = 0;

    EXPECT_THROW({ backend->broadcast(tensors, opts); }, c10::Error);
}

}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
