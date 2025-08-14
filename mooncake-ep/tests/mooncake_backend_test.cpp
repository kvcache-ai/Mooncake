#include <gtest/gtest.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <torch/csrc/distributed/c10d/ProcessGroup.hpp>
#include <torch/csrc/distributed/c10d/FileStore.hpp>
#include <mooncake_backend.h>

namespace mooncake {

constexpr size_t kNumRanks = 1;

class MooncakeBackendTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto store = c10::make_intrusive<::c10d::FileStore>(
            "/tmp/mooncake_backend_test_store", 1);
        auto options =
            c10::make_intrusive<::c10d::Backend::Options>("mooncake");
        for (size_t rank = 0; rank < kNumRanks; ++rank) {
            backends.emplace_back(std::make_shared<MooncakeBackend>(
                store, rank, kNumRanks, options));
        }
    }

    void TearDown() override {}

    std::vector<std::shared_ptr<MooncakeBackend>> backends;
};

TEST_F(MooncakeBackendTest, AllgatherTest) {
    std::vector<c10::intrusive_ptr<c10d::Work>> works;
    std::vector<std::vector<std::vector<at::Tensor>>> allOutputTensors;
    std::vector<std::vector<torch::Tensor>> allInputTensors;
    for (size_t rank = 0; rank < kNumRanks; ++rank) {
        std::vector<at::Tensor> outputTensorsInner;
        for (size_t i = 0; i < kNumRanks; ++i) {
            outputTensorsInner.emplace_back(torch::zeros({2, 2}));
        }
        std::vector<std::vector<at::Tensor>> outputTensors;
        outputTensors.emplace_back(outputTensorsInner);

        std::vector<at::Tensor> inputTensors;
        inputTensors.push_back(torch::full({2, 2}, rank));

        works.emplace_back(backends[rank]->allgather(
            outputTensors, inputTensors, c10d::AllgatherOptions()));
        allOutputTensors.emplace_back(outputTensors);
        allInputTensors.emplace_back(inputTensors);
    }
    for (size_t rank = 0; rank < kNumRanks; ++rank) {
        bool success = works[rank]->wait();
        ASSERT_TRUE(success) << "Allgather failed at rank " << rank;

        std::vector<at::Tensor> outputTensors = allOutputTensors[rank][0];
        for (size_t i = 0; i < kNumRanks; ++i) {
            auto expected = torch::full({2, 2}, i);
            ASSERT_TRUE(torch::allclose(outputTensors[i], expected))
                << "Allgather result mismatch. Got: " << outputTensors[i]
                << " Expected: " << expected;
        }
    }
}

TEST_F(MooncakeBackendTest, BroadcastTest) {
    std::vector<torch::Tensor> tensors;
    tensors.push_back(torch::ones({2, 2}));

    ::c10d::BroadcastOptions opts;
    opts.rootRank = 0;

    EXPECT_THROW({ backends[0]->broadcast(tensors, opts); }, c10::Error);
}

}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
