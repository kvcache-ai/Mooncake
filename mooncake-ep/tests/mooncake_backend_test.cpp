#include <ATen/cuda/CUDAContext.h>
#include <c10/cuda/CUDAGuard.h>
#include <gtest/gtest.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <torch/csrc/distributed/c10d/ProcessGroup.hpp>
#include <torch/csrc/distributed/c10d/HashStore.hpp>
#include <mooncake_backend.h>

namespace mooncake {

constexpr size_t kNumRanks = 2;

class MooncakeBackendTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto store = c10::make_intrusive<::c10d::HashStore>();
        std::thread threads[kNumRanks];
        for (size_t rank = 0; rank < kNumRanks; ++rank) {
            threads[rank] = std::thread([this, store, rank] {
                cudaSetDevice(rank);
                c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>
                    options;
                backends[rank].reset(new MooncakeBackend(store, rank, kNumRanks,
                                                         options, false, true));
            });
        }
        for (size_t rank = 0; rank < kNumRanks; ++rank) {
            threads[rank].join();
        }
    }

    void TearDown() override {}

    std::shared_ptr<MooncakeBackend> backends[kNumRanks];
};

TEST_F(MooncakeBackendTest, AllgatherTest) {
    std::vector<c10::intrusive_ptr<c10d::Work>> works;
    std::vector<std::vector<std::vector<at::Tensor>>> allOutputTensors;
    std::vector<std::vector<torch::Tensor>> allInputTensors;
    auto options = torch::dtype(torch::kInt32).device(torch::kCUDA);
    for (size_t rank = 0; rank < kNumRanks; ++rank) {
        auto stream = at::cuda::getStreamFromPool(false, rank);
        c10::cuda::CUDAStreamGuard guard(stream);
        std::vector<at::Tensor> outputTensorsInner;
        for (size_t i = 0; i < kNumRanks; ++i) {
            outputTensorsInner.emplace_back(
                torch::zeros({2, 2}, options.device_index(rank)));
        }
        std::vector<std::vector<at::Tensor>> outputTensors;
        outputTensors.emplace_back(outputTensorsInner);

        std::vector<at::Tensor> inputTensors;
        inputTensors.emplace_back(
            torch::full({2, 2}, rank, options.device_index(rank)));

        works.emplace_back(backends[rank]->allgather(
            outputTensors, inputTensors, c10d::AllgatherOptions()));
        allOutputTensors.emplace_back(outputTensors);
        allInputTensors.emplace_back(inputTensors);
    }
    for (size_t rank = 0; rank < kNumRanks; ++rank) {
        auto stream = at::cuda::getStreamFromPool(false, rank);
        c10::cuda::CUDAStreamGuard guard(stream);
        bool success = works[rank]->wait();
        ASSERT_TRUE(success) << "Allgather failed at rank " << rank;

        std::vector<at::Tensor> outputTensors = allOutputTensors[rank][0];
        for (size_t i = 0; i < kNumRanks; ++i) {
            auto expected = torch::full({2, 2}, i, options.device_index(rank));
            ASSERT_TRUE(torch::allclose(outputTensors[i], expected))
                << "Allgather result mismatch. Got: " << outputTensors[i]
                << " Expected: " << expected;
        }
    }
}

TEST_F(MooncakeBackendTest, AllreduceTest) {
    std::vector<c10::intrusive_ptr<c10d::Work>> works;
    std::vector<std::vector<torch::Tensor>> allTensors;
    auto options = torch::dtype(torch::kInt32).device(torch::kCUDA);
    for (size_t rank = 0; rank < kNumRanks; ++rank) {
        auto stream = at::cuda::getStreamFromPool(false, rank);
        c10::cuda::CUDAStreamGuard guard(stream);
        std::vector<at::Tensor> tensors;
        tensors.emplace_back(
            torch::full({2, 2}, rank, options.device_index(rank)));
        works.emplace_back(
            backends[rank]->allreduce(tensors, c10d::AllreduceOptions()));
        allTensors.emplace_back(tensors);
    }
    for (size_t rank = 0; rank < kNumRanks; ++rank) {
        auto stream = at::cuda::getStreamFromPool(false, rank);
        c10::cuda::CUDAStreamGuard guard(stream);
        bool success = works[rank]->wait();
        ASSERT_TRUE(success) << "Allreduce failed at rank " << rank;

        at::Tensor tensor = allTensors[rank][0];
        auto expected = torch::full({2, 2}, kNumRanks * (kNumRanks - 1) / 2,
                                    options.device_index(rank));
        ASSERT_TRUE(torch::allclose(tensor, expected))
            << "Allreduce result mismatch. Got: " << tensor
            << " Expected: " << expected;
    }
}

TEST_F(MooncakeBackendTest, BroadcastTest) {
    std::vector<c10::intrusive_ptr<c10d::Work>> works;
    std::vector<std::vector<torch::Tensor>> allTensors;
    auto options = torch::dtype(torch::kInt32).device(torch::kCUDA);
    for (size_t rank = 0; rank < kNumRanks; ++rank) {
        auto stream = at::cuda::getStreamFromPool(false, rank);
        c10::cuda::CUDAStreamGuard guard(stream);
        std::vector<at::Tensor> tensors;
        c10d::BroadcastOptions opts;
        int data = rank == opts.rootRank + opts.rootTensor ? 42 : 0;
        tensors.emplace_back(
            torch::full({2, 2}, data, options.device_index(rank)));
        works.emplace_back(backends[rank]->broadcast(tensors, opts));
        allTensors.emplace_back(tensors);
    }
    for (size_t rank = 0; rank < kNumRanks; ++rank) {
        auto stream = at::cuda::getStreamFromPool(false, rank);
        c10::cuda::CUDAStreamGuard guard(stream);
        bool success = works[rank]->wait();
        ASSERT_TRUE(success) << "Broadcast failed at rank " << rank;

        at::Tensor tensor = allTensors[rank][0];
        auto expected = torch::full({2, 2}, 42, options.device_index(rank));
        ASSERT_TRUE(torch::allclose(tensor, expected))
            << "Broadcast result mismatch. Got: " << tensor
            << " Expected: " << expected;
    }
}

}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
