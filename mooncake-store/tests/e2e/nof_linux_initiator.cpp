#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>

#include "nof/linux_initiator.h"

namespace {
void Require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

struct Completion {
    int calls = 0;
    bool success = false;
    static void Done(void* context, const mooncake::NofIOCompletion& result) {
        auto& completion = *static_cast<Completion*>(context);
        ++completion.calls;
        completion.success = result.success;
    }
};
}  // namespace

int main() {
    using namespace mooncake;
    try {
        const char* endpoint = std::getenv("MC_TEST_NOF_ENDPOINT");
        Require(endpoint && *endpoint,
                "MC_TEST_NOF_ENDPOINT is required; run "
                "run_nof_linux_initiator_e2e.sh");
        constexpr size_t size = 8192;
        void* allocation = nullptr;
        Require(posix_memalign(&allocation, 4096, size) == 0,
                "aligned buffer allocation failed");
        std::unique_ptr<void, decltype(&free)> buffer(allocation, free);
        std::array<Completion, 2> completions;
        std::array<NofIOAdaptor, 2> adaptors{{
            {Completion::Done, &completions[0]},
            {Completion::Done, &completions[1]},
        }};
        LinuxNvmeInitiator initiator;
        auto* handle = initiator.OpenSegment(endpoint);
        Require(handle != nullptr, "namespace discovery/open failed");
        const auto block_size = initiator.GetBlockSize(handle);
        Require(block_size == 512 || block_size == 4096,
                "unsupported logical block size");
        auto transfer = [&](NofIOOp op, size_t index) {
            Require(initiator.SubmitIO(handle, buffer.get(), 4096, size, op,
                                       &adaptors[index]) == 0,
                    "I/O submission failed");
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (!completions[index].calls) {
                Require(std::chrono::steady_clock::now() < deadline,
                        "I/O completion timed out");
                Require(initiator.PollCompletion(handle, 0) >= 0,
                        "completion polling failed");
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            Require(completions[index].calls == 1 && completions[index].success,
                    "I/O completion failed or callback was duplicated");
        };
        std::memset(buffer.get(), 0x5a, size);
        transfer(NofIOOp::kWrite, 0);
        std::memset(buffer.get(), 0, size);
        transfer(NofIOOp::kRead, 1);
        for (size_t i = 0; i < size; ++i) {
            Require(static_cast<unsigned char*>(buffer.get())[i] == 0x5a,
                    "readback payload mismatch");
        }
        Require(completions[0].calls == 1 && completions[1].calls == 1,
                "completion callback was duplicated");
        std::cout << "Linux NoF write/read payload verification passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "Linux NoF E2E failed: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
