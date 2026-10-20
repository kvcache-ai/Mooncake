#include <gflags/gflags.h>
#include <glog/logging.h>
#include <spdk/env.h>

#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "transfer_task.h"

DEFINE_string(endpoint, "", "TCP controller endpoint without a namespace ID");
DEFINE_string(control_dir, "",
              "Target lifecycle markers written by the runner");
DEFINE_uint32(timeout_sec, 30, "Timeout for each test phase");

namespace {
using namespace mooncake;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
constexpr size_t kIoSize = 256 * 1024;
using Segments = std::array<nof_seg_handle*, 2>;
using Buffer = std::unique_ptr<void, decltype(&spdk_free)>;
using Buffers = std::array<Buffer, 2>;
using Requests = std::array<std::shared_ptr<SpdkNofOperationState>, 2>;

void Require(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}

Clock::time_point Deadline() {
    return Clock::now() + std::chrono::seconds(FLAGS_timeout_sec);
}

void WaitForMarker(const char* name) {
    auto deadline = Deadline();
    auto path = std::filesystem::path(FLAGS_control_dir) / name;
    while (!std::filesystem::exists(path)) {
        Require(Clock::now() < deadline,
                "Timed out waiting for " + path.string());
        std::this_thread::sleep_for(1ms);
    }
}

std::thread::id WorkerBarrier(SpdkNofWorkerPool& pool, int index) {
    auto done = std::make_shared<std::promise<std::thread::id>>();
    auto future = done->get_future();
    pool.PostControlTask(
        index, [done](auto&) { done->set_value(std::this_thread::get_id()); });
    Require(future.wait_until(Deadline()) == std::future_status::ready,
            "Worker barrier timed out");
    return future.get();
}

Requests Submit(SpdkNofWorkerPool& pool, const Segments& segments,
                const Buffers& buffers, int op) {
    Requests requests;
    for (size_t i = 0; i < segments.size(); ++i) {
        auto block_size = SpdkWrapper::GetInstance().GetBlockSize(segments[i]);
        requests[i] = std::make_shared<SpdkNofOperationState>();
        pool.submitTask(SpdkNofTask(segments[i], buffers[i].get(), 0,
                                    kIoSize / block_size, op, requests[i]));
    }
    return requests;
}

bool Wait(const Requests& requests, Clock::time_point deadline) {
    bool success = true;
    for (const auto& request : requests) {
        while (!request->is_completed()) {
            Require(Clock::now() < deadline, "I/O did not complete");
            std::this_thread::sleep_for(1ms);
        }
        success &= request->get_result() == ErrorCode::OK;
    }
    return success;
}

bool WriteRead(SpdkNofWorkerPool& pool, const Segments& segments,
               const Buffers& buffers, int pattern,
               Clock::time_point deadline) {
    for (size_t i = 0; i < buffers.size(); ++i)
        std::memset(buffers[i].get(), pattern + i, kIoSize);
    if (!Wait(Submit(pool, segments, buffers, kSpdkNofOpWrite), deadline))
        return false;
    for (const auto& buffer : buffers) std::memset(buffer.get(), 0, kIoSize);
    if (!Wait(Submit(pool, segments, buffers, kSpdkNofOpRead), deadline))
        return false;
    for (size_t i = 0; i < buffers.size(); ++i) {
        auto* bytes = static_cast<unsigned char*>(buffers[i].get());
        for (size_t offset = 0; offset < kIoSize; ++offset)
            Require(bytes[offset] == pattern + i,
                    "Data mismatch in namespace " + std::to_string(i + 1));
    }
    return true;
}

void Run() {
    Require(!FLAGS_endpoint.empty() && !FLAGS_control_dir.empty() &&
                FLAGS_timeout_sec > 0,
            "Specify --endpoint, --control_dir and a positive --timeout_sec");
    Require(setenv("MC_NOF_WORKERS", "2", 1) == 0, "setenv failed");
    auto& wrapper = SpdkWrapper::GetInstance();
    // Buffers outlive the pool, including when a failed check unwinds Run().
    Buffers buffers{Buffer(wrapper.Alloc(kIoSize, 4096, -1), spdk_free),
                    Buffer(wrapper.Alloc(kIoSize, 4096, -1), spdk_free)};
    Require(buffers[0] && buffers[1], "DMA buffer allocation failed");
    SpdkNofWorkerPool pool;
    Segments segments{};
    for (size_t i = 0; i < segments.size(); ++i) {
        segments[i] = pool.OpenNofSegment(FLAGS_endpoint +
                                          " ns:" + std::to_string(i + 1));
        Require(segments[i] != nullptr, "Open failed");
        auto block_size = wrapper.GetBlockSize(segments[i]);
        Require(block_size != 0 && block_size != INVALID_BLOCK_SIZE &&
                    kIoSize % block_size == 0,
                "Invalid namespace block size");
    }
    Require(wrapper.GetController(segments[0]) ==
                wrapper.GetController(segments[1]),
            "Namespaces must share one controller");
    auto first = WorkerBarrier(pool, 0);
    Require(first != WorkerBarrier(pool, 1), "Expected two distinct workers");
    Require(WriteRead(pool, segments, buffers, 0x31, Deadline()),
            "Initial read/write failed");
    std::cout << "PASS initial_io" << std::endl;

    // The runner freezes the target before these requests are issued, so it
    // cannot race to complete them before the fault. Neither pool nor handles
    // are recreated during the test.
    WaitForMarker("target-paused");
    auto pending = Submit(pool, segments, buffers, kSpdkNofOpWrite);
    for (int round = 0; round < 2; ++round)
        for (int index = 0; index < 2; ++index) WorkerBarrier(pool, index);
    // Control tasks precede I/O within a snapshot. Two barriers ensure the
    // worker has processed the request snapshot before we announce the fault.
    for (const auto& request : pending)
        Require(!request->is_completed(), "Target completed I/O while paused");
    std::cout << "READY fault_io_pending" << std::endl;

    Wait(pending, Deadline());
    for (const auto& request : pending)
        Require(request->get_result() == ErrorCode::TRANSFER_FAIL,
                "Disconnected I/O did not fail");
    std::cout << "PASS failed_io_drained" << std::endl;

    WaitForMarker("target-restarted");
    auto deadline = Deadline();
    // Malloc bdev data is lost on restart: write a new pattern before reading.
    while (!WriteRead(pool, segments, buffers, 0x73, deadline)) {
        Require(Clock::now() < deadline, "Controller did not recover");
        std::this_thread::sleep_for(10ms);
    }
}
}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    try {
        Run();
        std::cout << "PASS controller_recovery" << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL controller_recovery: " << error.what() << std::endl;
        return 1;
    }
}
