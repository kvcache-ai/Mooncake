// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <hip/hip_runtime.h>

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "tent/transport/hylink/hylink_transport.h"

namespace {

struct HipScript {
    int copy_calls = 0;
    int accepted_copies = 0;
    int fail_copy_call = 0;
    int event_create_calls = 0;
    int fail_event_create_call = 0;
    int task_event_record_calls = 0;
    int fail_task_event_record_call = 0;
    int stream_sync_calls = 0;
    uintptr_t next_stream = 0x10000;
    uintptr_t next_event = 0x20000;
    std::unordered_set<hipEvent_t> live_events;
};

HipScript g_script;

}  // namespace

extern "C" hipError_t __wrap_hipGetDeviceCount(int* count) {
    *count = 1;
    return hipSuccess;
}

extern "C" hipError_t __wrap_hipGetDevice(int* device) {
    *device = 0;
    return hipSuccess;
}

extern "C" hipError_t __wrap_hipSetDevice(int device) {
    return device == 0 ? hipSuccess : hipErrorInvalidDevice;
}

extern "C" hipError_t __wrap_hipStreamCreate(hipStream_t* stream) {
    *stream = reinterpret_cast<hipStream_t>(g_script.next_stream);
    g_script.next_stream += 0x10;
    return hipSuccess;
}

extern "C" hipError_t __wrap_hipStreamDestroy(hipStream_t stream) {
    (void)stream;
    return hipSuccess;
}

extern "C" hipError_t __wrap_hipStreamSynchronize(hipStream_t stream) {
    (void)stream;
    ++g_script.stream_sync_calls;
    return hipSuccess;
}

extern "C" hipError_t __wrap_hipMemcpyAsync(void* dst, const void* src,
                                            size_t length, hipMemcpyKind kind,
                                            hipStream_t stream) {
    (void)kind;
    (void)stream;
    ++g_script.copy_calls;
    if (g_script.copy_calls == g_script.fail_copy_call) {
        return hipErrorInvalidValue;
    }
    ++g_script.accepted_copies;
    std::memcpy(dst, src, length);
    return hipSuccess;
}

extern "C" hipError_t __wrap_hipEventCreateWithFlags(hipEvent_t* event,
                                                     unsigned int flags) {
    (void)flags;
    ++g_script.event_create_calls;
    if (g_script.event_create_calls == g_script.fail_event_create_call) {
        *event = nullptr;
        return hipErrorOutOfMemory;
    }
    *event = reinterpret_cast<hipEvent_t>(g_script.next_event);
    g_script.next_event += 0x10;
    g_script.live_events.insert(*event);
    return hipSuccess;
}

extern "C" hipError_t __wrap_hipEventRecord(hipEvent_t event,
                                            hipStream_t stream) {
    (void)event;
    if (stream == hipStreamPerThread) return hipSuccess;
    ++g_script.task_event_record_calls;
    if (g_script.task_event_record_calls ==
        g_script.fail_task_event_record_call) {
        return hipErrorInvalidValue;
    }
    return hipSuccess;
}

extern "C" hipError_t __wrap_hipEventSynchronize(hipEvent_t event) {
    (void)event;
    return hipSuccess;
}

extern "C" hipError_t __wrap_hipEventQuery(hipEvent_t event) {
    return g_script.live_events.count(event) == 1 ? hipSuccess
                                                  : hipErrorInvalidHandle;
}

extern "C" hipError_t __wrap_hipEventDestroy(hipEvent_t event) {
    g_script.live_events.erase(event);
    return hipSuccess;
}

extern "C" hipError_t __wrap_hipGetLastError() { return hipSuccess; }

extern "C" const char* __wrap_hipGetErrorString(hipError_t error) {
    (void)error;
    return "injected HIP failure";
}

namespace mooncake {
namespace tent {
namespace {

constexpr size_t kLength = 32;

class HylinkTransportTest : public testing::Test {
   protected:
    void SetUp() override {
        g_script = HipScript{};
        ASSERT_EQ(setenv("MC_HYLINK_USE_VMM", "0", 1), 0);
        for (size_t i = 0; i < sources_.size(); ++i) {
            sources_[i].fill(static_cast<char>(i + 1));
            targets_[i].fill(0);
        }

        metadata_ = std::make_shared<ControlService>("p2p", "", nullptr);
        ASSERT_TRUE(metadata_->segmentManager()
                        .updateLocal([&](SegmentDesc& segment) {
                            segment.name = "hylink_test_segment";
                            segment.machine_id = "hylink_test_machine";
                            segment.type = SegmentType::Memory;
                            auto& memory =
                                std::get<MemorySegmentDesc>(segment.detail);
                            memory.buffers.clear();
                            for (auto& source : sources_) {
                                BufferDesc buffer;
                                buffer.addr =
                                    reinterpret_cast<uint64_t>(source.data());
                                buffer.length = source.size();
                                buffer.location = "hip:0";
                                memory.buffers.push_back(std::move(buffer));
                            }
                            return Status::OK();
                        })
                        .ok());

        std::string local_segment_name = "hylink_test_segment";
        ASSERT_TRUE(
            transport_.install(local_segment_name, metadata_, nullptr).ok());
        ASSERT_TRUE(transport_.allocateSubBatch(batch_, 8).ok());
    }

    void TearDown() override {
        if (batch_) EXPECT_TRUE(transport_.freeSubBatch(batch_).ok());
        EXPECT_TRUE(transport_.uninstall().ok());
        unsetenv("MC_HYLINK_USE_VMM");
    }

    Request request(size_t index) {
        Request result{};
        result.opcode = Request::WRITE;
        result.source = sources_[index].data();
        result.target_id = LOCAL_SEGMENT_ID;
        result.target_offset =
            reinterpret_cast<uint64_t>(targets_[index].data());
        result.length = kLength;
        return result;
    }

    Status submit(std::vector<Request> requests) {
        return transport_.submitTransferTasks(batch_, requests);
    }

    HylinkSubBatch* batch() { return static_cast<HylinkSubBatch*>(batch_); }

    void expectStatus(int task_id, TransferStatusEnum expected) {
        TransferStatus status{};
        ASSERT_TRUE(transport_.getTransferStatus(batch_, task_id, status).ok());
        EXPECT_EQ(status.s, expected);
        EXPECT_EQ(status.transferred_bytes,
                  expected == COMPLETED ? kLength : 0);
    }

    std::array<std::array<char, kLength>, 2> sources_{};
    std::array<std::array<char, kLength>, 2> targets_{};
    std::shared_ptr<ControlService> metadata_;
    HylinkTransport transport_;
    Transport::SubBatchRef batch_ = nullptr;
};

TEST_F(HylinkTransportTest, InvalidRequestPreventsAllDispatch) {
    auto invalid = request(1);
    char unregistered[kLength]{};
    invalid.source = unregistered;

    auto status = submit({request(0), invalid});

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(batch()->task_list.empty());
    EXPECT_EQ(g_script.copy_calls, 0);
}

TEST_F(HylinkTransportTest, CopyFailurePreservesAcceptedPrefix) {
    g_script.fail_copy_call = 2;

    auto status = submit({request(0), request(1)});

    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(batch()->task_list.size(), 2);
    EXPECT_EQ(g_script.copy_calls, 2);
    EXPECT_EQ(g_script.accepted_copies, 1);
    EXPECT_NE(batch()->task_list[0].completion_event, nullptr);
    EXPECT_EQ(batch()->task_list[1].completion_event, nullptr);
    expectStatus(0, COMPLETED);
    expectStatus(1, FAILED);
    EXPECT_EQ(std::memcmp(targets_[0].data(), sources_[0].data(), kLength), 0);
}

TEST_F(HylinkTransportTest, EventCreateFailureDoesNotDispatchCurrentTask) {
    g_script.fail_event_create_call = 3;

    auto status = submit({request(0), request(1)});

    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(batch()->task_list.size(), 2);
    EXPECT_EQ(g_script.copy_calls, 1);
    EXPECT_EQ(g_script.accepted_copies, 1);
    expectStatus(0, COMPLETED);
    expectStatus(1, FAILED);
    EXPECT_EQ(std::memcmp(targets_[0].data(), sources_[0].data(), kLength), 0);
}

TEST_F(HylinkTransportTest, EventRecordFailureCompletesAcceptedCopy) {
    g_script.fail_task_event_record_call = 2;

    auto status = submit({request(0), request(1)});

    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(batch()->task_list.size(), 2);
    EXPECT_EQ(g_script.copy_calls, 2);
    EXPECT_EQ(g_script.accepted_copies, 2);
    EXPECT_EQ(g_script.stream_sync_calls, 1);
    EXPECT_EQ(batch()->task_list[1].completion_event, nullptr);
    expectStatus(0, COMPLETED);
    expectStatus(1, COMPLETED);
    for (size_t i = 0; i < targets_.size(); ++i) {
        EXPECT_EQ(std::memcmp(targets_[i].data(), sources_[i].data(), kLength),
                  0);
    }
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
