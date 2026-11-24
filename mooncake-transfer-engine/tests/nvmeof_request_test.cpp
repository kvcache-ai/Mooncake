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
#include <unistd.h>

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <vector>

#include "transport/nvmeof_transport/nvmeof_transport.h"

// These executable-local cuFile fakes exercise the real transport and
// descriptor pool without opening a GPU driver or issuing storage I/O. Each
// batch owns its completion queue, and completed events are consumed only once.
namespace {
struct FakeBatch {
    std::vector<CUfileIOEvents_t> events;
};
std::vector<CUfileIOParams_t> submitted;
size_t submit_calls = 0;
size_t handle_registrations = 0;
CUfileError_t Success() { return {}; }
}  // namespace

extern "C" {
CUfileError_t cuFileDriverOpen() { return Success(); }
CUfileError_t cuFileBufRegister(const void*, size_t, int) { return Success(); }
CUfileError_t cuFileBufDeregister(const void*) { return Success(); }
CUfileError_t cuFileHandleRegister(CUfileHandle_t* handle, CUfileDescr_t*) {
    ++handle_registrations;
    *handle = reinterpret_cast<CUfileHandle_t>(handle_registrations);
    return Success();
}
void cuFileHandleDeregister(CUfileHandle_t) {}
CUfileError_t cuFileBatchIOSetUp(CUfileBatchHandle_t* handle, unsigned) {
    *handle = reinterpret_cast<CUfileBatchHandle_t>(new FakeBatch);
    return Success();
}
CUfileError_t cuFileBatchIOSubmit(CUfileBatchHandle_t handle, unsigned count,
                                  CUfileIOParams_t* params, unsigned) {
    ++submit_calls;
    auto* batch = reinterpret_cast<FakeBatch*>(handle);
    for (unsigned i = 0; i < count; ++i) {
        submitted.push_back(params[i]);
        CUfileIOEvents_t event = {};
        event.cookie = params[i].cookie;
        event.status = CUFILE_COMPLETE;
        event.ret = params[i].u.batch.size;
        batch->events.push_back(event);
    }
    return Success();
}
CUfileError_t cuFileBatchIOGetStatus(CUfileBatchHandle_t handle, unsigned,
                                     unsigned* count, CUfileIOEvents_t* events,
                                     timespec*) {
    auto* batch = reinterpret_cast<FakeBatch*>(handle);
    *count = std::min<size_t>(*count, batch->events.size());
    std::copy_n(batch->events.begin(), *count, events);
    batch->events.erase(batch->events.begin(), batch->events.begin() + *count);
    return Success();
}
CUfileError_t cuFileBatchIOCancel(CUfileBatchHandle_t) { return Success(); }
void cuFileBatchIODestroy(CUfileBatchHandle_t handle) {
    delete reinterpret_cast<FakeBatch*>(handle);
}
}  // extern "C"

namespace mooncake {
class NVMeoFTransportTestPeer {
   public:
    static std::unique_ptr<NVMeoFTransport> create(
        const std::shared_ptr<TransferMetadata>& metadata) {
        auto transport = std::unique_ptr<NVMeoFTransport>(
            new NVMeoFTransport(std::make_shared<CUFileDescPool>()));
        std::string name = "nvmeof-request-test";
        transport->install(name, metadata, nullptr);
        return transport;
    }

    static size_t sliceCount(NVMeoFTransport& transport,
                             Transport::BatchID batch_id) {
        auto* batch = reinterpret_cast<Transport::BatchDesc*>(batch_id);
        auto* context = static_cast<NVMeoFBatchDesc*>(batch->context);
        return transport.desc_pool_->getSliceNum(context->desc_idx_);
    }
};

// Only LOCAL_SEGMENT_ID is used, so no metadata backend or daemon is needed.
class LocalMetadata : public TransferMetadata {
   public:
    LocalMetadata()
        : TransferMetadata(std::shared_ptr<MetadataStoragePlugin>{}) {}
};

class NVMeoFRequestTest : public ::testing::Test {
   protected:
    void SetUp() override {
        submitted.clear();
        submit_calls = 0;
        handle_registrations = 0;
        char path[] = "/tmp/mooncake-nvmeof-request-XXXXXX";
        int fd = mkstemp(path);
        ASSERT_GE(fd, 0);
        close(fd);
        path_ = path;
        metadata_ = std::make_shared<LocalMetadata>();
        SetSegment({4096, 4096});
        transport_ = NVMeoFTransportTestPeer::create(metadata_);
        batch_id_ = transport_->allocateBatchID(4);
    }

    void TearDown() override {
        // Drain even unexpectedly accepted requests on the unfixed code so a
        // failed regression assertion cannot leak a batch into another test.
        for (size_t i = 0; i < Batch().task_list.size(); ++i) {
            Transport::TransferStatus status;
            EXPECT_TRUE(
                transport_->getTransferStatus(batch_id_, i, status).ok());
        }
        EXPECT_TRUE(transport_->freeBatchID(batch_id_).ok());
        transport_.reset();
        unlink(path_.c_str());
    }

    void SetSegment(std::initializer_list<uint64_t> lengths,
                    const std::string& protocol = "nvmeof") {
        auto segment = std::make_shared<TransferMetadata::SegmentDesc>();
        segment->name = "nvmeof-request-test";
        segment->protocol = protocol;
        for (auto length : lengths) {
            TransferMetadata::NVMeoFBufferDesc buffer;
            buffer.length = length;
            buffer.local_path_map["nvmeof-request-test"] = path_;
            segment->nvmeof_buffers.push_back(std::move(buffer));
        }
        ASSERT_EQ(metadata_->addLocalSegment(LOCAL_SEGMENT_ID, segment->name,
                                             std::move(segment)),
                  0);
    }

    Transport::TransferRequest Request(uint64_t offset, uint64_t length) {
        Transport::TransferRequest request = {};
        request.opcode = Transport::TransferRequest::WRITE;
        request.source = data_.data();
        request.target_id = LOCAL_SEGMENT_ID;
        request.target_offset = offset;
        request.length = length;
        return request;
    }

    Transport::BatchDesc& Batch() {
        return *reinterpret_cast<Transport::BatchDesc*>(batch_id_);
    }

    void ExpectRejected(
        const std::vector<Transport::TransferRequest>& entries) {
        const auto tasks = Batch().task_list.size();
        const auto slices =
            NVMeoFTransportTestPeer::sliceCount(*transport_, batch_id_);
        auto* context = static_cast<NVMeoFBatchDesc*>(Batch().context);
        const auto mappings = context->task_to_slices.size();
        const auto statuses = context->transfer_status.size();
        const auto calls = submit_calls;
        const auto handles = handle_registrations;
        auto status = transport_->submitTransfer(batch_id_, entries);
        EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
        EXPECT_EQ(Batch().task_list.size(), tasks);
        EXPECT_EQ(NVMeoFTransportTestPeer::sliceCount(*transport_, batch_id_),
                  slices);
        EXPECT_EQ(context->task_to_slices.size(), mappings);
        EXPECT_EQ(context->transfer_status.size(), statuses);
        EXPECT_EQ(submit_calls, calls);
        EXPECT_EQ(handle_registrations, handles);
    }

    std::string path_;
    std::shared_ptr<LocalMetadata> metadata_;
    std::unique_ptr<NVMeoFTransport> transport_;
    Transport::BatchID batch_id_;
    std::array<char, 16384> data_{};
};

TEST_F(NVMeoFRequestTest, SingleBuffer) {
    ASSERT_TRUE(
        transport_->submitTransfer(batch_id_, {Request(1024, 2048)}).ok());
    ASSERT_EQ(submitted.size(), 1);
    EXPECT_EQ(submitted[0].u.batch.file_offset, 1024);
    EXPECT_EQ(submitted[0].u.batch.size, 2048);
}

TEST_F(NVMeoFRequestTest, CrossBufferEndingAtSegmentBoundary) {
    ASSERT_TRUE(
        transport_->submitTransfer(batch_id_, {Request(2048, 6144)}).ok());
    ASSERT_EQ(submitted.size(), 2);
    EXPECT_EQ(submitted[0].u.batch.file_offset, 2048);
    EXPECT_EQ(submitted[0].u.batch.size, 2048);
    EXPECT_EQ(submitted[1].u.batch.file_offset, 0);
    EXPECT_EQ(submitted[1].u.batch.size, 4096);
    EXPECT_EQ(submitted[1].u.batch.devPtr_base, data_.data() + 2048);
    Transport::TransferStatus status;
    ASSERT_TRUE(transport_->getTransferStatus(batch_id_, 0, status).ok());
    EXPECT_EQ(status.s, Transport::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 6144);
}

TEST_F(NVMeoFRequestTest, RejectsPartialCoverage) {
    ExpectRejected({Request(4096, 8192)});
    if (!Batch().task_list.empty()) {
        Transport::TransferStatus status;
        ASSERT_TRUE(transport_->getTransferStatus(batch_id_, 0, status).ok());
        ADD_FAILURE() << "Out-of-range request was accepted: status="
                      << status.s
                      << ", transferred_bytes=" << status.transferred_bytes
                      << ", requested_bytes=8192";
    }
}
TEST_F(NVMeoFRequestTest, RejectsOneBytePastEnd) {
    ExpectRejected({Request(4096, 4097)});
}
TEST_F(NVMeoFRequestTest, RejectsStartAtEnd) {
    ExpectRejected({Request(8192, 1)});
}
TEST_F(NVMeoFRequestTest, RejectsStartPastEnd) {
    ExpectRejected({Request(8193, 1)});
}
TEST_F(NVMeoFRequestTest, RejectsRequestEndOverflow) {
    ExpectRejected({Request(1, std::numeric_limits<uint64_t>::max())});
}
TEST_F(NVMeoFRequestTest, RejectsOffsetNearUint64Max) {
    ExpectRejected({Request(std::numeric_limits<uint64_t>::max() - 1, 4096)});
}
TEST_F(NVMeoFRequestTest, RejectsSegmentLengthOverflow) {
    SetSegment({std::numeric_limits<uint64_t>::max(), 1});
    ExpectRejected({Request(0, 1)});
}
TEST_F(NVMeoFRequestTest, RejectsZeroLength) {
    ExpectRejected({Request(4096, 0)});
}
TEST_F(NVMeoFRequestTest, RejectsEmptySegment) {
    SetSegment({});
    ExpectRejected({Request(0, 1)});
}
TEST_F(NVMeoFRequestTest, SkipsEmptyBuffersOnRead) {
    SetSegment({0, 4096, 0, 4096, 0});
    auto request = Request(2048, 6144);
    request.opcode = Transport::TransferRequest::READ;
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {request}).ok());
    ASSERT_EQ(submitted.size(), 2);
    EXPECT_EQ(submitted[0].opcode, CUFILE_READ);
    EXPECT_EQ(submitted[1].opcode, CUFILE_READ);
    EXPECT_EQ(submitted[0].u.batch.size, 2048);
    EXPECT_EQ(submitted[1].u.batch.size, 4096);
}
TEST_F(NVMeoFRequestTest, RejectsMissingSegment) {
    ASSERT_EQ(metadata_->removeLocalSegment("nvmeof-request-test"), 0);
    ExpectRejected({Request(0, 4096)});
}
TEST_F(NVMeoFRequestTest, RejectsWrongProtocol) {
    SetSegment({8192}, "rdma");
    ExpectRejected({Request(0, 4096)});
}
TEST_F(NVMeoFRequestTest, RejectsWholeSubmissionAndAllowsRetry) {
    ExpectRejected({Request(0, 4096), Request(4096, 8192)});
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {Request(0, 4096)}).ok());
    EXPECT_EQ(Batch().task_list.size(), 1);
    EXPECT_EQ(submit_calls, 1);
}
TEST_F(NVMeoFRequestTest, PreservesExistingTaskOnRejection) {
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {Request(0, 4096)}).ok());
    ExpectRejected({Request(0, 4096), Request(8192, 1)});
    Transport::TransferStatus status;
    ASSERT_TRUE(transport_->getTransferStatus(batch_id_, 0, status).ok());
    EXPECT_EQ(status.s, Transport::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 4096);
}

TEST_F(NVMeoFRequestTest, RejectsMissingPathWithoutChangingMetadata) {
    auto segment = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    auto& paths = segment->nvmeof_buffers[1].local_path_map;
    paths.clear();
    ExpectRejected({Request(0, 4096), Request(4096, 4096)});
    EXPECT_TRUE(paths.empty());
    paths["nvmeof-request-test"] = path_;
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {Request(0, 8192)}).ok());
}
TEST_F(NVMeoFRequestTest, RejectsEmptyPathPreservingExistingTask) {
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {Request(0, 4096)}).ok());
    auto segment = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    segment->nvmeof_buffers[1].local_path_map["nvmeof-request-test"] = "";
    ExpectRejected({Request(4096, 4096)});
    Transport::TransferStatus status;
    ASSERT_TRUE(transport_->getTransferStatus(batch_id_, 0, status).ok());
    EXPECT_EQ(status.s, Transport::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 4096);
}
TEST_F(NVMeoFRequestTest, IgnoresMissingPathInUntouchedBuffer) {
    auto segment = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    segment->nvmeof_buffers[1].local_path_map.clear();
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {Request(0, 4096)}).ok());
}
TEST_F(NVMeoFRequestTest, AcceptsExactSliceCapacity) {
    auto segment = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    auto buffer = segment->nvmeof_buffers.front();
    buffer.length = 1;
    segment->nvmeof_buffers.assign(128, buffer);
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {Request(0, 128)}).ok());
    EXPECT_EQ(submitted.size(), 128);
}
TEST_F(NVMeoFRequestTest, RejectsSliceOverflowAndAllowsRetry) {
    auto segment = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    auto buffer = segment->nvmeof_buffers.front();
    buffer.length = 1;
    segment->nvmeof_buffers.assign(129, buffer);
    ExpectRejected({Request(0, 129)});
    ExpectRejected({Request(0, 64), Request(64, 65)});
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {Request(0, 128)}).ok());
    EXPECT_EQ(submitted.size(), 128);
}
TEST_F(NVMeoFRequestTest, RejectsSliceOverflowAfterAcceptedSubmission) {
    auto segment = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    auto buffer = segment->nvmeof_buffers.front();
    buffer.length = 1;
    segment->nvmeof_buffers.assign(129, buffer);
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {Request(0, 127)}).ok());
    ExpectRejected({Request(127, 2)});
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {Request(127, 1)}).ok());
    EXPECT_EQ(NVMeoFTransportTestPeer::sliceCount(*transport_, batch_id_), 128);
}
TEST_F(NVMeoFRequestTest, AcceptedSubmissionsDoNotReplayPendingSlices) {
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {Request(0, 4096)}).ok());
    ASSERT_TRUE(
        transport_->submitTransfer(batch_id_, {Request(4096, 4096)}).ok());
    ASSERT_EQ(submitted.size(), 2);
    EXPECT_NE(submitted[0].cookie, submitted[1].cookie);
    EXPECT_EQ(submit_calls, 2);
    for (size_t i = 0; i < 2; ++i) {
        Transport::TransferStatus status;
        ASSERT_TRUE(transport_->getTransferStatus(batch_id_, i, status).ok());
        EXPECT_EQ(status.s, Transport::COMPLETED);
        EXPECT_EQ(status.transferred_bytes, 4096);
    }
}
TEST_F(NVMeoFRequestTest, AcceptedSubmissionsDoNotReplayCompletedSlices) {
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {Request(0, 4096)}).ok());
    Transport::TransferStatus status;
    ASSERT_TRUE(transport_->getTransferStatus(batch_id_, 0, status).ok());
    ASSERT_EQ(status.s, Transport::COMPLETED);
    ASSERT_TRUE(
        transport_->submitTransfer(batch_id_, {Request(4096, 4096)}).ok());
    ASSERT_EQ(submitted.size(), 2);
    EXPECT_NE(submitted[0].cookie, submitted[1].cookie);
    ASSERT_TRUE(transport_->getTransferStatus(batch_id_, 1, status).ok());
    EXPECT_EQ(status.s, Transport::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 4096);
    ASSERT_TRUE(transport_->submitTransfer(batch_id_, {}).ok());
    EXPECT_EQ(submit_calls, 2);
}
}  // namespace mooncake
