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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "tent/common/config.h"
#include "tent/transport/gds/gds_transport.h"

namespace {

std::vector<CUfileIOEvents_t> reported_events;
size_t cancel_call_count = 0;
size_t handle_register_call_count = 0;
size_t handle_deregister_call_count = 0;
size_t failed_handle_register_count = 0;
size_t batch_submit_call_count = 0;
size_t failed_batch_submit_count = 0;
size_t failed_status_query_count = 0;
size_t buf_register_call_count = 0;
size_t buf_deregister_call_count = 0;

}  // namespace

extern "C" CUfileError_t __wrap_cuFileDriverOpen() {
    CUfileError_t result{};
    result.err = CU_FILE_SUCCESS;
    return result;
}

extern "C" CUfileError_t __wrap_cuFileBatchIOGetStatus(CUfileBatchHandle_t,
                                                       unsigned,
                                                       unsigned* num_events,
                                                       CUfileIOEvents_t* events,
                                                       timespec*) {
    if (failed_status_query_count > 0) {
        --failed_status_query_count;
        CUfileError_t result{};
        result.err = CU_FILE_INTERNAL_ERROR;
        return result;
    }
    const auto count = std::min<size_t>(*num_events, reported_events.size());
    std::copy_n(reported_events.begin(), count, events);
    *num_events = static_cast<unsigned>(count);

    CUfileError_t result{};
    result.err = CU_FILE_SUCCESS;
    return result;
}

extern "C" CUfileError_t __wrap_cuFileBatchIOCancel(CUfileBatchHandle_t) {
    ++cancel_call_count;
    CUfileError_t result{};
    result.err = CU_FILE_SUCCESS;
    return result;
}

extern "C" CUfileError_t __wrap_cuFileBatchIOSubmit(CUfileBatchHandle_t,
                                                    unsigned, CUfileIOParams_t*,
                                                    unsigned) {
    ++batch_submit_call_count;
    CUfileError_t result{};
    if (failed_batch_submit_count > 0) {
        --failed_batch_submit_count;
        result.err = CU_FILE_INTERNAL_ERROR;
        return result;
    }
    result.err = CU_FILE_SUCCESS;
    return result;
}

extern "C" CUfileError_t __wrap_cuFileHandleRegister(CUfileHandle_t* handle,
                                                     CUfileDescr_t*) {
    ++handle_register_call_count;
    CUfileError_t result{};
    if (failed_handle_register_count > 0) {
        --failed_handle_register_count;
        *handle = reinterpret_cast<CUfileHandle_t>(1);
        result.err = CU_FILE_INVALID_VALUE;
        return result;
    }
    *handle = reinterpret_cast<CUfileHandle_t>(1);
    result.err = CU_FILE_SUCCESS;
    return result;
}

extern "C" CUfileError_t __wrap_cuFileHandleDeregister(CUfileHandle_t) {
    ++handle_deregister_call_count;
    CUfileError_t result{};
    result.err = CU_FILE_SUCCESS;
    return result;
}

extern "C" CUfileError_t __wrap_cuFileBufRegister(const void*, size_t, int) {
    ++buf_register_call_count;
    CUfileError_t result{};
    result.err = CU_FILE_SUCCESS;
    return result;
}

extern "C" CUfileError_t __wrap_cuFileBufDeregister(const void*) {
    ++buf_deregister_call_count;
    CUfileError_t result{};
    result.err = CU_FILE_SUCCESS;
    return result;
}

namespace mooncake {
namespace tent {

class GdsTransportTestPeer {
   public:
    static std::shared_ptr<GdsFileContext> getOrCreate(
        GdsTransport& transport, SegmentID target_id, const std::string& path) {
        return transport.getOrCreateFileContext(target_id, path);
    }

    static std::shared_ptr<GdsFileContext> find(GdsTransport& transport,
                                                SegmentID target_id) {
        return transport.findFileContext(target_id);
    }

    static TransferStatus aggregate(const std::vector<CUfileIOEvents_t>& events,
                                    bool& all_terminal) {
        return GdsTransport::aggregateTransferStatus(events, 0, events.size(),
                                                     all_terminal);
    }
};

namespace {

CUfileIOEvents_t makeEvent(CUfileStatus_t status, int64_t bytes = 0,
                           size_t slice_id = 0) {
    CUfileIOEvents_t event{};
    event.status = status;
    event.ret = bytes;
    event.cookie = reinterpret_cast<void*>(slice_id + 1);
    return event;
}

TEST(GdsTransportStatusTest, DetectsFailureWhileSiblingIsActive) {
    bool all_terminal = true;
    auto status = GdsTransportTestPeer::aggregate(
        {makeEvent(CUFILE_FAILED), makeEvent(CUFILE_WAITING)}, all_terminal);

    EXPECT_EQ(status.s, FAILED);
    EXPECT_FALSE(all_terminal);
}

TEST(GdsTransportStatusTest, ReportsPendingWhenNoFailureIsKnown) {
    bool all_terminal = true;
    auto status = GdsTransportTestPeer::aggregate(
        {makeEvent(CUFILE_COMPLETE, 1024), makeEvent(CUFILE_PENDING)},
        all_terminal);

    EXPECT_EQ(status.s, PENDING);
    EXPECT_EQ(status.transferred_bytes, 1024);
    EXPECT_FALSE(all_terminal);
}

TEST(GdsTransportStatusTest, PublicStatusReportsCompletedBytesWhilePending) {
    GdsTransport transport;
    GdsSubBatch batch;
    BatchHandle batch_handle{};
    batch.batch_handle = &batch_handle;
    batch.io_params.resize(2);
    batch.io_events.resize(2);
    batch.cached_events = {makeEvent(CUFILE_PENDING, 0, 0),
                           makeEvent(CUFILE_PENDING, 0, 1)};
    batch.io_param_ranges.push_back(IOParamRange{0, 2, 0, PENDING, PENDING});
    reported_events = {makeEvent(CUFILE_COMPLETE, 1024, 0),
                       makeEvent(CUFILE_PENDING, 0, 1)};

    TransferStatus status{INITIAL, 0};
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, PENDING);
    EXPECT_EQ(status.transferred_bytes, 1024);
    EXPECT_EQ(batch.io_param_ranges[0].transferred_bytes, 1024);
}

TEST(GdsTransportStatusTest, KeepsFailurePendingUntilEverySliceIsTerminal) {
    GdsTransport transport;
    GdsSubBatch batch;
    BatchHandle batch_handle{};
    batch.batch_handle = &batch_handle;
    batch.io_params.resize(2);
    batch.io_events.resize(2);
    batch.cached_events = {makeEvent(CUFILE_PENDING, 0, 0),
                           makeEvent(CUFILE_PENDING, 0, 1)};
    batch.io_param_ranges.push_back(IOParamRange{0, 2, 0, PENDING, PENDING});
    reported_events = {makeEvent(CUFILE_FAILED, 0, 0),
                       makeEvent(CUFILE_PENDING, 0, 1)};
    cancel_call_count = 0;

    TransferStatus status{INITIAL, 0};
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, PENDING);
    EXPECT_EQ(batch.io_param_ranges[0].known_failure, FAILED);
    EXPECT_FALSE(batch.reusable);
    EXPECT_EQ(cancel_call_count, 1);

    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, PENDING);
    EXPECT_EQ(cancel_call_count, 1);

    reported_events = {makeEvent(CUFILE_FAILED, 0, 0),
                       makeEvent(CUFILE_CANCELED, 0, 1)};
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, FAILED);
    EXPECT_TRUE(batch.reusable);
    EXPECT_EQ(cancel_call_count, 1);
}

TEST(GdsTransportStatusTest, CancellationDoesNotMaskOriginalFailure) {
    GdsTransport transport;
    GdsSubBatch batch;
    BatchHandle batch_handle{};
    batch.batch_handle = &batch_handle;
    batch.io_params.resize(2);
    batch.io_events.resize(2);
    batch.cached_events = {makeEvent(CUFILE_PENDING, 0, 0),
                           makeEvent(CUFILE_PENDING, 0, 1)};
    batch.io_param_ranges.push_back(IOParamRange{0, 2, 0, PENDING, PENDING});
    reported_events = {makeEvent(CUFILE_INVALID, 0, 0),
                       makeEvent(CUFILE_PENDING, 0, 1)};
    cancel_call_count = 0;

    TransferStatus status{INITIAL, 0};
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, PENDING);
    EXPECT_EQ(batch.io_param_ranges[0].known_failure, INVALID);
    bool drained = true;
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_FALSE(drained);

    reported_events = {makeEvent(CUFILE_INVALID, 0, 0),
                       makeEvent(CUFILE_CANCELED, 0, 1)};
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, INVALID);
    EXPECT_EQ(cancel_call_count, 1);
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_TRUE(drained);
}

TEST(GdsTransportStatusTest, QueryFailureDoesNotConfirmDrain) {
    GdsTransport transport;
    GdsSubBatch batch;
    BatchHandle batch_handle{};
    batch.batch_handle = &batch_handle;
    batch.io_params.resize(1);
    batch.io_events.resize(1);
    batch.cached_events = {makeEvent(CUFILE_PENDING)};
    batch.io_param_ranges.push_back(IOParamRange{0, 1, 0, PENDING, PENDING});
    failed_status_query_count = 1;

    bool drained = true;
    EXPECT_FALSE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_FALSE(drained);

    reported_events = {makeEvent(CUFILE_INVALID)};
    ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
    EXPECT_TRUE(drained);
}

TEST(GdsTransportStatusTest, ReusesHandleOnlyAfterWholeBatchIsTerminal) {
    GdsTransport transport;
    GdsSubBatch batch;
    BatchHandle batch_handle{};
    batch.batch_handle = &batch_handle;
    batch.io_params.resize(2);
    batch.io_events.resize(2);
    batch.cached_events = {makeEvent(CUFILE_PENDING, 0, 0),
                           makeEvent(CUFILE_PENDING, 0, 1)};
    batch.io_param_ranges = {IOParamRange{0, 1, 0, PENDING, PENDING},
                             IOParamRange{1, 1, 0, PENDING, PENDING}};
    reported_events = {makeEvent(CUFILE_FAILED, 0, 0),
                       makeEvent(CUFILE_PENDING, 0, 1)};
    cancel_call_count = 0;

    TransferStatus status{INITIAL, 0};
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, FAILED);
    EXPECT_FALSE(batch.reusable);
    EXPECT_EQ(cancel_call_count, 0);

    reported_events = {makeEvent(CUFILE_FAILED, 0, 0),
                       makeEvent(CUFILE_COMPLETE, 1024, 1)};
    ASSERT_TRUE(transport.getTransferStatus(&batch, 1, status).ok());
    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_TRUE(batch.reusable);
}

TEST(GdsTransportStatusTest, AggregatesCompletedBytes) {
    bool all_terminal = false;
    auto status = GdsTransportTestPeer::aggregate(
        {makeEvent(CUFILE_COMPLETE, 1024), makeEvent(CUFILE_COMPLETE, 2048)},
        all_terminal);

    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 3072);
    EXPECT_TRUE(all_terminal);
}

TEST(GdsTransportStatusTest, FailedFileOpenReturnsNoContext) {
    char path[] = "/tmp/tent-gds-open-failure-XXXXXX";
    int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(close(fd), 0);
    ASSERT_EQ(unlink(path), 0);

    GdsTransport transport;
    EXPECT_EQ(GdsTransportTestPeer::getOrCreate(transport, 100, path), nullptr);
}

TEST(GdsTransportStatusTest, FailedHandleRegistrationCanBeRetried) {
    char path[] = "/tmp/tent-gds-registration-retry-XXXXXX";
    int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(close(fd), 0);

    handle_register_call_count = 0;
    handle_deregister_call_count = 0;
    failed_handle_register_count = 1;
    {
        GdsTransport transport;
        auto failed = GdsTransportTestPeer::getOrCreate(transport, 101, path);
        EXPECT_EQ(failed, nullptr);
        EXPECT_EQ(handle_register_call_count, 1);
        EXPECT_EQ(handle_deregister_call_count, 0);

        auto recovered =
            GdsTransportTestPeer::getOrCreate(transport, 101, path);
        ASSERT_NE(recovered, nullptr);
        EXPECT_EQ(handle_register_call_count, 2);
        EXPECT_EQ(GdsTransportTestPeer::getOrCreate(transport, 101, path),
                  recovered);
        EXPECT_EQ(handle_register_call_count, 2);
    }
    EXPECT_EQ(handle_deregister_call_count, 1);
    EXPECT_EQ(unlink(path), 0);
}

TEST(GdsTransportStatusTest, KeepsFileContextAliveDuringUninstall) {
    char path[] = "/tmp/tent-gds-context-lifetime-XXXXXX";
    int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(close(fd), 0);

    handle_deregister_call_count = 0;
    GdsTransport transport;
    std::string local_segment_name = "gds-context-lifetime";
    ASSERT_TRUE(transport
                    .install(local_segment_name, nullptr, nullptr,
                             std::make_shared<Config>())
                    .ok());
    auto created = GdsTransportTestPeer::getOrCreate(transport, 102, path);
    ASSERT_NE(created, nullptr);
    auto found = GdsTransportTestPeer::find(transport, 102);
    ASSERT_NE(found, nullptr);

    ASSERT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(handle_deregister_call_count, 0);

    found.reset();
    EXPECT_EQ(handle_deregister_call_count, 0);
    created.reset();
    EXPECT_EQ(handle_deregister_call_count, 1);
    EXPECT_EQ(unlink(path), 0);
}

TEST(GdsTransportStatusTest, SubmittedBatchKeepsFileContextDuringUninstall) {
    char path[] = "/tmp/tent-gds-submitted-context-XXXXXX";
    int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(close(fd), 0);

    handle_deregister_call_count = 0;
    batch_submit_call_count = 0;
    GdsTransport transport;
    std::string local_segment_name = "gds-submitted-context";
    ASSERT_TRUE(transport
                    .install(local_segment_name, nullptr, nullptr,
                             std::make_shared<Config>())
                    .ok());
    auto context = GdsTransportTestPeer::getOrCreate(transport, 103, path);
    ASSERT_NE(context, nullptr);
    context.reset();

    {
        GdsSubBatch batch;
        BatchHandle batch_handle{};
        batch.batch_handle = &batch_handle;
        Request request{};
        request.opcode = Request::WRITE;
        request.target_id = 103;
        request.length = 1;
        ASSERT_TRUE(transport.submitTransferTasks(&batch, {request}).ok());
        EXPECT_EQ(batch_submit_call_count, 1);

        ASSERT_TRUE(transport.uninstall().ok());
        EXPECT_EQ(handle_deregister_call_count, 0);
    }

    EXPECT_EQ(handle_deregister_call_count, 1);
    EXPECT_EQ(unlink(path), 0);
}

TEST(GdsTransportStatusTest, FailurePrecedenceIsCompletionOrderIndependent) {
    bool all_terminal = false;
    auto first = GdsTransportTestPeer::aggregate(
        {makeEvent(CUFILE_CANCELED), makeEvent(CUFILE_FAILED)}, all_terminal);
    EXPECT_EQ(first.s, FAILED);
    EXPECT_TRUE(all_terminal);

    auto second = GdsTransportTestPeer::aggregate(
        {makeEvent(CUFILE_FAILED), makeEvent(CUFILE_CANCELED)}, all_terminal);
    EXPECT_EQ(second.s, FAILED);
    EXPECT_TRUE(all_terminal);
}

TEST(GdsTransportStatusTest, RejectedSubmissionPreservesOnlyAcceptedIOs) {
    char path[] = "/tmp/tent-gds-submit-rollback-XXXXXX";
    int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(close(fd), 0);

    GdsTransport transport;
    std::string local_segment_name = "gds-submit-rollback";
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(transport
                    .install(local_segment_name, metadata, nullptr,
                             std::make_shared<Config>())
                    .ok());
    ASSERT_NE(GdsTransportTestPeer::getOrCreate(transport, 104, path), nullptr);
    ASSERT_EQ(unlink(path), 0);
    Request request{};
    request.opcode = Request::WRITE;
    request.target_id = 104;
    // Two slices per task keep task and slice rollback offsets distinct.
    request.length = (16ull << 20) + 1;

    auto check_submission = [&](bool accepted_prefix, bool native_rejection) {
        SCOPED_TRACE(::testing::Message()
                     << "accepted_prefix=" << accepted_prefix
                     << ", native_rejection=" << native_rejection);
        GdsSubBatch batch;
        BatchHandle batch_handle{};
        batch.batch_handle = &batch_handle;
        batch.io_params.reserve(8);
        batch.cached_events.reserve(8);
        batch.io_events.resize(8);
        if (accepted_prefix)
            ASSERT_TRUE(transport.submitTransferTasks(&batch, {request}).ok());
        const size_t prefix_size = batch.size();
        const size_t prefix_params = batch.io_params.size();
        const size_t submits = batch_submit_call_count;
        auto second = request;
        if (native_rejection)
            failed_batch_submit_count = 1;
        else
            second.target_id = 105;

        EXPECT_FALSE(
            transport.submitTransferTasks(&batch, {request, second}).ok());
        EXPECT_EQ(batch_submit_call_count, submits + native_rejection);
        ASSERT_EQ(batch.size(), prefix_size);
        ASSERT_EQ(batch.io_params.size(), prefix_params);
        ASSERT_EQ(batch.cached_events.size(), prefix_params);
        ASSERT_EQ(batch.file_contexts.size(), prefix_size);
        reported_events.clear();
        bool drained = true;
        ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
        EXPECT_EQ(drained, !accepted_prefix);

        ASSERT_TRUE(
            transport.submitTransferTasks(&batch, {request, request}).ok());
        ASSERT_EQ(batch.size(), prefix_size + 2);
        for (size_t i = 0; i < batch.io_params.size(); ++i) {
            EXPECT_EQ(batch.io_params[i].cookie,
                      reinterpret_cast<void*>(i + 1));
            reported_events.push_back(
                makeEvent(CUFILE_COMPLETE, batch.io_params[i].u.batch.size, i));
        }
        if (accepted_prefix) {
            reported_events[0] = makeEvent(CUFILE_PENDING);
            ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
            EXPECT_FALSE(drained);
            reported_events[0] =
                makeEvent(CUFILE_COMPLETE, batch.io_params[0].u.batch.size);
        }
        ASSERT_TRUE(transport.isSubBatchDrained(&batch, drained).ok());
        EXPECT_TRUE(drained);
    };
    for (bool accepted_prefix : {false, true})
        for (bool native_rejection : {false, true})
            check_submission(accepted_prefix, native_rejection);
}

#ifdef USE_XPU_GDS
constexpr const char* kGdsDeviceLocation = "xpu:0";
constexpr const char* kOtherDeviceLocation = "cuda:0";
#else
constexpr const char* kGdsDeviceLocation = "cuda:0";
constexpr const char* kOtherDeviceLocation = "xpu:0";
#endif

TEST(GdsTransportStatusTest, RegistersOnlyGdsDeviceBuffers) {
    GdsTransport transport;
    buf_register_call_count = 0;
    buf_deregister_call_count = 0;

    for (const char* location : {"cpu:0", kOtherDeviceLocation}) {
        BufferDesc desc{};
        desc.addr = 0x1000;
        desc.length = 4096;
        desc.location = location;
        MemoryOptions options;
        options.location = location;
        ASSERT_TRUE(transport.addMemoryBuffer(desc, options).ok()) << location;
        EXPECT_TRUE(desc.transports.empty()) << location;
        ASSERT_TRUE(transport.removeMemoryBuffer(desc).ok()) << location;
    }
    EXPECT_EQ(buf_register_call_count, 0);
    EXPECT_EQ(buf_deregister_call_count, 0);

    BufferDesc desc{};
    desc.addr = 0x1000;
    desc.length = 4096;
    desc.location = kGdsDeviceLocation;
    MemoryOptions options;
    options.location = kGdsDeviceLocation;
    ASSERT_TRUE(transport.addMemoryBuffer(desc, options).ok());
    EXPECT_EQ(buf_register_call_count, 1);
    ASSERT_EQ(desc.transports.size(), 1u);
    EXPECT_EQ(desc.transports.front(), GDS);
    ASSERT_TRUE(transport.removeMemoryBuffer(desc).ok());
    EXPECT_EQ(buf_deregister_call_count, 1);
}

TEST(GdsTransportStatusTest, FileCapabilitiesMatchCuFileLibrary) {
    GdsTransport transport;
    std::string local_segment_name = "gds-capabilities";
    ASSERT_TRUE(transport
                    .install(local_segment_name, nullptr, nullptr,
                             std::make_shared<Config>())
                    .ok());
    EXPECT_TRUE(transport.capabilities().gpu_to_file);
#ifdef USE_XPU_GDS
    EXPECT_FALSE(transport.capabilities().dram_to_file);
#else
    EXPECT_TRUE(transport.capabilities().dram_to_file);
#endif
    ASSERT_TRUE(transport.uninstall().ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
