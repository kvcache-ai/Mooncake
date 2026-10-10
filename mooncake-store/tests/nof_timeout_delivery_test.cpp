// The SPDK entry points below are a sequential TEST DOUBLE, not a real-SPDK
// E2E transport. GNU ld --wrap keeps the actual SpdkWrapper implementation,
// including its registered timeout callback and polling/abort paths, in use.
// These tests establish timeout delivery; they do not establish cross-pool
// qpair serialization or ownership of another worker's counters/free list.
#include "spdk/spdk_wrapper.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <cstdlib>
#include <optional>
#include <string>

namespace {

constexpr char kEndpoint[] =
    "trtype:TCP adrfam:IPv4 traddr:127.0.0.1 trsvcid:4420 "
    "subnqn:nqn.2026-10.io.mooncake:timeout-test-double ns:1";
constexpr char kTimeoutEnv[] = "MC_NOF_IO_TIMEOUT_MS";
constexpr uint64_t kDefaultTimeoutUs = 30 * 1000 * 1000;

// Opaque identities only: no real SPDK function may dereference these anchors.
template <typename T>
T* OpaqueIdentity() {
    static char anchor;
    return reinterpret_cast<T*>(&anchor);
}

struct TransportDouble {
    spdk_nvme_timeout_cb timeout_callback = nullptr;
    void* timeout_arg = nullptr;
    uint64_t timeout_us = 0;
    spdk_nvme_cmd_cb completion_callback = nullptr;
    void* completion_arg = nullptr;
    bool outstanding = false;
    bool inject_timeout = false;
    bool timeout_reported = false;
    int submissions = 0;
    int timeout_callbacks = 0;
    int disconnects = 0;
} transport;

int AcceptRequest(spdk_nvme_ns* ns, spdk_nvme_qpair* qpair,
                  spdk_nvme_cmd_cb callback, void* arg) {
    EXPECT_EQ(ns, OpaqueIdentity<spdk_nvme_ns>());
    EXPECT_EQ(qpair, OpaqueIdentity<spdk_nvme_qpair>());
    EXPECT_FALSE(transport.outstanding);
    transport.completion_callback = callback;
    transport.completion_arg = arg;
    transport.outstanding = true;
    ++transport.submissions;
    return 0;
}

struct CompletionState {
    int calls = 0;
    bool aborted = false;
};

void RecordCompletion(void* arg, const spdk_nvme_cpl* cpl) {
    auto* state = static_cast<CompletionState*>(arg);
    ++state->calls;
    state->aborted = cpl->status.sct == SPDK_NVME_SCT_GENERIC &&
                     cpl->status.sc == SPDK_NVME_SC_ABORTED_SQ_DELETION;
}

// Re-exec configuration checks so each child first initializes the cached
// setting with the requested value. A fast/fork child would inherit the
// parent's initialized setting after the delivery tests in a repeated run.
TEST(NofTimeoutConfigurationDeathTest, RejectsNegativeValue) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        {
            setenv(kTimeoutEnv, "-1", 1);
            auto* seg =
                mooncake::SpdkWrapper::GetInstance().OpenNofSegment(kEndpoint);
            std::exit(seg != nullptr &&
                              transport.timeout_us == kDefaultTimeoutUs
                          ? 0
                          : 1);
        },
        ::testing::ExitedWithCode(0), "Invalid value for MC_NOF_IO_TIMEOUT_MS");
}

TEST(NofTimeoutConfigurationDeathTest, RejectsWhitespacePrefixedNegativeValue) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        {
            setenv(kTimeoutEnv, " \t-1", 1);
            auto* seg =
                mooncake::SpdkWrapper::GetInstance().OpenNofSegment(kEndpoint);
            std::exit(seg != nullptr &&
                              transport.timeout_us == kDefaultTimeoutUs
                          ? 0
                          : 1);
        },
        ::testing::ExitedWithCode(0), "Invalid value for MC_NOF_IO_TIMEOUT_MS");
}

class NofTimeoutDeliveryTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (const char* value = std::getenv(kTimeoutEnv)) saved_env_ = value;
        unsetenv(kTimeoutEnv);
        wrapper_.Cleanup();
        transport = {};
        seg_ = wrapper_.OpenNofSegment(kEndpoint);
        ASSERT_NE(seg_, nullptr);
        ASSERT_NE(transport.timeout_callback, nullptr);
        ASSERT_EQ(wrapper_.SubmitRequest(seg_, buffer_, 0, 1,
                                         mooncake::kSpdkNofOpWrite,
                                         RecordCompletion, &completion_),
                  0);
        transport.inject_timeout = true;
    }

    void TearDown() override {
        // Retain the callback context and caller buffer until local abort has
        // delivered the outstanding request's completion, including on failure.
        if (seg_ && transport.outstanding) wrapper_.AbortNofSegmentIo(seg_);
        wrapper_.Cleanup();
        if (saved_env_) {
            setenv(kTimeoutEnv, saved_env_->c_str(), 1);
        } else {
            unsetenv(kTimeoutEnv);
        }
    }

    void ExpectOneOutstandingTimeout() {
        EXPECT_EQ(transport.submissions, 1);
        EXPECT_EQ(transport.timeout_callbacks, 1);
        EXPECT_TRUE(transport.outstanding);
        EXPECT_EQ(transport.completion_arg, &completion_);
        EXPECT_EQ(completion_.calls, 0);
    }

    mooncake::SpdkWrapper& wrapper_ = mooncake::SpdkWrapper::GetInstance();
    mooncake::nof_seg_handle* seg_ = nullptr;
    alignas(4096) char buffer_[4096] = {};
    CompletionState completion_;
    std::optional<std::string> saved_env_;
};

TEST_F(NofTimeoutDeliveryTest, OwnerReceivesAndAcknowledgesTimeoutInSamePoll) {
    bool timed_out = false;
    EXPECT_EQ(wrapper_.NvmePollProcessCompletion(seg_, 0, &timed_out), 0);
    EXPECT_TRUE(timed_out);
    ExpectOneOutstandingTimeout();
    // A non-null output is the designated owner's delivery/acknowledgement.
    // No SPDK callback repeats for this outstanding request.
    EXPECT_EQ(wrapper_.NvmePollProcessCompletion(seg_, 0, &timed_out), 0);
    EXPECT_FALSE(timed_out);
    ExpectOneOutstandingTimeout();
}

TEST_F(NofTimeoutDeliveryTest, OmittedOutputLeavesTimeoutForLaterOwnerPoll) {
    EXPECT_EQ(wrapper_.NvmePollProcessCompletion(seg_, 0), 0);
    ExpectOneOutstandingTimeout();
    bool timed_out = false;
    EXPECT_EQ(wrapper_.NvmePollProcessCompletion(seg_, 0, &timed_out), 0);
    EXPECT_TRUE(timed_out);
    ExpectOneOutstandingTimeout();
    EXPECT_EQ(wrapper_.NvmePollProcessCompletion(seg_, 0, &timed_out), 0);
    EXPECT_FALSE(timed_out);
}

TEST_F(NofTimeoutDeliveryTest, PendingTimeoutSurvivesInterveningNullPoll) {
    EXPECT_EQ(wrapper_.NvmePollProcessCompletion(seg_, 0, nullptr), 0);
    EXPECT_EQ(wrapper_.NvmePollProcessCompletion(seg_, 0, nullptr), 0);
    ExpectOneOutstandingTimeout();
    bool timed_out = false;
    EXPECT_EQ(wrapper_.NvmePollProcessCompletion(seg_, 0, &timed_out), 0);
    EXPECT_TRUE(timed_out);
    ExpectOneOutstandingTimeout();
}

TEST_F(NofTimeoutDeliveryTest, OwnerAbortRetainsContextUntilCompletion) {
    bool timed_out = false;
    EXPECT_EQ(wrapper_.NvmePollProcessCompletion(seg_, 0, &timed_out), 0);
    ASSERT_TRUE(timed_out);
    ExpectOneOutstandingTimeout();
    wrapper_.AbortNofSegmentIo(seg_);
    EXPECT_EQ(transport.disconnects, 1);
    EXPECT_FALSE(transport.outstanding);
    EXPECT_EQ(completion_.calls, 1);
    EXPECT_TRUE(completion_.aborted);
    EXPECT_EQ(
        wrapper_.SubmitRequest(seg_, buffer_, 0, 1, mooncake::kSpdkNofOpWrite,
                               RecordCompletion, &completion_),
        -ENXIO);
    EXPECT_EQ(transport.submissions, 1);
    EXPECT_EQ(completion_.calls, 1);
}

}  // namespace

extern "C" {

int __wrap_spdk_env_init(const spdk_env_opts*) { return 0; }
void __wrap_spdk_env_fini() {}

int __wrap_spdk_nvme_probe(const spdk_nvme_transport_id* trid, void* context,
                           spdk_nvme_probe_cb probe, spdk_nvme_attach_cb attach,
                           spdk_nvme_remove_cb) {
    spdk_nvme_ctrlr_opts opts = {};
    if (!probe(context, trid, &opts)) return -EINVAL;
    attach(context, trid, OpaqueIdentity<spdk_nvme_ctrlr>(), &opts);
    return 0;
}

void __wrap_spdk_nvme_ctrlr_register_timeout_callback(
    spdk_nvme_ctrlr* ctrlr, uint64_t io_us, uint64_t,
    spdk_nvme_timeout_cb callback, void* arg) {
    EXPECT_EQ(ctrlr, OpaqueIdentity<spdk_nvme_ctrlr>());
    transport.timeout_us = io_us;
    transport.timeout_callback = callback;
    transport.timeout_arg = arg;
}

bool __wrap_spdk_nvme_ctrlr_is_active_ns(spdk_nvme_ctrlr*, uint32_t nsid) {
    return nsid == 1;
}
spdk_nvme_ns* __wrap_spdk_nvme_ctrlr_get_ns(spdk_nvme_ctrlr*, uint32_t) {
    return OpaqueIdentity<spdk_nvme_ns>();
}
spdk_nvme_qpair* __wrap_spdk_nvme_ctrlr_alloc_io_qpair(
    spdk_nvme_ctrlr*, const spdk_nvme_io_qpair_opts*, size_t) {
    return OpaqueIdentity<spdk_nvme_qpair>();
}
int __wrap_spdk_nvme_ctrlr_free_io_qpair(spdk_nvme_qpair*) {
    EXPECT_FALSE(transport.outstanding);
    return 0;
}
int __wrap_spdk_nvme_detach(spdk_nvme_ctrlr*) { return 0; }

int __wrap_spdk_nvme_ns_cmd_write(spdk_nvme_ns* ns, spdk_nvme_qpair* qpair,
                                  void*, uint64_t, uint32_t,
                                  spdk_nvme_cmd_cb callback, void* arg,
                                  uint32_t) {
    return AcceptRequest(ns, qpair, callback, arg);
}
int __wrap_spdk_nvme_ns_cmd_read(spdk_nvme_ns* ns, spdk_nvme_qpair* qpair,
                                 void*, uint64_t, uint32_t,
                                 spdk_nvme_cmd_cb callback, void* arg,
                                 uint32_t) {
    return AcceptRequest(ns, qpair, callback, arg);
}

int32_t __wrap_spdk_nvme_qpair_process_completions(spdk_nvme_qpair* qpair,
                                                   uint32_t) {
    EXPECT_EQ(qpair, OpaqueIdentity<spdk_nvme_qpair>());
    if (transport.inject_timeout && transport.outstanding &&
        !transport.timeout_reported) {
        transport.timeout_reported = true;
        ++transport.timeout_callbacks;
        transport.timeout_callback(transport.timeout_arg,
                                   OpaqueIdentity<spdk_nvme_ctrlr>(), qpair, 1);
    }
    return 0;
}

void __wrap_spdk_nvme_ctrlr_disconnect_io_qpair(spdk_nvme_qpair* qpair) {
    EXPECT_EQ(qpair, OpaqueIdentity<spdk_nvme_qpair>());
    ++transport.disconnects;
    if (transport.outstanding) {
        transport.outstanding = false;
        spdk_nvme_cpl completion = {};
        completion.status.sct = SPDK_NVME_SCT_GENERIC;
        completion.status.sc = SPDK_NVME_SC_ABORTED_SQ_DELETION;
        transport.completion_callback(transport.completion_arg, &completion);
    }
}

}  // extern "C"
