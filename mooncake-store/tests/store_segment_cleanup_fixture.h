#pragma once

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <map>
#include <mutex>
#include <optional>

#include "common/client_buffer_allocation.h"
#include "real_client.h"
#include "test_server_helpers.h"
#include "transfer_metadata_plugin.h"

// GNU/Linux LP64, libstdc++'s C++11 string ABI. Keep these existing boundaries
// explicit: --wrap must intercept archive references, not replace Store code.
#define CLEANUP_TE "_ZN8mooncake14TransferEngine21unregisterLocalMemoryEPvb"
#define CLEANUP_MASTER \
    "_ZN8mooncake12MasterClient14UnmountSegmentERKSt4pairImmE"
#define CLEANUP_PLUGIN                                                         \
    "_ZN8mooncake21MetadataStoragePlugin6CreateERKNSt7__cxx1112basic_stringIc" \
    "St11char_traitsIcESaIcEEE"
#define CLEANUP_FREE                                                           \
    "_ZN8mooncake11free_memoryERKNSt7__cxx1112basic_stringIcSt11char_traitsIc" \
    "ESaIcEEEPvPNS_18DmaBufferAllocatorE"

extern "C" int RealUnregister(mooncake::TransferEngine*, void*,
                              bool) asm("__real_" CLEANUP_TE);
extern "C" tl::expected<void, mooncake::ErrorCode> RealUnmount(
    mooncake::MasterClient*,
    const mooncake::UUID&) asm("__real_" CLEANUP_MASTER);
extern "C" std::shared_ptr<mooncake::MetadataStoragePlugin> RealPlugin(
    const std::string&) asm("__real_" CLEANUP_PLUGIN);
extern "C" void RealFree(
    const std::string&, void*,
    mooncake::DmaBufferAllocator*) asm("__real_" CLEANUP_FREE);
extern "C" int __real_munmap(void*, size_t);
extern "C" void __real_free(void*);

namespace mooncake::cleanup_test {
using namespace std::chrono_literals;

class PublicationStore final : public MetadataStoragePlugin {
   public:
    bool get(const std::string& key, Json::Value& value) override {
        std::lock_guard lock(mutex_);
        auto it = values_.find(key);
        if (it == values_.end()) return false;
        value = it->second;
        return true;
    }
    bool set(const std::string& key, const Json::Value& value) override {
        std::lock_guard lock(mutex_);
        if (fail_publication && key.find("/ram/") != std::string::npos) {
            ++failed_publications;
            return false;  // Leave the real previously published value intact.
        }
        values_[key] = value;
        return true;
    }
    bool remove(const std::string& key) override {
        std::lock_guard lock(mutex_);
        values_.erase(key);
        return true;
    }
    std::atomic<bool> fail_publication{false};
    std::atomic<int> failed_publications{0};

   private:
    std::mutex mutex_;
    std::map<std::string, Json::Value> values_;
};

enum class Fault { None, Master, Transfer };
struct PoolBacking {
    void* base = nullptr;
    std::optional<UUID> segment;
    int attempts = 0, released = 0;
};
class Scenario;
inline Scenario* active = nullptr;

// Owns the real runtime and the release witness. Suppressed unsafe releases are
// cleaned after faults are disabled; no failing test frees advertised memory.
class Scenario {
   public:
    Scenario();
    ~Scenario();
    void Start(bool with_real, bool with_pools = false);
    void Mount(bool file_mapping = false);
    bool MasterRemoved() {
        auto status = master.service()->QuerySegmentStatusById(id);
        return (!status && status.error() == ErrorCode::SEGMENT_NOT_FOUND) ||
               (status && *status == SegmentStatus::UNDEFINED);
    }
    bool Contains(const std::shared_ptr<TransferMetadata::SegmentDesc>& desc) {
        return Contains(desc, base);
    }
    bool Contains(const std::shared_ptr<TransferMetadata::SegmentDesc>& desc,
                  void* address) {
        if (!desc) return true;  // Unavailable metadata never authorizes free.
        return std::any_of(desc->buffers.begin(), desc->buffers.end(),
                           [address](const auto& b) {
                               return b.addr ==
                                      reinterpret_cast<uintptr_t>(address);
                           });
    }
    bool LocalContains() {
        return Contains(
            engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID));
    }
    bool RemoteContains() {
        // Fetch/decode the plugin's actual stored descriptor, bypassing cache.
        return Contains(engine->getMetadata()->getSegmentDesc(name));
    }
    bool PoolSafe(const PoolBacking& pool) {
        const auto status =
            pool.segment
                ? master.service()->QuerySegmentStatusById(*pool.segment)
                : tl::expected<SegmentStatus, ErrorCode>(
                      SegmentStatus::UNDEFINED);
        const bool master_removed =
            (!status && status.error() == ErrorCode::SEGMENT_NOT_FOUND) ||
            (status && *status == SegmentStatus::UNDEFINED);
        const bool registered = Contains(
            engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID),
            pool.base);
        const bool published =
            Contains(engine->getMetadata()->getSegmentDesc(name), pool.base);
        const bool safe =
            (!pools_require_client_destruction || retained_client.expired()) &&
            master_removed && !registered && !published;
        EXPECT_TRUE(safe) << "Pool release before cleanup: base=" << pool.base
                          << " client_alive=" << !retained_client.expired()
                          << " master_removed=" << master_removed
                          << " local_registered=" << registered
                          << " remotely_published=" << published;
        return safe;
    }
    bool ReleasePool(void* address) {
        for (auto& pool : pools) {
            if (!pool.base || address != pool.base) continue;
            ++pool.attempts;
            if (!PoolSafe(pool))
                return false;  // Guard unsafe initial-patch free.
            if (pool.released++ != 0) {
                ADD_FAILURE() << "Pool backing released more than once";
                return false;
            }
        }
        return true;
    }
    bool ReleaseAuthorized() {
        ++release_attempts;
        const bool master_removed = MasterRemoved();
        const bool local_registered = LocalContains();
        const bool remotely_published = RemoteContains();
        const bool worker_safe = !before_release || before_release();
        const bool safe = master_removed && !local_registered &&
                          !remotely_published && worker_safe;
        if (!safe) ++unsafe_releases;
        EXPECT_TRUE(safe) << "Release before cleanup: master_removed="
                          << master_removed
                          << " local_registered=" << local_registered
                          << " remotely_published=" << remotely_published
                          << " worker_safe=" << worker_safe;
        return safe;
    }
    void Callback(const UUID& callback_id) {
        EXPECT_EQ(callback_id, id);
        ++callbacks;
        free_memory("tcp", base);
    }
    void Grace(uint64_t milliseconds) {
        ASSERT_TRUE(client->UnmountSegmentById(
            id, milliseconds, [this](const UUID& value) { Callback(value); }));
    }
    void GraceReal(uint64_t seconds) {
        const std::vector<std::string> ids{UuidToString(id)};
        ASSERT_EQ(mapped ? real->unmountSegment(ids, seconds)
                         : real->unmountAndFreeSegment(ids, seconds),
                  0);
    }
    void ObserveUnregisterFailure() {
        std::lock_guard lock(event_mutex);
        ++unregister_failures;
        event.notify_all();
    }
    bool WaitForUnregisterFailure() {
        std::unique_lock lock(event_mutex);
        return event.wait_for(lock, 30s,
                              [this] { return unregister_failures != 0; });
    }
    static constexpr size_t size = facebook::cachelib::Slab::kSize;
    const std::string uri = "http://store-cleanup.invalid/metadata";
    std::shared_ptr<PublicationStore> publication =
        std::make_shared<PublicationStore>();
    testing::InProcMaster master;
    std::shared_ptr<TransferEngine> engine;
    std::shared_ptr<Client> client;
    std::shared_ptr<RealClient> real;
    std::weak_ptr<Client> retained_client;
    std::array<PoolBacking, 2> pools{};
    bool pools_require_client_destruction = true;
    std::function<bool()> before_release;
    std::string name, path;
    UUID id{}, owner{};
    void* base = nullptr;
    bool mapped = false;
    std::atomic<Fault> fault{Fault::None};
    std::atomic<int> callbacks{0}, release_attempts{0}, unsafe_releases{0},
        released{0};

   private:
    std::mutex event_mutex;
    std::condition_variable event;
    int unregister_failures = 0;
};
}  // namespace mooncake::cleanup_test
