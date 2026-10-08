// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "gather_read.h"
#include <algorithm>
#include <atomic>
#include <cstring>
#include <cstdlib>
#include <future>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include "store_rpc_client_io_context.h"
#include "types.h"
namespace mooncake::store {
std::vector<GatherReadPlan> PlanGatherReads(
    const std::vector<TransferEngine::ScatterTransferRange>& transfers,
    const std::vector<std::string>& endpoints) {
    std::vector<GatherReadPlan> plans;
    const char* enabled = std::getenv("MC_STORE_GATHER_READ");
    if ((enabled && std::string_view(enabled) == "0") ||
        endpoints.size() != transfers.size())
        return plans;
    using Key = std::pair<std::string, uintptr_t>;
    std::map<Key, std::vector<size_t>> groups;
    for (size_t i = 0; i < transfers.size(); ++i)
        if (!endpoints[i].empty())
            groups[{endpoints[i],
                    reinterpret_cast<uintptr_t>(transfers[i].local_buffer)}]
                .push_back(i);
    for (const auto& [key, indices] : groups) {
        size_t count = 0;
        for (size_t i : indices) {
            if (transfers[i].lengths.size() > 131072 - count) {
                count = 131073;
                break;
            }
            count += transfers[i].lengths.size();
        }
        if (count < 256 || count > 131072) continue;
        struct Fragment {
            uint64_t target, source, bytes;
        };
        std::vector<Fragment> fragments;
        fragments.reserve(count);
        bool valid = true;
        for (size_t i : indices) {
            const auto& t = transfers[i];
            if (t.opcode != TransferRequest::READ ||
                t.lengths.size() != t.local_offsets.size() ||
                t.lengths.size() != t.remote_offsets.size() ||
                !t.local_buffer ||
                t.lengths.size() > 131072 - fragments.size()) {
                valid = false;
                break;
            }
            for (size_t k = 0; k < t.lengths.size(); ++k) {
                auto n = t.lengths[k];
                if (!n || n > 1024 || t.local_offsets[k] > t.local_capacity ||
                    n > t.local_capacity - t.local_offsets[k] ||
                    t.remote_offsets[k] > t.remote_size ||
                    n > t.remote_size - t.remote_offsets[k] ||
                    t.remote_base_offset > UINT64_MAX - t.remote_offsets[k] ||
                    t.remote_base_offset + t.remote_offsets[k] >
                        UINT64_MAX - n ||
                    key.second > UINT64_MAX - t.local_offsets[k] ||
                    key.second + t.local_offsets[k] > UINT64_MAX - n) {
                    valid = false;
                    break;
                }
                fragments.push_back({key.second + t.local_offsets[k],
                                     t.remote_base_offset + t.remote_offsets[k],
                                     n});
            }
            if (!valid) break;
        }
        if (!valid || fragments.size() < 256) continue;
        std::sort(
            fragments.begin(), fragments.end(),
            [](const auto& a, const auto& b) { return a.target < b.target; });
        GatherReadPlan plan{key.first,
                            indices,
                            {},
                            reinterpret_cast<void*>(fragments.front().target),
                            0};
        uint64_t next = fragments.front().target;
        size_t source_runs = 0;
        uint64_t previous_source_end = 0;
        for (const auto& fragment : fragments) {
            if (fragment.target != next ||
                fragment.bytes > (512ULL << 20) - plan.bytes) {
                valid = false;
                break;
            }
            if (!source_runs || previous_source_end != fragment.source)
                ++source_runs;
            previous_source_end = fragment.source + fragment.bytes;
            plan.ranges.push_back({fragment.source, fragment.bytes});
            plan.bytes += fragment.bytes;
            next += fragment.bytes;
        }
        // Count source runs, not merely submitted rows: contiguous source
        // rows are already inexpensive on the existing coalescing path.
        if (valid && source_runs >= 256 && plan.bytes >= (64 << 10))
            plans.push_back(std::move(plan));
    }
    return plans;
}

namespace {
struct GatherRequest {
    std::string session;
    uint64_t sequence;
    std::string destination_segment;
    uint64_t destination;
    uint64_t capacity;
    std::vector<GatherReadRange> ranges;
};

GatherReadResult rejected(std::string error) {
    return {GatherReadCompletion::Rejected, std::move(error), 0};
}

bool contains(uint64_t base, uint64_t size, uint64_t address, uint64_t bytes) {
    return address >= base && bytes <= size && address - base <= size - bytes;
}
}  // namespace

class GatherReadService::Impl {
   public:
    struct Slot {
        BatchID batch = INVALID_BATCH_ID;
        std::vector<TransferRequest> request{1};
    };
    struct Worker {
        std::atomic<bool> busy{false};
        std::shared_ptr<void> staging;
        std::vector<Slot> slots;
    };

    Impl(std::shared_ptr<TransferEngine> engine, GatherReadOptions options,
         const std::string& owner_name)
        : engine_(std::move(engine)), options_(options) {
        if (!engine_ || engine_->isUsingTent() || !options.workers ||
            options.workers > 64 || !options.chunk_bytes ||
            options.chunk_bytes > (16ULL << 20) || !options.pipeline_depth ||
            options.pipeline_depth > 8 || !options.max_ranges ||
            options.max_ranges > 131072 || !options.max_bytes ||
            options.max_bytes > (512ULL << 20)) {
            throw std::invalid_argument(
                "invalid gather service configuration (classic TE required)");
        }

        owner_name_ =
            owner_name.empty() ? engine_->getLocalIpAndPort() : owner_name;
        for (size_t i = 0; i < options.workers; ++i) {
            auto worker = std::make_unique<Worker>();
            const size_t size = options.chunk_bytes * options.pipeline_depth;
            void* memory = nullptr;
            if (posix_memalign(&memory, 4096, size) != 0)
                throw std::bad_alloc();
            std::memset(memory, 0, size);
            if (engine_->registerLocalMemory(memory, size, kWildcardLocation,
                                             false) != 0) {
                std::free(memory);
                throw std::runtime_error(
                    "cannot register gather staging memory");
            }
            worker->staging =
                std::shared_ptr<void>(memory, [engine = engine_](void* p) {
                    if (engine->unregisterLocalMemory(p) == 0)
                        std::free(p);
                    else
                        LOG(ERROR) << "gather staging unregister failed; "
                                      "retaining allocation";
                });
            worker->slots.resize(options.pipeline_depth);
            workers_.push_back(std::move(worker));
        }
    }

    ~Impl() {
        if (server_) server_->stop();
    }

    Status drain(Slot& slot) {
        if (slot.batch == INVALID_BATCH_ID) return Status::OK();
        Status result = Status::OK();
        size_t polls = 0;
        while (true) {
            TransferStatus progress;
            auto status = engine_->getTransferStatus(slot.batch, 0, progress);
            if (!status.ok()) {
                result = status;
                // Submission can fail before publishing task 0. Only an
                // empty/completed batch is safe to release in that case.
                status = engine_->getBatchTransferStatus(slot.batch, progress);
                if (status.ok() && progress.s != TransferStatusEnum::COMPLETED)
                    status = result;
            }
            if (status.ok() && (progress.s == TransferStatusEnum::COMPLETED ||
                                progress.s == TransferStatusEnum::FAILED)) {
                if (progress.s == TransferStatusEnum::FAILED)
                    result = Status::Socket("gather RDMA write failed");
                auto released = engine_->freeBatchID(slot.batch);
                // Physical completion was confirmed above. Deferred cleanup
                // may still own TE bookkeeping, but never poll that invalid
                // handle again. These writes have no user callbacks.
                if (released.ok() || released.IsBatchCleanupDeferred()) {
                    slot.batch = INVALID_BATCH_ID;
                    return result;
                }
                if (result.ok()) result = released;
            }
            // TIMEOUT alone does not confirm DMA completion. Keep staging
            // and the batch alive until the transport physically drains.
            if (++polls < 64)
                PAUSE();
            else
                std::this_thread::yield();
        }
    }

    GatherReadResult execute(const GatherRequest& request) {
        std::shared_lock source_lock(sources_mutex_);
        if (request.ranges.size() > options_.max_ranges ||
            request.destination_segment.empty() ||
            request.destination_segment.size() > 1024)
            return rejected("invalid gather request");
        uint64_t bytes = 0;
        for (const auto& range : request.ranges) {
            const auto upper = sources_.upper_bound(range.offset);
            if (upper == sources_.begin())
                return rejected("source not mounted");
            const auto& source = *std::prev(upper);
            if (!contains(source.first, source.second, range.offset,
                          range.length) ||
                range.length > options_.max_bytes - bytes)
                return rejected("source range or total size out of bounds");
            bytes += range.length;
        }
        if (bytes > request.capacity ||
            request.destination > UINT64_MAX - bytes)
            return rejected("destination capacity or address overflow");
        if (!bytes) return {GatherReadCompletion::Completed, {}, 0};

        Worker* worker = nullptr;
        for (auto& candidate : workers_) {
            bool idle = false;
            if (candidate->busy.compare_exchange_strong(idle, true)) {
                worker = candidate.get();
                break;
            }
        }
        if (!worker) return rejected("gather service busy");
        struct Release {
            Worker* worker;
            ~Release() { worker->busy.store(false); }
        } release{worker};
        const auto segment = engine_->openSegment(request.destination_segment);
        if (segment == static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT))
            return rejected("cannot open destination segment");
        struct Close {
            TransferEngine* engine;
            SegmentHandle segment;
            ~Close() { engine->closeSegment(segment); }
        } close{engine_.get(), segment};
        std::vector<SegmentBufferInfo> buffers;
        const auto destination_registered = [&] {
            return engine_->getSegmentBuffers(segment, buffers) == 0 &&
                   std::any_of(buffers.begin(), buffers.end(),
                               [&](const auto& buffer) {
                                   return contains(buffer.addr, buffer.length,
                                                   request.destination, bytes);
                               });
        };
        if (!destination_registered()) {
            if (engine_->syncSegmentCache(request.destination_segment) != 0 ||
                !destination_registered())
                return rejected("destination is not remotely registered");
        }

        // No allocations after the first write: all slots and request vectors
        // are persistent. Every exit drains all writes before reusing staging.
        struct Drain {
            Impl* self;
            Worker* worker;
            ~Drain() {
                for (auto& slot : worker->slots) self->drain(slot);
            }
        } drain_on_exit{this, worker};
        size_t range_index = 0, within_range = 0, written = 0, chunk_index = 0;
        Status result = Status::OK();
        while (written < bytes) {
            const size_t slot_index = chunk_index++ % worker->slots.size();
            auto& slot = worker->slots[slot_index];
            result = drain(slot);
            if (!result.ok()) break;
            char* output = static_cast<char*>(worker->staging.get()) +
                           slot_index * options_.chunk_bytes;
            size_t packed = 0;
            while (packed < options_.chunk_bytes &&
                   range_index < request.ranges.size()) {
                const auto& range = request.ranges[range_index];
                const size_t n = std::min<uint64_t>(
                    options_.chunk_bytes - packed, range.length - within_range);
                std::memcpy(
                    output + packed,
                    reinterpret_cast<const char*>(range.offset) + within_range,
                    n);
                packed += n;
                within_range += n;
                if (within_range == range.length) {
                    ++range_index;
                    within_range = 0;
                }
            }
            slot.batch = engine_->allocateBatchID(1);
            if (slot.batch == INVALID_BATCH_ID) {
                result = Status::Memory("cannot allocate gather write batch");
                break;
            }
            slot.request[0] =
                TransferRequest{.opcode = TransferRequest::WRITE,
                                .source = output,
                                .target_id = segment,
                                .target_offset = request.destination + written,
                                .length = packed};
            result = engine_->submitTransfer(slot.batch, slot.request);
            if (!result.ok()) break;
            written += packed;
        }
        for (auto& slot : worker->slots) {
            auto status = drain(slot);
            if (!status.ok() && result.ok()) result = status;
        }
        return result.ok()
                   ? GatherReadResult{GatherReadCompletion::Completed,
                                      {},
                                      bytes}
                   : GatherReadResult{GatherReadCompletion::FailedDrained,
                                      result.ToString(), 0};
    }

    struct Session {
        std::mutex mutex;
        uint64_t sequence = 0;
        GatherReadResult result = rejected("session has no operation");
    };
    std::string open(const std::string& expected_owner) {
        if (!expected_owner.empty() && expected_owner != owner_name_) return {};
        std::lock_guard lock(sessions_mutex_);
        if (sessions_.size() >= 4096) return {};
        auto id = UuidToString(generate_uuid());
        sessions_.emplace(id, std::make_shared<Session>());
        return id;
    }
    std::shared_ptr<Session> session(const std::string& id) {
        std::lock_guard lock(sessions_mutex_);
        auto it = sessions_.find(id);
        return it == sessions_.end() ? nullptr : it->second;
    }
    GatherReadResult read(GatherRequest request) {
        auto state = session(request.session);
        if (!state) return rejected("unknown session");
        std::lock_guard lock(state->mutex);
        if (request.sequence <= state->sequence)
            return rejected("operation already fenced");
        state->sequence = request.sequence;
        state->result = execute(request);
        return state->result;
    }
    GatherReadResult fence(std::string id, uint64_t sequence) {
        auto state = session(id);
        // A missing session can mean an owner restart. Do not infer that the
        // old owner's DMA has drained from a different process's reply.
        if (!state) return {GatherReadCompletion::Unknown, "session lost", 0};
        std::lock_guard lock(state->mutex);
        if (sequence > state->sequence) {
            state->sequence = sequence;
            state->result = rejected("operation fenced before execution");
        }
        return state->result;
    }
    void close(std::string id) {
        std::lock_guard lock(sessions_mutex_);
        sessions_.erase(id);
    }
    std::mutex sessions_mutex_;
    std::unordered_map<std::string, std::shared_ptr<Session>> sessions_;

    std::shared_ptr<TransferEngine> engine_;
    std::shared_mutex sources_mutex_;
    std::map<uint64_t, uint64_t> sources_;
    GatherReadOptions options_;
    std::string owner_name_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
};

GatherReadService::GatherReadService(std::shared_ptr<TransferEngine> engine,
                                     GatherReadOptions options,
                                     const std::string& owner_name)
    : impl_(std::make_unique<Impl>(std::move(engine), options, owner_name)) {}
void GatherReadService::addSource(void* base, size_t bytes) {
    std::unique_lock lock(impl_->sources_mutex_);
    impl_->sources_.emplace(reinterpret_cast<uint64_t>(base), bytes);
}
void GatherReadService::removeSource(void* base) {
    std::unique_lock lock(impl_->sources_mutex_);
    impl_->sources_.erase(reinterpret_cast<uint64_t>(base));
}
GatherReadService::~GatherReadService() = default;
void GatherReadService::start(const std::string& address, uint16_t port) {
    if (impl_->server_)
        throw std::logic_error("gather service already started");
    impl_->server_ = std::make_unique<coro_rpc::coro_rpc_server>(
        impl_->options_.workers, port, address);
    impl_->server_->register_handler<&Impl::open, &Impl::read, &Impl::fence,
                                     &Impl::close>(impl_.get());
    impl_->server_->async_start();
    if (auto error = impl_->server_->get_errc()) {
        impl_->server_.reset();
        throw std::runtime_error(std::string(error.message()));
    }
}
uint16_t GatherReadService::port() const {
    return impl_->server_ ? impl_->server_->port() : 0;
}

struct GatherReadOperation::State {
    std::promise<GatherReadResult> promise;
    std::shared_future<GatherReadResult> future = promise.get_future().share();
    std::shared_ptr<void> lifetime;
    GatherRequest request;
};
GatherReadOperation::GatherReadOperation(std::shared_ptr<State> state)
    : state_(std::move(state)) {}
GatherReadResult GatherReadOperation::wait() const {
    return state_->future.get();
}
GatherReadResult GatherReadOperation::waitFor(
    std::chrono::milliseconds timeout) const {
    if (state_->future.wait_for(timeout) != std::future_status::ready)
        return {GatherReadCompletion::Pending, "gather read still pending", 0};
    return wait();
}

class GatherReadClient::Impl {
   public:
    Impl(std::shared_ptr<TransferEngine> engine, const std::string& endpoint,
         std::chrono::milliseconds timeout, const std::string& expected_owner)
        : engine(std::move(engine)),
          client(std::make_shared<coro_rpc::coro_rpc_client>(
              GetStoreRpcClientIoContextPool().get_executor())),
          timeout(timeout) {
        if (!this->engine || this->engine->isUsingTent() ||
            timeout.count() <= 0)
            throw std::invalid_argument(
                "invalid gather client configuration (classic TE required)");
        auto error = async_simple::coro::syncAwait(client->connect(endpoint));
        if (error.val()) throw std::runtime_error(std::string(error.message()));
        auto opened = async_simple::coro::syncAwait(
            client->call<&GatherReadService::Impl::open>(expected_owner));
        if (!opened || opened->empty())
            throw std::runtime_error("cannot open gather session");
        session = std::move(*opened);
        this->endpoint = endpoint;
    }
    ~Impl() {
        if (!poisoned && !session.empty()) {
            // The last owner can be the read callback on this RPC executor.
            // Never block that executor waiting for its own close response.
            auto rpc = client;
            rpc->call_for<&GatherReadService::Impl::close>(timeout, session)
                .start([rpc](auto&&) {});
        }
    }
    std::string endpoint, session;
    uint64_t sequence = 0;
    std::shared_ptr<TransferEngine> engine;
    std::shared_ptr<coro_rpc::coro_rpc_client> client;
    std::chrono::milliseconds timeout;
    std::atomic<bool> busy{false}, poisoned{false};
};
GatherReadClient::GatherReadClient(std::shared_ptr<TransferEngine> engine,
                                   const std::string& endpoint,
                                   std::chrono::milliseconds timeout,
                                   const std::string& expected_owner)
    : impl_(std::make_shared<Impl>(std::move(engine), endpoint, timeout,
                                   expected_owner)) {}
GatherReadClient::~GatherReadClient() = default;
bool GatherReadClient::available() const {
    return !impl_->busy.load() && !impl_->poisoned.load();
}

GatherReadOperation GatherReadClient::submitGatherRead(
    const std::vector<GatherReadRange>& ranges, void* destination,
    size_t capacity, std::shared_ptr<void> lifetime) {
    auto state = std::make_shared<GatherReadOperation::State>();
    GatherReadOperation operation(state);
    uint64_t bytes = 0;
    for (const auto& range : ranges) {
        if (range.length > capacity - bytes ||
            range.offset > UINT64_MAX - range.length) {
            state->promise.set_value(
                rejected("invalid gather range or destination capacity"));
            return operation;
        }
        bytes += range.length;
    }
    if (ranges.size() > 131072 || bytes > (512ULL << 20) ||
        reinterpret_cast<uintptr_t>(destination) > UINT64_MAX - bytes ||
        (bytes && (!destination || !lifetime))) {
        state->promise.set_value(
            rejected("invalid gather destination or request size"));
        return operation;
    }
    if (!bytes) {
        state->promise.set_value({GatherReadCompletion::Completed, {}, 0});
        return operation;
    }
    auto impl = impl_;
    if (impl->poisoned.load() || impl->busy.exchange(true)) {
        state->promise.set_value(
            rejected("gather client busy or requires recovery"));
        return operation;
    }
    state->lifetime = std::move(lifetime);
    state->request = {impl->session,
                      ++impl->sequence,
                      impl->engine->getLocalIpAndPort(),
                      reinterpret_cast<uint64_t>(destination),
                      capacity,
                      ranges};
    impl->client
        ->call_for<&GatherReadService::Impl::read>(impl->timeout,
                                                   state->request)
        .start([state, impl](auto&& response) {
            GatherReadResult result{GatherReadCompletion::Unknown,
                                    "gather RPC failed; retain destination "
                                    "until owner is quiescent",
                                    0};
            if (!response.hasError()) {
                auto& value = response.value();
                if (value.has_value()) result = std::move(value.value());
            }
            if (!result.drained()) {
                impl->poisoned.store(true);
                std::thread([state, impl] {
                    LOG(ERROR) << "Gather control connection lost; fencing "
                                  "remote writes before return";
                    while (true) {
                        coro_rpc::coro_rpc_client recovery(
                            GetStoreRpcClientIoContextPool().get_executor());
                        auto connected = async_simple::coro::syncAwait(
                            recovery.connect(impl->endpoint));
                        if (!connected) {
                            auto fenced = async_simple::coro::syncAwait(
                                recovery
                                    .call_for<&GatherReadService::Impl::fence>(
                                        impl->timeout, state->request.session,
                                        state->request.sequence));
                            if (fenced && fenced->drained()) {
                                async_simple::coro::syncAwait(
                                    recovery.call_for<
                                        &GatherReadService::Impl::close>(
                                        impl->timeout, impl->session));
                                impl->busy.store(false);
                                state->promise.set_value(std::move(*fenced));
                                return;
                            }
                        }
                        // Safe synchronous semantics: an unreachable/restarted
                        // owner cannot authorize reuse of the destination.
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                    }
                }).detach();
                return;
            }
            impl->busy.store(false);
            state->promise.set_value(std::move(result));
        });
    return operation;
}

}  // namespace mooncake::store
