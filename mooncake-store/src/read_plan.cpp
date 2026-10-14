#include "read_plan.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "pyclient.h"

namespace mooncake {
namespace {
// Scoped reservations protect plans sharing the legacy client's key-indexed
// session map. The lock is not held during RPC, transfers, or waits.
std::mutex active_mutex;
std::map<PyClient *, std::set<std::string>> active_keys;
class ActiveKeys {
    PyClient *client_;
    std::set<std::string> keys_;

   public:
    ActiveKeys(PyClient *client, const std::vector<std::string> &keys)
        : client_(client), keys_(keys.begin(), keys.end()) {
        std::lock_guard<std::mutex> lock(active_mutex);
        auto &active = active_keys[client_];
        for (const auto &key : keys_)
            if (active.count(key))
                throw std::runtime_error(
                    "overlapping read plan on the same client/key");
        // Roll back even if allocating a set node fails partway through.
        try {
            active.insert(keys_.begin(), keys_.end());
        } catch (...) {
            for (const auto &key : keys_) active.erase(key);
            if (active.empty()) active_keys.erase(client_);
            throw;
        }
    }
    ~ActiveKeys() {
        std::lock_guard<std::mutex> lock(active_mutex);
        auto it = active_keys.find(client_);
        if (it == active_keys.end()) return;
        for (const auto &key : keys_) it->second.erase(key);
        if (it->second.empty()) active_keys.erase(it);
    }
};
// Keep tuple/array inputs at the API boundary; execution uses named fields.
struct Component {
    size_t destination_base;
    size_t destination_row_stride;
    size_t byte_count;
    size_t source_offset;
};
struct Layout {
    std::vector<std::string> keys;
    std::vector<size_t> rows;
    bool packed;
    std::vector<std::vector<Component>> groups;
};
std::vector<Layout> decode_layouts(std::vector<ReadLayout> inputs) {
    std::vector<Layout> layouts;
    layouts.reserve(inputs.size());
    for (auto &[keys, rows, packed, groups] : inputs) {
        Layout layout{std::move(keys), std::move(rows), packed, {}};
        layout.groups.reserve(groups.size());
        for (const auto &group : groups) {
            std::vector<Component> components;
            components.reserve(group.size());
            for (const auto &[base, stride, bytes, offset] : group)
                components.push_back({base, stride, bytes, offset});
            layout.groups.push_back(std::move(components));
        }
        layouts.push_back(std::move(layout));
    }
    return layouts;
}
struct Ranges {
    std::vector<std::string> keys;
    std::vector<std::vector<void *>> addresses;
    std::vector<std::vector<size_t>> sizes, offsets;
};
size_t checked_add(size_t a, size_t b) {
    if (b > SIZE_MAX - a) throw std::overflow_error("range overflow");
    return a + b;
}
size_t address(const Component &component, size_t row) {
    if (component.destination_row_stride &&
        row > SIZE_MAX / component.destination_row_stride)
        throw std::overflow_error("row address overflow");
    auto p = checked_add(component.destination_base,
                         row * component.destination_row_stride);
    checked_add(p, component.byte_count);
    checked_add(component.source_offset, component.byte_count);
    return p;
}
}  // namespace
struct ReadPlan::Impl {
    std::shared_ptr<mooncake::PyClient> client;
    std::vector<Layout> layouts;
    int groups;
    std::mutex mutex;
    std::condition_variable cv;
    int ready = -1;
    bool running = false, finished = false;
    std::exception_ptr failure;
    std::vector<uint64_t> stats{0, 0, 0};  // calls, keys, bytes
    void mark(int group) {
        {
            std::lock_guard lock(mutex);
            ready = group;
        }
        cv.notify_all();
    }
    void finish(std::exception_ptr error) {
        {
            std::lock_guard lock(mutex);
            failure = error;
            finished = true;
            if (!error) ready = groups - 1;
        }
        cv.notify_all();
    }

   public:
    Impl(std::shared_ptr<PyClient> c, std::vector<ReadLayout> inputs, int n)
        : client(std::move(c)),
          layouts(decode_layouts(std::move(inputs))),
          groups(n) {
        if (!client) throw std::invalid_argument("read plan requires a client");
        if (n <= 0) throw std::invalid_argument("num_groups must be positive");
        for (const auto &p : layouts) {
            const auto &[keys, rows, packed, layout] = p;
            if (layout.size() != size_t(n))
                throw std::invalid_argument("group layout count mismatch");
            // All groups in this layout share the same destination rows.
            const auto [lo, hi] = std::minmax_element(rows.begin(), rows.end());
            for (const auto &items : layout) {
                if (items.empty()) continue;
                if (rows.size() > SIZE_MAX / items.size())
                    throw std::overflow_error("key count overflow");
                if (keys.size() != rows.size() * (packed ? 1 : items.size()))
                    throw std::invalid_argument(
                        "key/row/component count mismatch");
                if (!rows.empty()) {
                    for (const auto &component : items) {
                        address(component, *lo);
                        address(component, *hi);
                    }
                }
            }
        }
    }
    Ranges build(int group) const {
        if (group < 0 || group >= groups)
            throw std::out_of_range("group out of range");
        Ranges out;
        size_t total = 0;
        for (const auto &[keys, rows, packed, layout] : layouts)
            if (!layout[group].empty()) total = checked_add(total, keys.size());
        out.keys.reserve(total);
        out.addresses.reserve(total);
        out.sizes.reserve(total);
        out.offsets.reserve(total);
        for (const auto &[keys, rows, packed, layout] : layouts) {
            const auto &items = layout[group];
            if (items.empty()) continue;
            out.keys.insert(out.keys.end(), keys.begin(), keys.end());
            for (auto row : rows) {
                if (packed) {
                    std::vector<void *> a;
                    std::vector<size_t> s, o;
                    a.reserve(items.size());
                    s.reserve(items.size());
                    o.reserve(items.size());
                    for (const auto &component : items) {
                        a.push_back(
                            reinterpret_cast<void *>(address(component, row)));
                        s.push_back(component.byte_count);
                        o.push_back(component.source_offset);
                    }
                    out.addresses.push_back(std::move(a));
                    out.sizes.push_back(std::move(s));
                    out.offsets.push_back(std::move(o));
                } else
                    for (const auto &component : items) {
                        out.addresses.push_back({reinterpret_cast<void *>(
                            address(component, row))});
                        out.sizes.push_back({component.byte_count});
                        out.offsets.push_back({component.source_offset});
                    }
            }
        }
        return out;
    }
    void check(const Ranges &r, const std::vector<int> &results, int group) {
        if (results.size() != r.keys.size())
            throw std::runtime_error(
                "Mooncake read plan result count mismatch at group=" +
                std::to_string(group) +
                " expected=" + std::to_string(r.keys.size()) +
                " actual=" + std::to_string(results.size()));
        uint64_t bytes = 0;
        for (size_t k = 0; k < results.size(); ++k) {
            size_t expected = 0;
            for (auto s : r.sizes[k]) expected = checked_add(expected, s);
            if (results[k] < 0 || size_t(results[k]) != expected)
                throw std::runtime_error(
                    "Mooncake read plan range get failed at group=" +
                    std::to_string(group) + " key_index=" + std::to_string(k) +
                    " expected_bytes=" + std::to_string(expected) +
                    (results[k] < 0 ? " error_code=" : " actual_bytes=") +
                    std::to_string(results[k]));
            bytes += expected;
        }
        ++stats[0];
        stats[1] += results.size();
        stats[2] += bytes;
    }

    void run_pipelined() {
        struct Slot {
            Ranges ranges;
            std::vector<int> result;
            std::exception_ptr error;
            bool done = false;
        };
        std::vector<Slot> slots(groups);
        std::mutex work_mutex;
        std::condition_variable work_cv;
        // Publication advances this window; callers need not have consumed
        // the data or called wait(). Destination groups must remain disjoint.
        int next = 0, published_groups = 0;
        bool stop = false;
        auto worker = [&] {
            while (true) {
                int g;
                {
                    std::unique_lock lock(work_mutex);
                    work_cv.wait(lock, [&] {
                        return stop || next >= groups ||
                               next < published_groups + 2;
                    });
                    if (stop || next >= groups) return;
                    g = next++;
                }
                auto &slot = slots[g];
                try {
                    slot.ranges = build(g);
                    auto &r = slot.ranges;
                    if (!r.keys.empty())
                        slot.result =
                            client->batch_get_into_multi_buffer_ranges(
                                r.keys, r.addresses, r.sizes, r.offsets);
                } catch (...) {
                    slot.error = std::current_exception();
                }
                {
                    std::lock_guard lock(work_mutex);
                    slot.done = true;
                }
                work_cv.notify_all();
            }
        };
        std::vector<std::thread> workers;
        auto drain = [&] {
            {
                std::lock_guard lock(work_mutex);
                stop = true;
            }
            work_cv.notify_all();
            for (auto &t : workers)
                if (t.joinable()) t.join();
        };
        // reserve before creating threads so allocation cannot destroy a
        // joinable temporary. Always drain reads before session cleanup.
        workers.reserve(2);
        try {
            workers.emplace_back(worker);
            workers.emplace_back(worker);
            for (int g = 0; g < groups; ++g) {
                auto &slot = slots[g];
                {
                    std::unique_lock lock(work_mutex);
                    work_cv.wait(lock, [&] { return slot.done; });
                }
                if (slot.error) std::rethrow_exception(slot.error);
                if (!slot.ranges.keys.empty())
                    check(slot.ranges, slot.result, g);
                if (g < groups - 1) mark(g);
                slot.ranges = Ranges{};
                slot.result.clear();
                {
                    std::lock_guard lock(work_mutex);
                    published_groups = g + 1;
                }
                work_cv.notify_all();
            }
        } catch (...) {
            drain();
            throw;
        }
        drain();
    }

    void run_impl() {
        {
            std::lock_guard lock(mutex);
            if (running) throw std::runtime_error("plan may only run once");
            running = true;
        }
        std::vector<std::string> session;
        bool started = false;
        std::exception_ptr error;
        std::unique_ptr<ActiveKeys> reservation;
        try {
            std::set<std::string> seen;
            for (const auto &p : layouts) {
                for (const auto &key : p.keys)
                    if (seen.insert(key).second) session.push_back(key);
            }
            reservation = std::make_unique<ActiveKeys>(client.get(), session);
            {
                started = true;
                auto result = client->batch_get_session_start(session);
                if (result.size() != session.size() ||
                    std::any_of(result.begin(), result.end(),
                                [](int x) { return x != 0; }))
                    throw std::runtime_error(
                        "Mooncake read plan session start failed");
            }
            const char *enabled = std::getenv("MOONCAKE_READ_PLAN_PIPELINE");
            const bool requested = enabled && std::string(enabled) == "1";
            const bool pipeline = requested && groups > 1;
            if (requested) {
                static std::atomic<bool> logged_yes{false}, logged_no{false};
                auto &logged = pipeline ? logged_yes : logged_no;
                if (!logged.exchange(true))
                    std::fprintf(
                        stderr,
                        "MOONCAKE_READ_PLAN_PIPELINE enabled=%d groups=%d "
                        "keys=%zu\n",
                        int(pipeline), groups, session.size());
            }
            if (pipeline) {
                run_pipelined();
            } else {
                for (int group = 0; group < groups; ++group) {
                    auto r = build(group);
                    if (!r.keys.empty()) {
                        auto result =
                            client->batch_get_into_multi_buffer_ranges(
                                r.keys, r.addresses, r.sizes, r.offsets);
                        check(r, result, group);
                    }
                    if (group < groups - 1)
                        mark(group);  // Final readiness includes session
                                      // cleanup.
                }
            }
        } catch (...) {
            error = std::current_exception();
        }
        if (started) {
            try {
                if (client->batch_get_session_end(session) != 0)
                    throw std::runtime_error(
                        "read plan session cleanup failed");
            } catch (...) {
                if (!error) error = std::current_exception();
            }
        }
        reservation.reset();
        finish(error);
        if (error) std::rethrow_exception(error);
    }
    void run() { run_impl(); }
    void wait(int group) {
        if (group < 0 || group >= groups)
            throw std::out_of_range("group out of range");
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return ready >= group || finished; });
        if (ready >= group) return;
        if (failure) std::rethrow_exception(failure);
        if (ready < group)
            throw std::runtime_error("plan ended before requested group");
    }
    bool is_finished() {
        std::lock_guard lock(mutex);
        return finished;
    }
    std::vector<uint64_t> get_stats() {
        std::lock_guard lock(mutex);
        if (!finished) throw std::runtime_error("stats require completed plan");
        return stats;
    }
};
ReadPlan::ReadPlan(std::shared_ptr<PyClient> client,
                   std::vector<ReadLayout> layouts, int groups)
    : impl_(std::make_unique<Impl>(std::move(client), std::move(layouts),
                                   groups)) {}
ReadPlan::~ReadPlan() = default;
void ReadPlan::run() { impl_->run(); }
void ReadPlan::wait(int group) { impl_->wait(group); }
bool ReadPlan::is_finished() const { return impl_->is_finished(); }
std::vector<uint64_t> ReadPlan::stats() { return impl_->get_stats(); }
}  // namespace mooncake
