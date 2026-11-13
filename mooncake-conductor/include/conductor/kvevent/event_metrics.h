#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <locale>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>

namespace mooncake::conductor::kvevent {

// Fixed-size counters owned by a subscription, without request or hash labels.
// Unregistering releases the counters with the handler.
class EventMetrics {
   public:
    enum Type { kStored, kRemoved, kCleared, kUnknown };
    enum Result { kApplied, kIgnored, kRejected };

    // Observe redeliveries, but never suppress processing results. A batch can
    // contain both successful and rejected events, and a retry can correct the
    // rejected ones. Negative sequences identify direct calls without an ID.
    void RecordBatch(int64_t sequence, bool replay) {
        std::lock_guard lock(mu_);
        if (sequence < 0) return;
        if (sequence <= last_sequence_) {
            ++redeliveries_[replay];
        } else {
            last_sequence_ = sequence;
        }
    }

    static std::string Quote(std::string_view value) {
        std::string out = "\"";
        for (char c : value) {
            if (c == '\\' || c == '"') out += '\\';
            if (c == '\n') {
                out += "\\n";
            } else {
                out += c;
            }
        }
        return out + "\"";
    }

    void Record(Type type, Result result, bool replay, uint64_t blocks = 0) {
        std::lock_guard lock(mu_);
        ++events_[replay][type][result];
        if (result == kApplied) {
            if (type == kStored || type == kRemoved) {
                blocks_[replay][type] += blocks;
            }
            last_applied_ =
                std::chrono::duration<double>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();
        }
    }

    // Take one snapshot, then format independently of the event writer. The
    // caller can combine subscriptions while keeping each metric family intact.
    std::map<std::string, std::string> RenderFamilies(
        const std::string& labels) const {
        static constexpr std::array<const char*, 4> types = {
            "stored", "removed", "cleared", "unknown"};
        static constexpr std::array<const char*, 3> results = {
            "applied", "ignored", "rejected"};
        decltype(events_) events;
        decltype(blocks_) blocks;
        decltype(redeliveries_) redeliveries;
        double last_applied;
        {
            std::lock_guard lock(mu_);
            events = events_;
            blocks = blocks_;
            redeliveries = redeliveries_;
            last_applied = last_applied_;
        }
        // Formatting a scrape must not hold the event writer's lock.
        std::ostringstream events_out, blocks_out, timestamp_out,
            redeliveries_out;
        for (auto* out :
             {&events_out, &blocks_out, &timestamp_out, &redeliveries_out}) {
            out->imbue(std::locale::classic());
            *out << std::setprecision(17);
        }
        for (size_t origin = 0; origin < 2; ++origin) {
            const std::string source =
                labels + ",origin=" + Quote(origin == 0 ? "live" : "replay");
            redeliveries_out
                << "mooncake_conductor_event_batch_redeliveries_total{"
                << source << "} " << redeliveries[origin] << '\n';
            for (size_t type = 0; type < types.size(); ++type) {
                for (size_t result = 0; result < results.size(); ++result) {
                    // Undecodable events have no trusted type.
                    if (type == kUnknown && result != kRejected) continue;
                    events_out << "mooncake_conductor_events_total{" << source
                               << ",event_type=" << Quote(types[type])
                               << ",result=" << Quote(results[result]) << "} "
                               << events[origin][type][result] << '\n';
                }
                if (type == kStored || type == kRemoved) {
                    blocks_out
                        << "mooncake_conductor_event_block_references_total{"
                        << source << ",event_type=" << Quote(types[type])
                        << "} " << blocks[origin][type] << '\n';
                }
            }
        }
        if (last_applied > 0) {
            timestamp_out
                << "mooncake_conductor_last_applied_event_timestamp_seconds{"
                << labels << "} " << last_applied << '\n';
        }
        return {{"mooncake_conductor_events_total", events_out.str()},
                {"mooncake_conductor_event_block_references_total",
                 blocks_out.str()},
                {"mooncake_conductor_last_applied_event_timestamp_seconds",
                 timestamp_out.str()},
                {"mooncake_conductor_event_batch_redeliveries_total",
                 redeliveries_out.str()}};
    }

    std::string Render(const std::string& labels) const {
        std::string out;
        for (const auto& [name, samples] : RenderFamilies(labels)) {
            out += samples;
        }
        return out;
    }

   private:
    mutable std::mutex mu_;
    std::array<std::array<std::array<uint64_t, 3>, 4>, 2> events_{};
    std::array<std::array<uint64_t, 2>, 2> blocks_{};
    std::array<uint64_t, 2> redeliveries_{};
    double last_applied_ = 0;
    int64_t last_sequence_ = -1;
};

}  // namespace mooncake::conductor::kvevent
