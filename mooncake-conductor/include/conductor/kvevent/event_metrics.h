#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <locale>
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

    // Live-only subscribers can redispatch a transport sequence. Suppress
    // duplicate observations without changing index mutation or replay policy.
    bool AcceptSequence(int64_t sequence) {
        std::lock_guard lock(mu_);
        if (sequence < 0) return true;  // Direct calls without a transport ID.
        if (sequence <= last_sequence_) return false;
        last_sequence_ = sequence;
        return true;
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

    std::string Render(const std::string& labels) const {
        static constexpr std::array<const char*, 4> types = {
            "stored", "removed", "cleared", "unknown"};
        static constexpr std::array<const char*, 3> results = {
            "applied", "ignored", "rejected"};
        decltype(events_) events;
        decltype(blocks_) blocks;
        double last_applied;
        {
            std::lock_guard lock(mu_);
            events = events_;
            blocks = blocks_;
            last_applied = last_applied_;
        }
        // Formatting a scrape must not hold the event writer's lock.
        std::ostringstream out;
        out.imbue(std::locale::classic());
        out << std::setprecision(17);
        for (size_t origin = 0; origin < 2; ++origin) {
            const std::string source =
                labels + ",origin=" + Quote(origin == 0 ? "live" : "replay");
            for (size_t type = 0; type < types.size(); ++type) {
                for (size_t result = 0; result < results.size(); ++result) {
                    // Undecodable events have no trusted type.
                    if (type == kUnknown && result != kRejected) continue;
                    out << "mooncake_conductor_events_total{" << source
                        << ",event_type=" << Quote(types[type])
                        << ",result=" << Quote(results[result]) << "} "
                        << events[origin][type][result] << '\n';
                }
                if (type == kStored || type == kRemoved) {
                    out << "mooncake_conductor_event_block_references_total{"
                        << source << ",event_type=" << Quote(types[type])
                        << "} " << blocks[origin][type] << '\n';
                }
            }
        }
        if (last_applied > 0) {
            out << "mooncake_conductor_last_applied_event_timestamp_seconds{"
                << labels << "} " << last_applied << '\n';
        }
        return out.str();
    }

   private:
    mutable std::mutex mu_;
    std::array<std::array<std::array<uint64_t, 3>, 4>, 2> events_{};
    std::array<std::array<uint64_t, 2>, 2> blocks_{};
    double last_applied_ = 0;
    int64_t last_sequence_ = -1;
};

}  // namespace mooncake::conductor::kvevent
