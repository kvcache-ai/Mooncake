#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <ylt/metric/gauge.hpp>

namespace mooncake {

// A sampled Ping response, not segment completeness or data-plane health.
// Connection generations prevent a late response from reviving an observation
// after reconnect. No metric lock is held across a network operation.
class MasterHeartbeatMetric {
   public:
    explicit MasterHeartbeatMetric(
        const std::map<std::string, std::string>& labels = {})
        : status_ok_("mooncake_client_master_heartbeat_status_ok",
                     "Last observed Master Ping status: 1 for OK, 0 for "
                     "NEED_REMOUNT; absent when unknown",
                     EscapeLabels(labels)),
          timestamp_seconds_(
              "mooncake_client_master_heartbeat_observation_timestamp_seconds",
              "Client Unix receive time of the same Master Ping observation "
              "in seconds; absent when unknown",
              EscapeLabels(labels)) {}

    uint64_t BeginConnection() {
        std::lock_guard<std::mutex> lock(mutex_);
        connected_ = false;
        known_ = false;
        return ++generation_;
    }

    void EndConnection(uint64_t generation, bool connected) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (generation == generation_) connected_ = connected;
    }

    std::optional<uint64_t> BeginObservation() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) return std::nullopt;
        return generation_;
    }

    // nullopt represents a failed Ping or an unsupported status, never a
    // negative registration verdict. A missing generation means the Ping
    // overlapped a connection attempt and cannot be attributed safely.
    void Observe(std::optional<uint64_t> generation,
                 std::optional<bool> status_ok, double timestamp_seconds) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_ || !generation || *generation != generation_) return;
        known_ = status_ok.has_value();
        if (known_) {
            status_ok_.update(*status_ok ? 1 : 0);
            timestamp_seconds_.update(timestamp_seconds);
        }
    }

    void serialize(std::string& str) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!known_) return;
        status_ok_.serialize(str);
        timestamp_seconds_.serialize(str);
    }

   private:
    // The pinned YLT static-gauge serializer writes label values verbatim.
    static std::map<std::string, std::string> EscapeLabels(
        std::map<std::string, std::string> labels) {
        for (auto& [name, value] : labels) {
            std::string escaped;
            for (char ch : value) {
                if (ch == '\\' || ch == '"') escaped += '\\';
                if (ch == '\n') {
                    escaped += "\\n";
                } else {
                    escaped += ch;
                }
            }
            value = std::move(escaped);
        }
        return labels;
    }

    mutable std::mutex mutex_;
    uint64_t generation_ = 0;
    bool connected_ = false;
    bool known_ = false;
    ylt::metric::gauge_t status_ok_;
    ylt::metric::gauge_d timestamp_seconds_;
};

}  // namespace mooncake
