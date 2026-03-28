#pragma once

#include <memory>

namespace mooncake {
namespace store {

class HotStandbyService {
public:
    HotStandbyService();
    ~HotStandbyService();

    // Disable copy constructor and assignment operator
    HotStandbyService(const HotStandbyService&) = delete;
    HotStandbyService& operator=(const HotStandbyService&) = delete;

    // Start the hot standby service
    bool Start();

    // Stop the hot standby service
    bool Stop();

    // Promote to primary mode
    bool PromoteToPrimary();

    // Check if service is running
    bool IsRunning() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace store
} // namespace mooncake
