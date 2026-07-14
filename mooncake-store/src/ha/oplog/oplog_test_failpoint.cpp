#include "ha/oplog/oplog_test_failpoint.h"

#ifdef MOONCAKE_ENABLE_TEST_FAILPOINTS

#include <glog/logging.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

namespace mooncake {
namespace {

bool IsValidName(std::string_view name) {
    if (name.empty()) {
        return false;
    }
    for (char c : name) {
        if ((c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_') {
            return false;
        }
    }
    return true;
}

std::chrono::seconds Timeout() {
    const char* value = std::getenv("MOONCAKE_TEST_FAILPOINT_TIMEOUT_SEC");
    if (value == nullptr) {
        return std::chrono::seconds(30);
    }
    char* end = nullptr;
    const long seconds = std::strtol(value, &end, 10);
    if (end == value || *end != '\0' || seconds <= 0 || seconds > 3600) {
        return std::chrono::seconds(30);
    }
    return std::chrono::seconds(seconds);
}

void Remove(const std::filesystem::path& path) {
    std::error_code error;
    std::filesystem::remove(path, error);
}

}  // namespace

bool TestFailPoint::Wait(std::string_view name) {
    const char* directory = std::getenv("MOONCAKE_TEST_FAILPOINT_DIR");
    if (directory == nullptr || !IsValidName(name)) {
        return false;
    }
    const std::filesystem::path root(directory);
    const std::string base(name);
    const auto arm = root / (base + ".arm");
    const auto claim = root / (base + ".claimed." + std::to_string(::getpid()));
    std::error_code error;
    std::filesystem::rename(arm, claim, error);
    if (error) {
        return false;
    }

    const auto hit = root / (base + ".hit");
    const auto release = root / (base + ".release");
    const auto temporary =
        root / (base + ".hit.tmp." + std::to_string(::getpid()));
    Remove(release);
    {
        std::ofstream output(temporary);
        if (!output) {
            Remove(claim);
            LOG(ERROR) << "Failed to create failpoint hit file: " << temporary;
            return false;
        }
        output << ::getpid() << '\n';
    }
    error.clear();
    std::filesystem::rename(temporary, hit, error);
    if (error) {
        Remove(temporary);
        Remove(claim);
        LOG(ERROR) << "Failed to publish failpoint hit file: "
                   << error.message();
        return false;
    }

    const auto deadline = std::chrono::steady_clock::now() + Timeout();
    while (std::chrono::steady_clock::now() < deadline) {
        if (std::filesystem::exists(release)) {
            Remove(release);
            Remove(hit);
            Remove(claim);
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    Remove(hit);
    Remove(claim);
    LOG(ERROR) << "Timed out waiting for failpoint release: " << name;
    return false;
}

}  // namespace mooncake

#else

namespace mooncake {

bool TestFailPoint::Wait(std::string_view) { return false; }

}  // namespace mooncake

#endif
