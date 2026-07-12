#pragma once

// Shared helpers for loading JSON golden-vector fixtures. uint64 values are
// stored as decimal strings so that values above 2^53 survive JSON
// round-trips without precision loss.

#include <json/json.h>

#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>

namespace conductor_test {

inline Json::Value LoadJsonFixture(const std::string& filename) {
    const std::string path =
        std::string(CONDUCTOR_TEST_FIXTURE_DIR) + "/" + filename;
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("cannot open fixture: " + path);
    }
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errs;
    if (!Json::parseFromStream(builder, in, &root, &errs)) {
        throw std::runtime_error("cannot parse fixture " + path + ": " + errs);
    }
    return root;
}

inline uint64_t ParseU64(const Json::Value& v) {
    return std::stoull(v.asString());
}

}  // namespace conductor_test
