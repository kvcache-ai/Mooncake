// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_TESTS_METRICS_TEST_UTILS_H_
#define TENT_TESTS_METRICS_TEST_UTILS_H_

#include <sstream>
#include <string>
#include <string_view>

namespace mooncake::tent::test {

inline double PrometheusSeriesValue(const std::string& exposition,
                                    std::string_view series) {
    std::istringstream input(exposition);
    std::string line;
    const std::string prefix = std::string(series) + " ";
    while (std::getline(input, line)) {
        if (line.rfind(prefix, 0) != 0) continue;
        try {
            return std::stod(line.substr(prefix.size()));
        } catch (...) {
            return 0.0;
        }
    }
    return 0.0;
}

}  // namespace mooncake::tent::test

#endif  // TENT_TESTS_METRICS_TEST_UTILS_H_
