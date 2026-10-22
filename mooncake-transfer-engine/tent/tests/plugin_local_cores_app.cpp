// Copyright 2025 KVCache.AI
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

// Two TENT cores, each a module that links TENT statically, both opened
// RTLD_LOCAL by a program that links no TENT itself (like two Python
// extensions). Usage: plugin_local_cores_app <plugin-dir> <core-a> <core-b>.
// The first core to load plugins is promoted and keeps them; the second is
// refused with host_symbols_not_exported naming the first.

#include <dlfcn.h>

#include <iostream>
#include <string>

#define REQUIRE(cond)                                                  \
    do {                                                               \
        if (!(cond)) {                                                 \
            std::cerr << "FAILED: " #cond " (line " << __LINE__ << ")" \
                      << std::endl;                                    \
            return 1;                                                  \
        }                                                              \
    } while (0)

using Check = int (*)(const char*, char*, size_t);

static std::string runCore(const char* path, const char* dir) {
    void* core = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (!core) return std::string("dlopen failed: ") + dlerror();
    auto check =
        reinterpret_cast<Check>(dlsym(core, "tent_test_second_core_check"));
    if (!check) return "no check function";
    char result[4096] = {};
    check(dir, result, sizeof(result));
    return result;
}

int main(int argc, char** argv) {
    REQUIRE(argc == 4);
    const std::string first = runCore(argv[2], argv[1]);
    std::cout << "first core:  " << first << std::endl;
    REQUIRE(first.rfind("available;loaded|", 0) == 0);
    const std::string second = runCore(argv[3], argv[1]);
    std::cout << "second core: " << second << std::endl;
    REQUIRE(second.rfind("available;host_symbols_not_exported|", 0) == 0);
    REQUIRE(second.find("another TENT copy") != std::string::npos);
    REQUIRE(second.find(argv[2]) != std::string::npos);
    std::cout << "plugin_local_cores_app: OK" << std::endl;
    return 0;
}
