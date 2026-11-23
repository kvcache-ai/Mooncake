// Copyright 2024 KVCache.AI
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

#include <iostream>
#include <map>
#include <string>

#include <pybind11/embed.h>
#include <pybind11/stl.h>

#include "store/store_key_py.h"

namespace py = pybind11;

namespace {

// hashlib.sha256(b"image").digest(). Byte 2 is 0xd6, which is not UTF-8.
std::string Sha256ImageKey() {
    return std::string(
        "\x61\x05\xd6\xcc\x76\xaf\x40\x03"
        "\x25\xe9\x4d\x58\x8c\xe5\x11\xbe"
        "\x5b\xfd\xbb\x73\xb4\x37\xdc\x51"
        "\xec\xa4\x39\x17\xd7\xa4\x3e\x3d",
        32);
}

const char* TypeName(const py::handle& obj) {
    return Py_TYPE(obj.ptr())->tp_name;
}

}  // namespace

int main() {
    py::scoped_interpreter guard{};
    int failures = 0;
    const auto expect = [&failures](bool condition, const char* message) {
        if (!condition) {
            std::cerr << message << '\n';
            ++failures;
        }
    };

    const py::object text_key = mooncake::StoreKeyToPy("mooncake_key");
    std::cout << "utf8 key -> " << TypeName(text_key) << '\n';
    expect(py::isinstance<py::str>(text_key), "utf8 key must be str");
    expect(!py::isinstance<py::bytes>(text_key), "utf8 key must not be bytes");
    expect(text_key.cast<std::string>() == "mooncake_key",
           "utf8 key must round-trip");

    const std::string binary = Sha256ImageKey();
    expect(binary.size() == 32 && static_cast<unsigned char>(binary[2]) == 0xd6,
           "fixture must be the reported sha256 digest");
    const py::object binary_key = mooncake::StoreKeyToPy(binary);
    std::cout << "sha256 key -> " << TypeName(binary_key) << '\n';
    expect(py::isinstance<py::bytes>(binary_key), "binary key must be bytes");
    expect(!py::isinstance<py::str>(binary_key), "binary key must not be str");
    expect(binary_key.cast<std::string>() == binary,
           "binary key must keep its bytes");

    const std::string embedded_nul("a\0b", 3);
    const py::object nul_key = mooncake::StoreKeyToPy(embedded_nul);
    std::cout << "embedded nul -> " << TypeName(nul_key) << '\n';
    expect(py::isinstance<py::str>(nul_key), "nul inside utf8 must stay str");
    expect(nul_key.cast<std::string>() == embedded_nul,
           "nul inside utf8 must be preserved");

    py::dict batch;
    batch[text_key] = 1;
    batch[binary_key] = 2;
    std::cout << "batch size -> " << batch.size() << '\n';
    expect(batch.size() == 2, "one binary key must not drop the other key");
    expect(batch["mooncake_key"].cast<int>() == 1, "str key lookup failed");
    expect(batch[py::bytes(binary.data(), binary.size())].cast<int>() == 2,
           "bytes key lookup failed");

    bool map_caster_rejects_binary = false;
    try {
        py::object mapped = py::cast(std::map<std::string, int>{{binary, 1}});
        (void)mapped;
    } catch (const py::error_already_set& error) {
        const std::string text = error.what();
        map_caster_rejects_binary =
            text.find("UnicodeDecodeError") != std::string::npos;
        std::cout << "map caster -> " << text << '\n';
    }
    expect(map_caster_rejects_binary,
           "std::map caster must still reject a binary key");

    if (failures != 0) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "ok\n";
    return 0;
}
