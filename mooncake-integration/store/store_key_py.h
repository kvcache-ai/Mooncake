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

#pragma once

#include <pybind11/pybind11.h>

#include <string>

namespace mooncake {

// Caller must hold the GIL. Valid UTF-8 stays str so existing dict lookups
// keep working. Any other byte string becomes bytes. Returning
// std::map<std::string, ...> from pybind would UTF-8-decode every key and
// raise UnicodeDecodeError for the whole batch.
inline pybind11::object StoreKeyToPy(const std::string& key) {
    PyObject* decoded = PyUnicode_DecodeUTF8(
        key.data(), static_cast<Py_ssize_t>(key.size()), nullptr);
    if (decoded != nullptr) {
        return pybind11::reinterpret_steal<pybind11::object>(decoded);
    }
    PyErr_Clear();
    return pybind11::bytes(key.data(), key.size());
}

}  // namespace mooncake
