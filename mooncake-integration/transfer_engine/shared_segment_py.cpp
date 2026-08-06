// Copyright 2026 Huawei Technologies Co., Ltd
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

#include "shared_segment_py.h"

#include <pybind11/stl.h>

#include <stdexcept>
#include <utility>

namespace py = pybind11;

namespace mooncake {
namespace {
void ThrowIfError(const Status& status) {
    if (!status.ok()) {
        throw std::runtime_error(status.ToString());
    }
}

// Returns the unfinished segment plus this rank's blob. The caller all-gathers
// the blob and passes every rank's copy back to Complete.
// SharedSegmentOptions is an implementation detail and is not exposed to
// Python; callers pass the fields directly.
std::pair<std::shared_ptr<SharedSegment>, py::bytes> SegmentCreate(
    const std::string& name, uint64_t size, uint32_t world_size,
    uint32_t rank_id, uint32_t owner_rank, int32_t device_id, bool mmap) {
    SharedSegmentOptions options;
    options.world_size = world_size;
    options.rank_id = rank_id;
    options.owner_rank = owner_rank;
    options.device_id = device_id;
    options.mmap = mmap;

    std::string blob;
    std::shared_ptr<SharedSegment> segment;
    Status status = Status::OK();
    {
        py::gil_scoped_release release;
        status = SharedSegment::Create(name, size, options, segment, blob);
    }
    ThrowIfError(status);
    return {std::move(segment), py::bytes(blob)};
}

void SegmentComplete(const std::shared_ptr<SharedSegment>& segment,
                     const std::vector<std::string>& blobs) {
    Status status = Status::OK();
    {
        py::gil_scoped_release release;
        status = segment->Complete(blobs);
    }
    ThrowIfError(status);
}
}  // namespace

void bind_shared_segment(py::module_& m) {
    py::class_<SharedSegment, std::shared_ptr<SharedSegment>>(m,
                                                              "SharedSegment")
        .def_static("create", &SegmentCreate, py::arg("name"), py::arg("size"),
                    py::arg("world_size"), py::arg("rank_id"),
                    py::arg("owner_rank") = 0, py::arg("device_id") = 0,
                    py::arg("mmap") = false,
                    "Phase one; returns (segment, blob).")
        .def_static("supported", &SharedSegment::Supported,
                    py::arg("mmap") = false,
                    "Whether this build can share memory across processes.")
        .def("complete", &SegmentComplete, py::arg("blobs"),
             "Phase two; blobs are indexed by rank.")
        .def("ready", &SharedSegment::ready)
        .def("base_addr", &SharedSegment::base_addr)
        .def("device_addr", &SharedSegment::device_addr)
        .def("size", &SharedSegment::size);
}

}  // namespace mooncake
