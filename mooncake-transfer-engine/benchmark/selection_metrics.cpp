// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "selection_metrics.h"

#include <algorithm>
#include <fstream>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
namespace {

uint64_t delta(uint64_t start, uint64_t end) {
    return end >= start ? end - start : 0;
}

const DeviceSelectionStats* findDevice(
    const std::vector<DeviceSelectionStats>& devices, int dev_id) {
    const auto it = std::find_if(
        devices.begin(), devices.end(),
        [dev_id](const auto& device) { return device.dev_id == dev_id; });
    return it == devices.end() ? nullptr : &*it;
}

}  // namespace

RdmaSelectionMetricsReport calculateRdmaSelectionMetrics(
    size_t block_size, size_t batch_size, int num_threads,
    const std::string& backend, const std::string& op_type,
    const SelectionStats& start, const SelectionStats& end) {
    RdmaSelectionMetricsReport report;
    report.block_size = block_size;
    report.batch_size = batch_size;
    report.num_threads = num_threads;
    report.backend = backend;
    report.op_type = op_type;
    report.selection.allocations = delta(start.allocations, end.allocations);
    report.selection.single_path_allocations =
        delta(start.single_path_allocations, end.single_path_allocations);
    report.selection.multi_path_allocations =
        delta(start.multi_path_allocations, end.multi_path_allocations);
    report.selection.probe_allocations =
        delta(start.probe_allocations, end.probe_allocations);
    report.selection.devices.reserve(end.devices.size());
    for (const auto& device : end.devices) {
        const auto* previous = findDevice(start.devices, device.dev_id);
        report.selection.devices.push_back({
            device.dev_id,
            device.available,
            delta(previous ? previous->selected_slices : 0,
                  device.selected_slices),
            delta(previous ? previous->selected_bytes : 0,
                  device.selected_bytes),
            device.device_name,
            device.numa_node,
        });
    }
    return report;
}

bool appendRdmaSelectionMetricsJsonl(const std::string& path,
                                     const RdmaSelectionMetricsReport& report,
                                     std::string* error) {
    nlohmann::json root = {
        {"schema_version", 1},
        {"record_type", "rdma_selection_metrics"},
        {"measurement_window", "snapshot_delta"},
        {"backend", report.backend},
        {"op_type", report.op_type},
        {"block_size", report.block_size},
        {"batch_size", report.batch_size},
        {"num_threads", report.num_threads},
        {"selection",
         {
             {"allocations", report.selection.allocations},
             {"single_path_allocations",
              report.selection.single_path_allocations},
             {"multi_path_allocations",
              report.selection.multi_path_allocations},
             {"probe_allocations", report.selection.probe_allocations},
             {"devices", nlohmann::json::array()},
         }},
    };
    for (const auto& device : report.selection.devices) {
        root["selection"]["devices"].push_back({
            {"dev_id", device.dev_id},
            {"available", device.available},
            {"selected_slices", device.selected_slices},
            {"selected_bytes", device.selected_bytes},
            {"device_name", device.device_name},
            {"numa_node", device.numa_node},
        });
    }

    std::ofstream output(path, std::ios::app);
    if (!output) {
        *error = "failed to open selection JSONL output: " + path;
        return false;
    }
    output << root.dump() << '\n';
    output.close();
    if (!output) {
        *error = "failed to write selection JSONL output: " + path;
        return false;
    }
    return true;
}

}  // namespace tent
}  // namespace mooncake
