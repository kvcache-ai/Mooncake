// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef TEBENCH_SELECTION_METRICS_H
#define TEBENCH_SELECTION_METRICS_H

#include <cstddef>
#include <string>

#include "tent/common/selection_stats.h"

namespace mooncake {
namespace tent {

struct RdmaSelectionMetricsReport {
    size_t block_size = 0;
    size_t batch_size = 0;
    int num_threads = 0;
    std::string backend;
    std::string op_type;
    SelectionStats selection;
};

// The caller must take both snapshots from one selector without resetting it
// between reads. The report contains the end-minus-start measurement window.
RdmaSelectionMetricsReport calculateRdmaSelectionMetrics(
    size_t block_size, size_t batch_size, int num_threads,
    const std::string& backend, const std::string& op_type,
    const SelectionStats& start, const SelectionStats& end);

bool appendRdmaSelectionMetricsJsonl(const std::string& path,
                                     const RdmaSelectionMetricsReport& report,
                                     std::string* error);

}  // namespace tent
}  // namespace mooncake

#endif  // TEBENCH_SELECTION_METRICS_H
