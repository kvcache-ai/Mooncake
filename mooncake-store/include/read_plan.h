#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

namespace mooncake {
class PyClient;
// Destination base, destination row stride, byte count, object source offset.
using ReadComponent = std::array<size_t, 4>;
// Object keys, destination row indices, packed-object flag, components per
// group. Packed: one object per row. Unpacked: one object per row/component.
using ReadLayout = std::tuple<std::vector<std::string>, std::vector<size_t>,
                              bool, std::vector<std::vector<ReadComponent>>>;

// One-shot ordered range reads. Owns a strong client reference, not raw
// destination memory. Keep destination allocations registered/alive and do not
// close the client until run() ends. Do not mix legacy sessions on these keys
// with a plan. Concurrent plans on the same client with overlapping keys fail
// explicitly. Callers must ensure destination byte ranges in different groups
// do not overlap, including across layouts, in sequential and pipeline modes.
// This precondition is not checked; violating it may corrupt consumed data.
// Published groups remain successful even if a later group fails.
class ReadPlan {
   public:
    ReadPlan(std::shared_ptr<PyClient> client, std::vector<ReadLayout> layouts,
             int num_groups);
    ~ReadPlan();
    ReadPlan(const ReadPlan&) = delete;
    ReadPlan& operator=(const ReadPlan&) = delete;
    void run();
    void wait(int group);
    // True only after reads and session cleanup have stopped using the client.
    bool is_finished() const;
    std::vector<uint64_t> stats();

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace mooncake
