#ifndef MOONCAKE_PG_COMM_TYPES_H
#define MOONCAKE_PG_COMM_TYPES_H

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <utility>

#include "error_types.h"

namespace mooncake {

enum class OpType : uint8_t {
    Unknown = 0,
    Broadcast,
    AllReduce,
    AllGather,
    ReduceScatter,
    AllToAll,
    Barrier,
    Reduce,
    Gather,
    Scatter,
    Send,
    Recv,
};

enum class DataType : uint8_t {
    Int8 = 0,
    Uint8,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int64,
    Uint64,
    Float16,
    Float32,
    Float64,
    Bfloat16,
    Bool,
    Float8e4m3fn,
    Float8e5m2,
    Float8e4m3fnuz,
    Float8e5m2fnuz,
    Float8e8m0fnu,
};

inline size_t elementSize(DataType dataType) {
    switch (dataType) {
        case DataType::Int8:
        case DataType::Uint8:
        case DataType::Bool:
        case DataType::Float8e4m3fn:
        case DataType::Float8e5m2:
        case DataType::Float8e4m3fnuz:
        case DataType::Float8e5m2fnuz:
        case DataType::Float8e8m0fnu:
            return 1;
        case DataType::Int16:
        case DataType::Uint16:
        case DataType::Float16:
        case DataType::Bfloat16:
            return 2;
        case DataType::Int32:
        case DataType::Uint32:
        case DataType::Float32:
            return 4;
        case DataType::Int64:
        case DataType::Uint64:
        case DataType::Float64:
            return 8;
    }
    PG_ASSERT(false,
              "unsupported Mooncake datatype: ", static_cast<int>(dataType));
}

enum class ReduceOp : uint8_t {
    Sum = 0,
    Avg = 1,
    Product = 2,
    Min = 3,
    Max = 4,
};

class WorkCompletion {
   public:
    explicit WorkCompletion(std::shared_future<void> completion)
        : completion_(std::move(completion)) {}

    bool isCompleted() const {
        if (completion_.wait_for(std::chrono::microseconds(0)) !=
            std::future_status::ready) {
            return false;
        }
        completion_.get();
        return true;
    }

    bool wait(std::chrono::microseconds timeout) const {
        if (timeout.count() < 0) {
            completion_.wait();
        } else if (completion_.wait_for(timeout) != std::future_status::ready) {
            return false;
        }
        completion_.get();
        return true;
    }

   private:
    std::shared_future<void> completion_;
};

struct CudaTaskSubmissionToken {
    size_t task_id;
    uint64_t sequence;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_COMM_TYPES_H
