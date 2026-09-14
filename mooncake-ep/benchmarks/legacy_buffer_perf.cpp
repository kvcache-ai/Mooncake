#include <cuda_alike.h>

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <mooncake_ep_buffer.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace mooncake {
namespace {

struct LegacyBufferPerfTensors {
    uint64_t x_ptr = 0;
    uint64_t topk_idx_ptr = 0;
    uint64_t topk_weights_ptr = 0;
    uint64_t active_ranks_ptr = 0;
    uint64_t expert_x_ptr = 0;
    uint64_t packed_recv_x_ptr = 0;
    uint64_t packed_recv_count_ptr = 0;
    uint64_t packed_recv_src_info_ptr = 0;
    uint64_t packed_recv_layout_range_ptr = 0;
    uint64_t combined_x_ptr = 0;
};

struct LegacyBufferPerfConfig {
    int num_tokens = 0;
    int hidden = 0;
    int num_topk = 0;
    int num_local_experts = 0;
    int num_max_dispatch_tokens_per_rank = 0;
    int num_experts = 0;
    int timeout_us = -1;
    int warmups = 20;
    int iterations = 30;
    uint64_t compute_stream_ptr = 0;
};

struct LegacyBufferPerfResult {
    double average_us = 0.0;
    double min_us = 0.0;
    double max_us = 0.0;
    double dispatch_average_us = 0.0;
    double combine_average_us = 0.0;
};

void cuda_check(cudaError_t status, const char* operation) {
    if (status != cudaSuccess) {
        throw std::runtime_error(std::string(operation) + ": " +
                                 cudaGetErrorString(status));
    }
}

void validate(MooncakeEpBuffer& buffer, const LegacyBufferPerfTensors& tensors,
              const LegacyBufferPerfConfig& config) {
    if (config.num_tokens <= 0 || config.hidden <= 0 || config.num_topk <= 0 ||
        config.num_max_dispatch_tokens_per_rank < config.num_tokens ||
        config.num_experts <= 0 || config.num_local_experts <= 0 ||
        config.warmups < 0 || config.iterations <= 1) {
        throw std::invalid_argument(
            "invalid legacy EP benchmark configuration");
    }
    if (buffer.ibgda_disabled() && !buffer.use_fast_path()) {
        throw std::runtime_error(
            "legacy EP benchmark requires the native fast path");
    }
    if (!tensors.x_ptr || !tensors.topk_idx_ptr || !tensors.topk_weights_ptr ||
        !tensors.active_ranks_ptr || !tensors.expert_x_ptr ||
        !tensors.packed_recv_x_ptr || !tensors.packed_recv_count_ptr ||
        !tensors.packed_recv_src_info_ptr ||
        !tensors.packed_recv_layout_range_ptr || !tensors.combined_x_ptr) {
        throw std::invalid_argument(
            "legacy EP benchmark received a null tensor pointer");
    }
}

LegacyBufferPerfResult run_legacy_buffer_perf(
    MooncakeEpBuffer& buffer, const LegacyBufferPerfTensors& tensors,
    const LegacyBufferPerfConfig& config) {
    validate(buffer, tensors, config);

    const int num_local_experts = config.num_local_experts;
    const auto compute_stream =
        reinterpret_cast<cudaStream_t>(config.compute_stream_ptr);
    const size_t recv_count_bytes =
        static_cast<size_t>(num_local_experts) * sizeof(int);
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA split SEND/RECV uses a communication stream. Avoid the synchronous
    // compute<->communication stream cycle in the generic legacy wrapper; the
    // benchmark synchronizes the device before recording each end event.
    constexpr bool kAsyncDispatch = true;
    constexpr bool kAsyncCombine = false;
#else
    constexpr bool kAsyncDispatch = false;
    constexpr bool kAsyncCombine = false;
#endif

    auto run_dispatch = [&] {
        cuda_check(cudaMemsetAsync(
                       reinterpret_cast<void*>(tensors.packed_recv_count_ptr),
                       0, recv_count_bytes, compute_stream),
                   "cudaMemsetAsync(packed_recv_count)");
        buffer.dispatch(
            tensors.x_ptr, tensors.topk_idx_ptr, tensors.active_ranks_ptr,
            config.num_tokens, config.hidden, config.num_topk,
            config.num_max_dispatch_tokens_per_rank, config.num_experts,
            config.timeout_us, false, tensors.packed_recv_x_ptr, 0,
            tensors.packed_recv_count_ptr, tensors.packed_recv_src_info_ptr,
            tensors.packed_recv_layout_range_ptr, kAsyncDispatch, false,
            config.compute_stream_ptr);
    };
    auto run_combine = [&] {
        buffer.combine(
            tensors.expert_x_ptr, tensors.topk_idx_ptr,
            tensors.topk_weights_ptr, tensors.packed_recv_src_info_ptr,
            tensors.packed_recv_layout_range_ptr, tensors.active_ranks_ptr,
            num_local_experts, config.num_tokens, config.hidden,
            config.num_topk, config.num_max_dispatch_tokens_per_rank,
            config.num_experts, config.timeout_us, false,
            tensors.combined_x_ptr, kAsyncCombine, false,
            config.compute_stream_ptr);
    };
    auto run_once = [&] {
        run_dispatch();
        run_combine();
    };

    for (int i = 0; i < config.warmups; ++i) run_once();
    cuda_check(cudaDeviceSynchronize(), "cudaDeviceSynchronize(warmup)");

    std::vector<cudaEvent_t> starts(config.iterations);
    std::vector<cudaEvent_t> ends(config.iterations);
    std::vector<cudaEvent_t> dispatch_starts(config.iterations);
    std::vector<cudaEvent_t> dispatch_ends(config.iterations);
    std::vector<cudaEvent_t> combine_starts(config.iterations);
    std::vector<cudaEvent_t> combine_ends(config.iterations);
    for (int i = 0; i < config.iterations; ++i) {
        cuda_check(cudaEventCreate(&starts[i]), "cudaEventCreate(start)");
        cuda_check(cudaEventCreate(&ends[i]), "cudaEventCreate(end)");
        cuda_check(cudaEventCreate(&dispatch_starts[i]), "cudaEventCreate(dispatch_start)");
        cuda_check(cudaEventCreate(&dispatch_ends[i]), "cudaEventCreate(dispatch_end)");
        cuda_check(cudaEventCreate(&combine_starts[i]), "cudaEventCreate(combine_start)");
        cuda_check(cudaEventCreate(&combine_ends[i]), "cudaEventCreate(combine_end)");
        cuda_check(cudaEventRecord(starts[i], compute_stream),
                   "cudaEventRecord(start)");
        cuda_check(cudaEventRecord(dispatch_starts[i], compute_stream),
                   "cudaEventRecord(dispatch_start)");
        run_dispatch();
        cuda_check(cudaDeviceSynchronize(), "cudaDeviceSynchronize(dispatch)");
        cuda_check(cudaEventRecord(dispatch_ends[i], compute_stream),
                   "cudaEventRecord(dispatch_end)");
        cuda_check(cudaEventRecord(combine_starts[i], compute_stream),
                   "cudaEventRecord(combine_start)");
        run_combine();
        cuda_check(cudaDeviceSynchronize(), "cudaDeviceSynchronize(iteration)");
        cuda_check(cudaEventRecord(combine_ends[i], compute_stream),
                   "cudaEventRecord(combine_end)");
        cuda_check(cudaEventRecord(ends[i], compute_stream),
                   "cudaEventRecord(end)");
    }
    cuda_check(cudaEventSynchronize(ends.back()), "cudaEventSynchronize(end)");

    std::vector<float> timings_us;
    timings_us.reserve(config.iterations - 1);
    for (int i = 1; i < config.iterations; ++i) {
        float elapsed_ms = 0.0f;
        cuda_check(cudaEventElapsedTime(&elapsed_ms, starts[i], ends[i]),
                   "cudaEventElapsedTime");
        timings_us.push_back(elapsed_ms * 1000.0f);
    }
    for (auto event : starts) cudaEventDestroy(event);
    for (auto event : ends) cudaEventDestroy(event);

    std::vector<float> dispatch_timings_us;
    std::vector<float> combine_timings_us;
    dispatch_timings_us.reserve(config.iterations - 1);
    combine_timings_us.reserve(config.iterations - 1);
    for (int i = 1; i < config.iterations; ++i) {
        float elapsed_ms = 0.0f;
        cuda_check(cudaEventElapsedTime(&elapsed_ms, dispatch_starts[i], dispatch_ends[i]),
                   "cudaEventElapsedTime(dispatch)");
        dispatch_timings_us.push_back(elapsed_ms * 1000.0f);
        elapsed_ms = 0.0f;
        cuda_check(cudaEventElapsedTime(&elapsed_ms, combine_starts[i], combine_ends[i]),
                   "cudaEventElapsedTime(combine)");
        combine_timings_us.push_back(elapsed_ms * 1000.0f);
    }
    for (auto event : dispatch_starts) cudaEventDestroy(event);
    for (auto event : dispatch_ends) cudaEventDestroy(event);
    for (auto event : combine_starts) cudaEventDestroy(event);
    for (auto event : combine_ends) cudaEventDestroy(event);

    double total_us = 0.0;
    for (float value : timings_us) total_us += value;
    const auto [min_it, max_it] =
        std::minmax_element(timings_us.begin(), timings_us.end());
    double dispatch_total_us = 0.0;
    double combine_total_us = 0.0;
    for (size_t i = 0; i < dispatch_timings_us.size(); ++i) {
        dispatch_total_us += dispatch_timings_us[i];
        combine_total_us += combine_timings_us[i];
    }
    return {total_us / timings_us.size(), *min_it, *max_it,
            dispatch_total_us / dispatch_timings_us.size(),
            combine_total_us / combine_timings_us.size()};
}

}  // namespace

void bind_legacy_buffer_perf(py::module_& module) {
    py::class_<LegacyBufferPerfTensors>(module, "_LegacyBufferPerfTensors")
        .def(py::init<>())
        .def_readwrite("x_ptr", &LegacyBufferPerfTensors::x_ptr)
        .def_readwrite("topk_idx_ptr", &LegacyBufferPerfTensors::topk_idx_ptr)
        .def_readwrite("topk_weights_ptr",
                       &LegacyBufferPerfTensors::topk_weights_ptr)
        .def_readwrite("active_ranks_ptr",
                       &LegacyBufferPerfTensors::active_ranks_ptr)
        .def_readwrite("expert_x_ptr", &LegacyBufferPerfTensors::expert_x_ptr)
        .def_readwrite("packed_recv_x_ptr",
                       &LegacyBufferPerfTensors::packed_recv_x_ptr)
        .def_readwrite("packed_recv_count_ptr",
                       &LegacyBufferPerfTensors::packed_recv_count_ptr)
        .def_readwrite("packed_recv_src_info_ptr",
                       &LegacyBufferPerfTensors::packed_recv_src_info_ptr)
        .def_readwrite("packed_recv_layout_range_ptr",
                       &LegacyBufferPerfTensors::packed_recv_layout_range_ptr)
        .def_readwrite("combined_x_ptr",
                       &LegacyBufferPerfTensors::combined_x_ptr);

    py::class_<LegacyBufferPerfConfig>(module, "_LegacyBufferPerfConfig")
        .def(py::init<>())
        .def_readwrite("num_tokens", &LegacyBufferPerfConfig::num_tokens)
        .def_readwrite("hidden", &LegacyBufferPerfConfig::hidden)
        .def_readwrite("num_topk", &LegacyBufferPerfConfig::num_topk)
        .def_readwrite("num_local_experts",
                       &LegacyBufferPerfConfig::num_local_experts)
        .def_readwrite(
            "num_max_dispatch_tokens_per_rank",
            &LegacyBufferPerfConfig::num_max_dispatch_tokens_per_rank)
        .def_readwrite("num_experts", &LegacyBufferPerfConfig::num_experts)
        .def_readwrite("timeout_us", &LegacyBufferPerfConfig::timeout_us)
        .def_readwrite("warmups", &LegacyBufferPerfConfig::warmups)
        .def_readwrite("iterations", &LegacyBufferPerfConfig::iterations)
        .def_readwrite("compute_stream_ptr",
                       &LegacyBufferPerfConfig::compute_stream_ptr);

    py::class_<LegacyBufferPerfResult>(module, "_LegacyBufferPerfResult")
        .def_readonly("average_us", &LegacyBufferPerfResult::average_us)
        .def_readonly("min_us", &LegacyBufferPerfResult::min_us)
        .def_readonly("max_us", &LegacyBufferPerfResult::max_us)
        .def_readonly("dispatch_average_us",
                      &LegacyBufferPerfResult::dispatch_average_us)
        .def_readonly("combine_average_us",
                      &LegacyBufferPerfResult::combine_average_us);

    module.def("_benchmark_legacy_buffer", &run_legacy_buffer_perf,
               py::arg("buffer"), py::arg("tensors"), py::arg("config"),
               py::call_guard<py::gil_scoped_release>());
}

}  // namespace mooncake
