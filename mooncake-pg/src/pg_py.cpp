#include <mooncake_backend.h>
#include <pybind11/gil.h>
#include <pybind11/stl.h>
#include <torch/csrc/utils/pybind.h>
#include <torch/python.h>
#include <torch/torch.h>

#include <mutex>

#include "control_plane/agent_host.h"

namespace py = pybind11;

namespace mooncake {

// Process-level context — definition in mooncake_backend.h.
static MooncakeProcessContext g_ctx;

static std::once_flag g_init_control_plane_once;

#ifdef USE_MACA
static void requireMacaHostTransport() {
    TORCH_CHECK(std::getenv("MC_MACA_HOST_TRANSPORT") != nullptr,
                "MACA PG requires MC_MACA_HOST_TRANSPORT=1 so the transfer "
                "engine uses a host transport.");
}

#endif

static AgentHost& initControlPlane(const c10::intrusive_ptr<c10d::Store>& store,
                                   int rank, int max_world_size) {
    std::call_once(g_init_control_plane_once, [&] {
        // Ordering constraint: AgentHost::start() sends registerAgent
        // immediately, which includes LinkManager's localServerName() and
        // getWarmupRecvAddr().  These must be non-empty, so the engine and
        // LinkManager must be initialized BEFORE AgentHost starts.
        if (!g_ctx.engine_initialized) {
#ifdef USE_MACA
            requireMacaHostTransport();
#endif
            g_ctx.engine->init(P2PHANDSHAKE, g_ctx.host_ip);
            g_ctx.engine_initialized = true;
        }
        if (!g_ctx.link_manager.isInitialized()) {
            g_ctx.link_manager.init(static_cast<GlobalRank>(rank),
                                    max_world_size, g_ctx.engine);
        }

        // Rank 0 hosts the Coordinator in-process.  It must start BEFORE
        // AgentHost so the coordinator_addr key is in the Store when
        // AgentHost::start() reads it.
        if (rank == 0) {
            g_ctx.coordinator_host = std::make_unique<CoordinatorHost>(
                store, g_ctx.host_ip, max_world_size);
            g_ctx.coordinator_host->start();
        }

        g_ctx.agent_host = std::make_unique<AgentHost>(
            store, g_ctx.host_ip, static_cast<GlobalRank>(rank), max_world_size,
            g_ctx.link_manager);
        g_ctx.agent_host->start();
    });
    return *g_ctx.agent_host;
}

c10::intrusive_ptr<c10d::ProcessGroup> createMooncakeBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>
        backendOptions) {
    int rank = distBackendOpts.group_rank;
    int size = distBackendOpts.group_size;
    int max_size = (backendOptions && backendOptions->maxWorldSize_ > 0)
                       ? backendOptions->maxWorldSize_
                       : size;
    auto& host = initControlPlane(distBackendOpts.store, rank, max_size);
    return c10::make_intrusive<MooncakeBackend>(
        std::move(distBackendOpts), std::move(backendOptions), host, g_ctx);
}

c10::intrusive_ptr<c10d::ProcessGroup> createMooncakeCpuBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>
        backendOptions) {
    int rank = distBackendOpts.group_rank;
    int size = distBackendOpts.group_size;
    int max_size = (backendOptions && backendOptions->maxWorldSize_ > 0)
                       ? backendOptions->maxWorldSize_
                       : size;
    auto& host = initControlPlane(distBackendOpts.store, rank, max_size);
    return c10::make_intrusive<MooncakeBackend>(std::move(distBackendOpts),
                                                std::move(backendOptions), host,
                                                g_ctx, true);
}

__attribute__((constructor)) static void MooncakeBackendConstructor() {
    py::object module = py::module::import("torch.distributed");
    py::object register_backend =
        module.attr("Backend").attr("register_backend");
    py::dict kwargsCpu;
    kwargsCpu["devices"] = py::make_tuple("cpu");
    register_backend("mooncake-cpu", py::cpp_function(createMooncakeCpuBackend),
                     /* extended_api */ true, **kwargsCpu);
#ifndef MOONCAKE_EP_USE_MUSA
    py::dict kwargsCuda;
    kwargsCuda["devices"] = py::make_tuple("cuda");
    register_backend("mooncake", py::cpp_function(createMooncakeBackend),
                     /* extended_api */ true, **kwargsCuda);
#else
    py::dict kwargsMusa;
    kwargsMusa["devices"] = py::make_tuple("musa");
    register_backend("mooncake", py::cpp_function(createMooncakeBackend),
                     /* extended_api */ true, **kwargsMusa);
#endif
}

std::string getPreferredHca(c10::intrusive_ptr<c10d::ProcessGroup> backend,
                            std::string location) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->getPreferredHca(location);
}

at::Tensor getActiveRanks(c10::intrusive_ptr<c10d::ProcessGroup> backend) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->getActiveRanksTensor();
}

int getNumSyncedRanks(c10::intrusive_ptr<c10d::ProcessGroup> backend) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->getNumSyncedRanks();
}

void extendGroupSizeTo(c10::intrusive_ptr<c10d::ProcessGroup> backend,
                       int size) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    mooncakeBackend->extendGroupSizeTo(size);
}

std::vector<bool> getPeerState(c10::intrusive_ptr<c10d::ProcessGroup> backend,
                               const std::vector<int>& ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->getPeerState(ranks);
}

void recoverRanks(c10::intrusive_ptr<c10d::ProcessGroup> backend,
                  const std::vector<int>& ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    mooncakeBackend->recoverRanks(ranks);
}

void deactivateRank(c10::intrusive_ptr<c10d::ProcessGroup> backend,
                    const std::vector<int>& ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    auto resp = mooncakeBackend->deactivateRank(ranks);
    if (resp.status == ViewUpdateStatus::Rejected) {
        throw std::runtime_error("deactivate_rank rejected: " +
                                 resp.reject_reason);
    }
}

void activateRank(c10::intrusive_ptr<c10d::ProcessGroup> backend,
                  const std::vector<int>& ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    auto resp = mooncakeBackend->activateRank(ranks);
    if (resp.status == ViewUpdateStatus::Rejected) {
        throw std::runtime_error("activate_rank rejected: " +
                                 resp.reject_reason);
    }
}

void joinGroup(c10::intrusive_ptr<c10d::ProcessGroup> backend) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    mooncakeBackend->joinGroup();
}

// ---- New APIs ----

int64_t getGroupEpoch(c10::intrusive_ptr<c10d::ProcessGroup> backend) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return static_cast<int64_t>(mooncakeBackend->getGroupEpoch());
}

at::Tensor getFailedRanks(c10::intrusive_ptr<c10d::Work> work) {
    if (auto* w = dynamic_cast<MooncakeWorkCuda*>(work.get())) {
        return w->getFailedRanks();
    }
    if (auto* w = dynamic_cast<MooncakeWorkCpu*>(work.get())) {
        return w->getFailedRanks();
    }
    if (auto* w = dynamic_cast<MooncakeP2PWork*>(work.get())) {
        return w->getFailedRanks();
    }
    return at::Tensor();
}

/// Python-facing wrapper that extracts the raw TransferEngine* from a
/// mooncake.engine.TransferEngine Python object and passes it to
/// MooncakeBackend::setExternalEngine().  The caller must ensure the
/// TransferEnginePy object outlives all MooncakeBackend instances.
void setTransferEnginePy(pybind11::object engine_obj) {
    if (engine_obj.is_none()) {
        g_ctx.external_engine = nullptr;
        return;
    }
    auto get_engine_ptr = engine_obj.attr("get_engine_ptr");
    uintptr_t ptr = get_engine_ptr().cast<uintptr_t>();
    g_ctx.external_engine = reinterpret_cast<TransferEngine*>(ptr);
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("createMooncakeBackend", &createMooncakeBackend);
    m.def("createMooncakeCpuBackend", &createMooncakeCpuBackend);
    m.def("set_host_ip", [](const std::string& host) { g_ctx.host_ip = host; });
    m.def(
        "set_collective_timeout_us",
        [](size_t us) { g_ctx.collective_timeout_us = us; }, py::arg("us"),
        "Set the default peer-liveness probe timeout (microseconds) for "
        "collective operations.");
    m.def(
        "set_p2p_timeout_us", [](int64_t us) { g_ctx.p2p_timeout_us = us; },
        py::arg("us"), "Set the default P2P transfer timeout (microseconds).");
    m.def("set_device_filter", [](std::vector<std::string> filters) {
        if (g_ctx.engine) g_ctx.engine->setWhitelistFilters(std::move(filters));
    });
    m.def("set_transfer_engine", &setTransferEnginePy, py::arg("engine"),
          "Set an external TransferEngine to be used by MooncakeBackend. "
          "Must be called before init_process_group(). The engine must already "
          "be initialized. Pass None to reset to default behavior. "
          "The caller must ensure the TransferEngine object outlives all "
          "MooncakeBackend instances.");
    m.def("get_preferred_hca", &getPreferredHca);
    m.def("get_active_ranks", &getActiveRanks);
    m.def("get_num_synced_ranks", &getNumSyncedRanks);
    m.def("extend_group_size_to", &extendGroupSizeTo);
    m.def("get_peer_state", &getPeerState);
    m.def("recover_ranks", &recoverRanks);
    m.def("activate_rank", &activateRank);
    m.def("deactivate_rank", &deactivateRank, py::arg("backend"),
          py::arg("ranks"));
    m.def("join_group", &joinGroup);
    m.def("get_group_epoch", &getGroupEpoch, py::arg("backend"),
          "Get the current GroupView epoch for the backend's group.");
    m.def("get_failed_ranks", &getFailedRanks, py::arg("work"));

    py::class_<MooncakeBackend::MooncakeBackendOptions,
               c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>>(
        m, "MooncakeBackendOptions")
        .def(py::init<at::Tensor>(), py::arg("active_ranks"))
        .def(py::init<at::Tensor, bool>(), py::arg("active_ranks"),
             py::arg("is_extension"))
        .def(py::init<at::Tensor, bool, int>(), py::arg("active_ranks"),
             py::arg("is_extension"), py::arg("max_world_size"))
        .def(py::init<at::Tensor, bool, int, bool>(), py::arg("active_ranks"),
             py::arg("is_extension"), py::arg("max_world_size"),
             py::arg("auto_deactivate_on_failure"));
}

}  // namespace mooncake
