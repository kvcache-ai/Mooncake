#include <mooncake_backend.h>
#include <pybind11/gil.h>
#include <pybind11/stl.h>
#include <torch/csrc/utils/pybind.h>
#include <torch/python.h>
#include <torch/torch.h>

#include <cstdlib>
#include <glog/logging.h>
#include <mutex>

#include "control_plane/agent_host.h"

namespace py = pybind11;

namespace mooncake {

static MooncakeProcessContext g_ctx;

MooncakeProcessContext::MooncakeProcessContext() {
    if (const char* val =
            std::getenv("MOONCAKE_PG_FAULT_RECONCILIATION_WINDOW_US")) {
        try {
            fault_reconciliation_window_us = std::stoll(val);
        } catch (...) {
            LOG(WARNING)
                << "Invalid MOONCAKE_PG_FAULT_RECONCILIATION_WINDOW_US: " << val
                << ", using default " << fault_reconciliation_window_us
                << " us";
        }
    }
}

MooncakeProcessContext::~MooncakeProcessContext() {
    // Shutdown AgentHost first so any queued unregisterGroup RPCs are drained
    // before RpcClient is torn down.
    if (agent_host) agent_host->shutdown();
    // Shutdown CoordinatorHost second so rank 0 fails pending proposals.
    if (coordinator_host) coordinator_host->shutdown();
}

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
        g_ctx.max_world_size = max_world_size;

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
            g_ctx.link_manager.init(rank, max_world_size, g_ctx.engine);
        }

        // Rank 0 hosts the Coordinator in-process.
        if (rank == 0) {
            g_ctx.coordinator_host = std::make_unique<CoordinatorHost>(
                store, g_ctx.host_ip, max_world_size,
                g_ctx.fault_reconciliation_window_us);
            g_ctx.coordinator_host->start();
        }

        g_ctx.agent_host = std::make_unique<AgentHost>(
            store, g_ctx.host_ip, rank, max_world_size, g_ctx.link_manager);
        g_ctx.agent_host->start();
    });
    return *g_ctx.agent_host;
}

c10::intrusive_ptr<c10d::ProcessGroup> createMooncakeBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>
        backendOptions) {
    int rank = distBackendOpts.group_rank;
    auto& host = initControlPlane(distBackendOpts.store, rank, kMaxNumRanks);
    auto backend = c10::make_intrusive<MooncakeBackend>(
        std::move(distBackendOpts), std::move(backendOptions), host, g_ctx);
    return backend;
}

c10::intrusive_ptr<c10d::ProcessGroup> createMooncakeCpuBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>
        backendOptions) {
    int rank = distBackendOpts.group_rank;
    auto& host = initControlPlane(distBackendOpts.store, rank, kMaxNumRanks);
    auto backend = c10::make_intrusive<MooncakeBackend>(
        std::move(distBackendOpts), std::move(backendOptions), host, g_ctx,
        true);
    return backend;
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

ProposeViewUpdateResponse recoverRanks(
    c10::intrusive_ptr<c10d::ProcessGroup> backend,
    const std::vector<int>& ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->recoverRanks(ranks);
}

ProposeViewUpdateResponse deactivateRanks(
    c10::intrusive_ptr<c10d::ProcessGroup> backend,
    const std::vector<int>& ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->deactivateRanks(ranks);
}

ProposeViewUpdateResponse activateRanks(
    c10::intrusive_ptr<c10d::ProcessGroup> backend,
    const std::vector<int>& ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->activateRanks(ranks);
}

void joinGroup(c10::intrusive_ptr<c10d::ProcessGroup> backend) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    mooncakeBackend->joinGroup();
}

at::Tensor getFailedRanksHint(c10::intrusive_ptr<c10d::Work> work) {
    if (auto* w = dynamic_cast<MooncakeWorkCuda*>(work.get())) {
        return w->getFailedRanksHint();
    }
    if (auto* w = dynamic_cast<MooncakeWorkCpu*>(work.get())) {
        return w->getFailedRanksHint();
    }
    if (auto* w = dynamic_cast<MooncakeP2PWork*>(work.get())) {
        return w->getFailedRanksHint();
    }
    return at::Tensor();
}

bool getLocalSuccess(c10::intrusive_ptr<c10d::Work> work) {
    if (auto* w = dynamic_cast<MooncakeWorkCuda*>(work.get())) {
        return w->getLocalSuccess();
    }
    if (auto* w = dynamic_cast<MooncakeWorkCpu*>(work.get())) {
        return w->getLocalSuccess();
    }
    if (auto* w = dynamic_cast<MooncakeP2PWork*>(work.get())) {
        return w->getLocalSuccess();
    }
    return false;
}

int64_t getCurrentEpoch(c10::intrusive_ptr<c10d::ProcessGroup> backend) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return static_cast<int64_t>(mooncakeBackend->getCurrentEpoch());
}

/// Python-facing wrapper that extracts the raw TransferEngine* from a
/// mooncake.engine.TransferEngine Python object and makes it the process-wide
/// engine for all MooncakeBackend instances.  The caller must ensure the
/// TransferEnginePy object outlives all MooncakeBackend instances.
void setTransferEnginePy(pybind11::object engine_obj) {
    if (engine_obj.is_none()) {
        g_ctx.external_engine = nullptr;
        g_ctx.engine = g_ctx.owned_engine.get();
        g_ctx.engine_initialized = false;
        return;
    }
    auto get_engine_ptr = engine_obj.attr("get_engine_ptr");
    uintptr_t ptr = get_engine_ptr().cast<uintptr_t>();
    g_ctx.external_engine = reinterpret_cast<TransferEngine*>(ptr);
    g_ctx.engine = g_ctx.external_engine;
    g_ctx.engine_initialized = true;
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
    m.def(
        "set_fault_reconciliation_window_us",
        [](int64_t us) { g_ctx.fault_reconciliation_window_us = us; },
        py::arg("us"),
        "Set the coordinator fault reconciliation window (microseconds).");
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
    m.def("activate_ranks", &activateRanks);
    m.def("deactivate_ranks", &deactivateRanks, py::arg("backend"),
          py::arg("ranks"));
    m.def("join_group", &joinGroup);
    m.def("get_failed_ranks_hint", &getFailedRanksHint, py::arg("work"));
    m.def("get_local_success", &getLocalSuccess, py::arg("work"),
          "Return True iff all locally-attempted peers succeeded in this "
          "operation.");
    m.def("get_current_epoch", &getCurrentEpoch, py::arg("backend"),
          "Get the current GroupView epoch (monotonically increasing on "
          "membership changes).");

    m.def(
        "sync_after_failure",
        [](c10::intrusive_ptr<c10d::ProcessGroup> backend) {
            auto mooncakeBackend =
                c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
            return mooncakeBackend->syncAfterFailure();
        },
        py::arg("backend"));

    py::enum_<SyncAfterFailureStatus>(m, "SyncAfterFailureStatus")
        .value("Reconciled", SyncAfterFailureStatus::Reconciled)
        .value("NoPending", SyncAfterFailureStatus::NoPending)
        .value("Rejected", SyncAfterFailureStatus::Rejected);

    py::enum_<ViewUpdateStatus>(m, "ViewUpdateStatus")
        .value("Rejected", ViewUpdateStatus::Rejected)
        .value("Applied", ViewUpdateStatus::Applied)
        .value("AppliedWithDroppedRanks",
               ViewUpdateStatus::AppliedWithDroppedRanks);

    py::class_<SyncAfterFailureResponse>(m, "SyncAfterFailureResponse")
        .def_readonly("status", &SyncAfterFailureResponse::status)
        .def_readonly("new_epoch", &SyncAfterFailureResponse::new_epoch)
        .def_readonly("reject_reason",
                      &SyncAfterFailureResponse::reject_reason);

    py::class_<ProposeViewUpdateResponse>(m, "ProposeViewUpdateResponse")
        .def_readonly("status", &ProposeViewUpdateResponse::status)
        .def_readonly("new_epoch", &ProposeViewUpdateResponse::new_epoch)
        .def_readonly("dropped_ranks",
                      &ProposeViewUpdateResponse::dropped_ranks)
        .def_readonly("reject_reason",
                      &ProposeViewUpdateResponse::reject_reason);

    py::class_<MooncakeBackend::MooncakeBackendOptions,
               c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>>(
        m, "MooncakeBackendOptions")
        .def(py::init<at::Tensor>(), py::arg("active_ranks"))
        // Deprecated constructors: isExtension is ignored
        // IMPORTANT: these deprecated constructors MUST be registered
        // before the (int, ...) constructors.  Otherwise, when a 1-element
        // Tensor is passed, pybind11 implicitly converts it to int and
        // resolves to the wrong overload:
        // e.g. MooncakeBackendOptions(tensor([1]), False) ->
        //      MooncakeBackendOptions(int maxGroupSize=1,
        //                            bool autoDeactivateOnFailure=False)
        // instead of the intended Tensor-based path.
        .def(py::init<at::Tensor, bool>(), py::arg("active_ranks"),
             py::arg("is_extension"))
        .def(py::init<at::Tensor, bool, int>(), py::arg("active_ranks"),
             py::arg("is_extension"), py::arg("max_group_size"))
        // Recommended constructors
        .def(py::init<int>(), py::arg("max_group_size"))
        .def(py::init<int, bool, bool>(), py::arg("max_group_size"),
             py::arg("auto_deactivate_on_failure"),
             py::arg("auto_sync_on_failure"));
}

}  // namespace mooncake
