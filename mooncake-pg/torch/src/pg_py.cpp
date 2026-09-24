#include <mooncake_backend.h>
#include <torch_utils.h>
#include <pybind11/gil.h>
#include <pybind11/stl.h>
#include <torch/csrc/utils/pybind.h>
#include <torch/python.h>
#include <torch/torch.h>

#include <algorithm>
#include <array>
#include <mutex>
#include <string>
#include <vector>

namespace py = pybind11;

namespace mooncake {

constexpr const char* kCoordinatorStoreKey = "coordinator_addr";
constexpr size_t kCoordinatorAddressBufSize = 256;

struct MooncakeProcessContext {
    mooncakePgContext_t handle = nullptr;
    c10::intrusive_ptr<c10d::Store> bootstrap_store;

    ~MooncakeProcessContext() {
        if (handle) (void)mooncakePgContextDestroy(handle);
    }
};

static MooncakeProcessContext g_ctx;
static std::once_flag g_create_context_once;
static std::once_flag g_init_control_plane_once;

mooncakePgContext_t getContext() {
    std::call_once(g_create_context_once, [] {
        mooncakePgContext_t context = nullptr;
        checkResult(mooncakePgContextCreate(&context),
                    "mooncakePgContextCreate");
        g_ctx.handle = context;
    });
    return g_ctx.handle;
}

static mooncakePgContext_t initControlPlane(
    const c10::intrusive_ptr<c10d::Store>& store, int rank,
    int max_world_size) {
    auto context = getContext();
    std::call_once(g_init_control_plane_once, [&] {
        // Ordering constraint: AgentHost::start() sends registerAgent
        // immediately, which includes LinkManager's localServerName() and
        // getWarmupRecvAddr().  These must be non-empty, so the engine and
        // LinkManager must be initialized BEFORE AgentHost starts.
        checkResult(mooncakePgContextInitialize(context, rank, max_world_size),
                    "mooncakePgContextInitialize");

        // Rank 0 hosts the Coordinator in-process.
        if (rank == 0) {
            std::array<char, kCoordinatorAddressBufSize>
                coordinator_address_buf{};
            checkResult(mooncakePgContextLaunchCoordinator(
                            context, coordinator_address_buf.data(),
                            coordinator_address_buf.size()),
                        "mooncakePgContextLaunchCoordinator");
            store->set(kCoordinatorStoreKey,
                       std::string(coordinator_address_buf.data()));
        }

        store->wait({kCoordinatorStoreKey});
        const std::string value = store->get_to_str(kCoordinatorStoreKey);
        TORCH_CHECK(!value.empty(),
                    "invalid Mooncake coordinator address in Store");
        checkResult(mooncakePgContextConnectCoordinator(context, value.c_str()),
                    "mooncakePgContextConnectCoordinator");

        // Keep the first rendezvous Store alive for the process-wide control
        // plane. In particular, this keeps rank 0's TCPStore server alive while
        // the default ProcessGroup is destroyed and re-created.
        g_ctx.bootstrap_store = store;
    });
    return context;
}

c10::intrusive_ptr<c10d::ProcessGroup> createMooncakeBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>
        backendOptions) {
    int rank = distBackendOpts.group_rank;
    auto context =
        initControlPlane(distBackendOpts.store, rank, MOONCAKE_PG_MAX_RANKS);
    auto backend = c10::make_intrusive<MooncakeBackend>(
        std::move(distBackendOpts), std::move(backendOptions), context);
    return backend;
}

c10::intrusive_ptr<c10d::ProcessGroup> createMooncakeCpuBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>
        backendOptions) {
    int rank = distBackendOpts.group_rank;
    auto context =
        initControlPlane(distBackendOpts.store, rank, MOONCAKE_PG_MAX_RANKS);
    auto backend = c10::make_intrusive<MooncakeBackend>(
        std::move(distBackendOpts), std::move(backendOptions), context, true);
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

mooncakePgProposalResponse_t recoverRanks(
    c10::intrusive_ptr<c10d::ProcessGroup> backend,
    const std::vector<int>& ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->activateRanks(ranks);
}

mooncakePgProposalResponse_t deactivateRanks(
    c10::intrusive_ptr<c10d::ProcessGroup> backend,
    const std::vector<int>& ranks) {
    auto mooncakeBackend =
        c10::static_intrusive_pointer_cast<MooncakeBackend>(backend);
    return mooncakeBackend->deactivateRanks(ranks);
}

mooncakePgProposalResponse_t activateRanks(
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
        checkResult(mooncakePgContextSetTransferEngine(getContext(), nullptr),
                    "mooncakePgContextSetTransferEngine");
        return;
    }
    auto get_engine_ptr = engine_obj.attr("get_engine_ptr");
    uintptr_t ptr = get_engine_ptr().cast<uintptr_t>();
    checkResult(mooncakePgContextSetTransferEngine(
                    getContext(), reinterpret_cast<void*>(ptr)),
                "mooncakePgContextSetTransferEngine");
}

std::vector<int> droppedRanks(const mooncakePgProposalResponse_t& response) {
    const size_t count = std::min(response.droppedRankCount,
                                  static_cast<size_t>(MOONCAKE_PG_MAX_RANKS));
    return std::vector<int>(response.droppedRanks,
                            response.droppedRanks + count);
}

void shutdownProcessContext() {
    auto context = g_ctx.handle;
    if (!context) return;
    // ContextDestroy rejects a parent-before-child teardown while a
    // communicator is still alive. Keep the handle for the static-destructor
    // fallback instead of losing ownership.
    if (mooncakePgContextDestroy(context) == mooncakePgSuccess) {
        g_ctx.handle = nullptr;
        g_ctx.bootstrap_store.reset();
    }
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    // Python atexit handlers run while module globals still own an injected
    // TransferEngine. Py_AtExit is too late: CPython may have already decref'd
    // the Python TE wrapper before invoking its native exit handlers.
    py::module_::import("atexit").attr("register")(
        py::cpp_function(&shutdownProcessContext));
    m.def("createMooncakeBackend", &createMooncakeBackend);
    m.def("createMooncakeCpuBackend", &createMooncakeCpuBackend);
    m.def("set_host_ip", [](const std::string& host) {
        checkResult(mooncakePgContextSetHostIp(getContext(), host.c_str()),
                    "mooncakePgContextSetHostIp");
    });
    m.def(
        "set_collective_timeout_us",
        [](size_t us) {
            checkResult(mooncakePgContextSetCollectiveTimeout(getContext(), us),
                        "mooncakePgContextSetCollectiveTimeout");
        },
        py::arg("us"),
        "Set the default peer-liveness probe timeout (microseconds) for "
        "collective operations.");
    m.def(
        "set_p2p_timeout_us",
        [](int64_t us) {
            checkResult(mooncakePgContextSetP2PTimeout(getContext(), us),
                        "mooncakePgContextSetP2PTimeout");
        },
        py::arg("us"), "Set the default P2P transfer timeout (microseconds).");
    m.def(
        "set_fault_reconciliation_window_us",
        [](int64_t us) {
            checkResult(
                mooncakePgContextSetFaultReconciliationWindow(getContext(), us),
                "mooncakePgContextSetFaultReconciliationWindow");
        },
        py::arg("us"),
        "Set the coordinator fault reconciliation window (microseconds).");
    m.def("set_device_filter", [](std::vector<std::string> filters) {
        std::vector<const char*> filter_pointers;
        filter_pointers.reserve(filters.size());
        for (const auto& filter : filters) {
            filter_pointers.push_back(filter.c_str());
        }
        checkResult(
            mooncakePgContextSetDeviceFilter(
                getContext(), filter_pointers.data(), filter_pointers.size()),
            "mooncakePgContextSetDeviceFilter");
    });
    m.def("set_transfer_engine", &setTransferEnginePy, py::arg("engine"),
          "Set an external TransferEngine to be used by MooncakeBackend. "
          "Must be called before init_process_group(). The engine must already "
          "be initialized. Pass None to reset to default behavior. "
          "The caller must ensure the TransferEngine object outlives all "
          "MooncakeBackend instances.");
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

    py::enum_<mooncakePgSyncAfterFailureStatus_t>(m, "SyncAfterFailureStatus")
        .value("Reconciled", mooncakePgSyncReconciled)
        .value("NoPending", mooncakePgSyncNoPending)
        .value("Rejected", mooncakePgSyncRejected);

    auto proposal_status =
        py::enum_<mooncakePgProposalStatus_t>(m, "ProposalStatus")
            .value("Rejected", mooncakePgProposalRejected)
            .value("Applied", mooncakePgProposalApplied)
            .value("AppliedWithDroppedRanks",
                   mooncakePgProposalAppliedWithDroppedRanks);
    // Keep existing Python callers source-compatible with the renamed enum.
    m.attr("ViewUpdateStatus") = proposal_status;

    py::class_<mooncakePgSyncAfterFailureResponse_t>(m,
                                                     "SyncAfterFailureResponse")
        .def_property_readonly(
            "status",
            [](const mooncakePgSyncAfterFailureResponse_t& value) {
                return value.status;
            })
        .def_property_readonly(
            "reject_reason",
            [](const mooncakePgSyncAfterFailureResponse_t& value) {
                return std::string(value.rejectReason);
            });

    py::class_<mooncakePgProposalResponse_t>(m, "ProposeViewUpdateResponse")
        .def_property_readonly("status",
                               [](const mooncakePgProposalResponse_t& value) {
                                   return value.status;
                               })
        .def_property_readonly("new_epoch",
                               [](const mooncakePgProposalResponse_t& value) {
                                   return value.newEpoch;
                               })
        .def_property_readonly("dropped_ranks", &droppedRanks)
        .def_property_readonly("reject_reason",
                               [](const mooncakePgProposalResponse_t& value) {
                                   return std::string(value.rejectReason);
                               });

    py::class_<MooncakeBackend::MooncakeBackendOptions,
               c10::intrusive_ptr<MooncakeBackend::MooncakeBackendOptions>>(
        m, "MooncakeBackendOptions")
        // IMPORTANT: these constructors with tensor MUST be registered
        // before the (int, ...) constructors.  Otherwise, when a 1-element
        // Tensor is passed, pybind11 implicitly converts it to int and
        // resolves to the wrong overload:
        // e.g. MooncakeBackendOptions(tensor([1]), False) ->
        //      MooncakeBackendOptions(int maxGroupSize=1,
        //                            bool isExtension=False)
        // instead of the intended Tensor-based path.
        .def(py::init<at::Tensor>(), py::arg("active_ranks"))
        .def(py::init<at::Tensor, bool>(), py::arg("active_ranks"),
             py::arg("is_extension"))
        .def(py::init<at::Tensor, bool, int>(), py::arg("active_ranks"),
             py::arg("is_extension"), py::arg("max_group_size"))
        // Recommended constructors
        .def(py::init<int>(), py::arg("max_group_size"))
        .def(py::init<int, bool>(), py::arg("max_group_size"),
             py::arg("is_extension"))
        .def(py::init<int, bool, bool, bool>(), py::arg("max_group_size"),
             py::arg("is_extension"), py::arg("auto_deactivate_on_failure"),
             py::arg("auto_sync_on_failure"));
}

}  // namespace mooncake
