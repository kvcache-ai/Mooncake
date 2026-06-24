add_library(mooncake_pg
    control_plane/link_manager.cpp
    control_plane/coordinator.cpp
    control_plane/coordinator_host.cpp
    control_plane/agent.cpp
    control_plane/agent_host.cpp
    mooncake_backend.cpp
    mooncake_worker.cu
    mooncake_worker_host.cpp
    mooncake_worker_thread.cpp
    p2p_proxy.cpp
    pg_py.cpp
    work_handles.cpp
)

set_target_properties(mooncake_pg PROPERTIES POSITION_INDEPENDENT_CODE ON)

# coro_rpc dependency (yalantinglibs) — required by Coordinator/Agent RPC.
# Transfer Engine already depends on yalantinglibs; the include path and
# library are available via the transfer_engine target.
find_package(ylt QUIET)
if(ylt_FOUND)
    target_link_libraries(mooncake_pg PUBLIC ${TORCH_LIBRARIES} transfer_engine ibverbs mlx5 ylt::coro_rpc)
else()
    target_link_libraries(mooncake_pg PUBLIC ${TORCH_LIBRARIES} transfer_engine ibverbs mlx5)
    message(WARNING "yalantinglibs (ylt) not found via find_package. "
                    "coro_rpc must be available through transfer_engine or "
                    "the include path must be set manually.")
endif()
