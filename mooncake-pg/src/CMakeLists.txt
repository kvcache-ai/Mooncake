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
target_link_libraries(mooncake_pg PUBLIC ${TORCH_LIBRARIES} transfer_engine ibverbs mlx5)
