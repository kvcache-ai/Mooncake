add_library(mooncake_pg connection_poller.cpp mooncake_backend.cpp mooncake_worker.cu mooncake_worker_thread.cpp p2p_proxy.cpp pg_py.cpp)

set_target_properties(mooncake_pg PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(mooncake_pg PUBLIC ${TORCH_LIBRARIES} transfer_engine ibverbs mlx5)
