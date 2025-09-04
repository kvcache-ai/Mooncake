add_library(mooncake_ep mooncake_backend.cpp mooncake_ep_buffer.cpp mooncake_ep_kernel.cu mooncake_worker.cu mooncake_worker_thread.cpp mooncake_ibgda/mlx5gda.cpp)

set_target_properties(mooncake_ep PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(mooncake_ep PUBLIC ${TORCH_LIBRARIES} transfer_engine ibverbs mlx5)
