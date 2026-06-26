add_library(mooncake_ep ep_py.cpp mooncake_ep_buffer.cpp mooncake_ep_kernel.cu)

set_target_properties(mooncake_ep PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(mooncake_ep PUBLIC ${TORCH_LIBRARIES} transfer_engine ibverbs mlx5)
