set(MOONCAKE_EP_SOURCES ep_py.cpp mooncake_ep_buffer.cpp mooncake_ep_kernel.cu)
if(NOT USE_TENT)
  list(APPEND MOONCAKE_EP_SOURCES mooncake_ibgda/mlx5gda.cpp)
endif()

add_library(mooncake_ep ${MOONCAKE_EP_SOURCES})

set_target_properties(mooncake_ep PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(mooncake_ep PUBLIC ${TORCH_LIBRARIES} transfer_engine ibverbs mlx5)
