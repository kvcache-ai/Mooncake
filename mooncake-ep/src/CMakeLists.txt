if(USE_MUSA)
  set(MOONCAKE_EP_KERNEL_SRC mooncake_ep_kernel.mu)
else()
  set(MOONCAKE_EP_KERNEL_SRC mooncake_ep_kernel.cu)
endif()

set(MOONCAKE_EP_SOURCES ep_py.cpp mooncake_ep_buffer.cpp ${MOONCAKE_EP_KERNEL_SRC})
if(NOT USE_TENT)
  list(APPEND MOONCAKE_EP_SOURCES
       ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/tent/src/transport/ibgda/detail/mlx5gda.cpp)
endif()

if(USE_MUSA)
  list(APPEND MOONCAKE_EP_SOURCES
       ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/tent/src/transport/mtlink/mtlink_device.cpp
       ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/tent/src/common/status.cpp)
endif()

add_library(mooncake_ep ${MOONCAKE_EP_SOURCES})

set_target_properties(mooncake_ep PROPERTIES POSITION_INDEPENDENT_CODE ON)

if(USE_MUSA)
  target_link_libraries(mooncake_ep PUBLIC ${TORCH_LIBRARIES} musart)
else()
  target_link_libraries(mooncake_ep PUBLIC ${TORCH_LIBRARIES} transfer_engine ibverbs mlx5)
endif()
