if(USE_MUSA)
  set(MOONCAKE_EP_KERNEL_SRC mooncake_ep_kernel.mu)
else()
  set(MOONCAKE_EP_KERNEL_SRC mooncake_ep_kernel.cu)
endif()

set(MOONCAKE_EP_SOURCES ep_py.cpp mooncake_ep_buffer.cpp ${MOONCAKE_EP_KERNEL_SRC})

# All builds now use TENT DeviceTransport — include the transport sources.
if(USE_MUSA)
  list(APPEND MOONCAKE_EP_SOURCES
       ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/tent/src/transport/mtlink/mtlink_device.cpp
       ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/tent/src/common/status.cpp)
else()
  list(APPEND MOONCAKE_EP_SOURCES
       ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/tent/src/transport/nvlink/nvlink_device.cpp
       ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/tent/src/transport/ibgda/ibgda_device.cpp
       ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/tent/src/common/status.cpp)
endif()

add_library(mooncake_ep ${MOONCAKE_EP_SOURCES})

set_target_properties(mooncake_ep PROPERTIES POSITION_INDEPENDENT_CODE ON)

if(USE_MUSA)
  target_link_libraries(mooncake_ep PUBLIC ${TORCH_LIBRARIES} musart glog)
else()
  target_link_libraries(mooncake_ep PUBLIC ${TORCH_LIBRARIES} transfer_engine ibverbs mlx5)
endif()
