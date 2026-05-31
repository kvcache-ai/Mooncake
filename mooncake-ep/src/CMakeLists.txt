set(EP_TRANSPORT_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/src/transport/device/p2p_device_transport.cpp
)
if(MOONCAKE_EP_USE_MUSA)
    list(APPEND EP_TRANSPORT_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/src/transport/device/ibgda_device_transport_musa_stub.cpp
    )
else()
    list(APPEND EP_TRANSPORT_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/src/transport/device/ibgda_device_transport.cpp
    )
endif()

if(MOONCAKE_EP_USE_MUSA)
    set(EP_IBGDA_SOURCES "")
    set(EP_KERNEL_SOURCE mooncake_ep_kernel.mu)
else()
    set(EP_IBGDA_SOURCES mooncake_ibgda/mlx5gda.cpp)
    set(EP_KERNEL_SOURCE mooncake_ep_kernel.cu)
endif()

add_library(mooncake_ep ep_py.cpp mooncake_ep_buffer.cpp ${EP_KERNEL_SOURCE}
            ${EP_IBGDA_SOURCES} ${EP_TRANSPORT_SOURCES})

set_target_properties(mooncake_ep PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(mooncake_ep PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include
)

if(MOONCAKE_EP_USE_MUSA)
    target_compile_definitions(mooncake_ep PRIVATE USE_MUSA MOONCAKE_EP_USE_MUSA=1)
    target_link_libraries(mooncake_ep PUBLIC ${TORCH_LIBRARIES} transfer_engine)
else()
    target_link_libraries(mooncake_ep PUBLIC ${TORCH_LIBRARIES} transfer_engine ibverbs mlx5)
endif()
