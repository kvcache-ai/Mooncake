if(MOONCAKE_EP_USE_MUSA)
    set(EP_KERNEL_SOURCE mooncake_ep_kernel.mu)
else()
    set(EP_KERNEL_SOURCE mooncake_ep_kernel.cu)
endif()

# EP only compiles its own sources — device transport comes from transfer_engine
add_library(mooncake_ep ep_py.cpp mooncake_ep_buffer.cpp ${EP_KERNEL_SOURCE})

set_target_properties(mooncake_ep PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(mooncake_ep PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include
)

if(MOONCAKE_EP_USE_MUSA)
    target_compile_definitions(mooncake_ep PRIVATE USE_MUSA MOONCAKE_EP_USE_MUSA=1)
    target_link_libraries(mooncake_ep ${MUSA_LIBRARIES} ${TORCH_LIBRARIES} transfer_engine)
else()
    target_link_libraries(mooncake_ep PUBLIC ${TORCH_LIBRARIES} transfer_engine ibverbs mlx5)
endif()
