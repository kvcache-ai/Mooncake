if(USE_TPU)
    file(GLOB XPORT_SOURCES "*.cpp")
    add_library(tent_xport_tpu STATIC ${XPORT_SOURCES})
    # platform_tpu provides TpuPjrtShim, which the transport consults to
    # verify that a staging copy really has a TPU-device side.
    target_link_libraries(tent_xport_tpu PUBLIC tent_rpc tent_common
                                                platform_tpu)
endif()
