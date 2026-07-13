if(USE_TPU)
    file(GLOB TENT_PLATFORM_TPU_SOURCES "*.cpp")
    add_library(platform_tpu STATIC ${TENT_PLATFORM_TPU_SOURCES})
    # No PJRT / TPU SDK link dependency: the adapter is resolved at runtime via
    # dlopen() in TpuPjrtShim, so we only need libdl.
    target_link_libraries(platform_tpu PUBLIC tent_common ${CMAKE_DL_LIBS})
endif()
