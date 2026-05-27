# NVLink Transport - Platform-specific builds
# Builds separate .so for each available GPU platform

file(GLOB XPORT_SOURCES "*.cpp")

# Build static library for current platform (for static linking)
if (CUDAToolkit_FOUND)
    add_library(tent_xport_nvlink STATIC ${XPORT_SOURCES})
    target_link_libraries(tent_xport_nvlink PUBLIC CUDA::cuda_driver CUDA::cudart tent_platform tent_common)
    target_compile_definitions(tent_xport_nvlink PRIVATE USE_CUDA)
endif()

# Build platform-specific plugin shared libraries
option(TENT_BUILD_TRANSPORT_PLUGINS "Build transport plugins as platform-specific .so files" ON)

if (TENT_BUILD_TRANSPORT_PLUGINS)
    # CUDA variant
    if (CUDAToolkit_FOUND)
        add_library(tent_nvlink_cuda SHARED ${XPORT_SOURCES})
        target_compile_definitions(tent_nvlink_cuda PRIVATE TENT_BUILD_PLUGIN USE_CUDA)
        target_link_libraries(tent_nvlink_cuda PUBLIC tent_platform CUDA::cuda_driver CUDA::cudart tent_common)
        set_target_properties(tent_nvlink_cuda PROPERTIES
            OUTPUT_NAME "tent_nvlink_cuda"
        )
        install(TARGETS tent_nvlink_cuda
            LIBRARY DESTINATION lib/tent/transport
            RUNTIME DESTINATION bin
        )
        message(STATUS "Transport plugin: nvlink_cuda (CUDA)")
    endif()

    # MUSA variant (Moore Threads)
    if (USE_MUSA)
        add_library(tent_nvlink_musa SHARED ${XPORT_SOURCES})
        target_compile_definitions(tent_nvlink_musa PRIVATE TENT_BUILD_PLUGIN USE_MUSA)
        target_link_libraries(tent_nvlink_musa PUBLIC tent_platform musa musart tent_common)
        set_target_properties(tent_nvlink_musa PROPERTIES
            VERSION ${PROJECT_VERSION}
            SOVERSION 0
            OUTPUT_NAME "tent_nvlink_musa"
        )
        install(TARGETS tent_nvlink_musa
            LIBRARY DESTINATION lib/tent/transport
            RUNTIME DESTINATION bin
        )
        message(STATUS "Transport plugin: nvlink_musa (Moore Threads)")
    endif()

    # HIP variant (AMD)
    if (USE_HIP)
        add_library(tent_nvlink_hip SHARED ${XPORT_SOURCES})
        target_compile_definitions(tent_nvlink_hip PRIVATE TENT_BUILD_PLUGIN USE_HIP)
        target_link_libraries(tent_nvlink_hip PUBLIC tent_platform amdhip64 tent_common)
        set_target_properties(tent_nvlink_hip PROPERTIES
            VERSION ${PROJECT_VERSION}
            SOVERSION 0
            OUTPUT_NAME "tent_nvlink_hip"
        )
        install(TARGETS tent_nvlink_hip
            LIBRARY DESTINATION lib/tent/transport
            RUNTIME DESTINATION bin
        )
        message(STATUS "Transport plugin: nvlink_hip (AMD HIP)")
    endif()

    # Ascend variant (Huawei)
    if (USE_ASCEND OR USE_ASCEND_DIRECT)
        add_library(tent_nvlink_ascend SHARED ${XPORT_SOURCES})
        target_compile_definitions(tent_nvlink_ascend PRIVATE TENT_BUILD_PLUGIN USE_ASCEND)
        target_link_libraries(tent_nvlink_ascend PUBLIC tent_platform ascendcl tent_common)
        set_target_properties(tent_nvlink_ascend PROPERTIES
            VERSION ${PROJECT_VERSION}
            SOVERSION 0
            OUTPUT_NAME "tent_nvlink_ascend"
        )
        install(TARGETS tent_nvlink_ascend
            LIBRARY DESTINATION lib/tent/transport
            RUNTIME DESTINATION bin
        )
        message(STATUS "Transport plugin: nvlink_ascend (Huawei Ascend)")
    endif()
endif()
