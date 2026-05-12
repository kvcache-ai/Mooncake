# RDMA Transport - Platform-specific builds

file(GLOB XPORT_SOURCES "*.cpp")

# Include RDMA headers
find_path(IBVERBS_INCLUDE infiniband/verbs.h PATHS /usr/include /usr/local/include)
find_library(IBVERBS_LIB ibverbs PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)

if (IBVERBS_INCLUDE AND IBVERBS_LIB)
    message(STATUS "RDMA: Enabled")

    if(TENT_USE_PLUGINS)
        # Dynamic mode: Build only plugins, no static libraries
        message(STATUS "  RDMA: Plugin mode (platform-specific .so files)")

        # Set output directory for plugins
        set(PLUGIN_OUTPUT_DIR "${CMAKE_BINARY_DIR}/lib/tent/transport")

        # CPU variant (platform-independent)
        add_library(tent_rdma_cpu SHARED ${XPORT_SOURCES})
        target_compile_definitions(tent_rdma_cpu PRIVATE TENT_BUILD_PLUGIN USE_RDMA)
        target_include_directories(tent_rdma_cpu PUBLIC ${IBVERBS_INCLUDE})
        target_link_libraries(tent_rdma_cpu PUBLIC ${IBVERBS_LIB} tent_platform tent_common numa)
        set_target_properties(tent_rdma_cpu PROPERTIES
            OUTPUT_NAME "tent_rdma_cpu"
            LIBRARY_OUTPUT_DIRECTORY "${PLUGIN_OUTPUT_DIR}"
        )
        install(TARGETS tent_rdma_cpu LIBRARY DESTINATION lib/tent/transport)

        # CUDA variant
        if (CUDAToolkit_FOUND)
            add_library(tent_rdma_cuda SHARED ${XPORT_SOURCES})
            target_compile_definitions(tent_rdma_cuda PRIVATE TENT_BUILD_PLUGIN USE_RDMA USE_CUDA)
            target_include_directories(tent_rdma_cuda PUBLIC ${IBVERBS_INCLUDE})
            target_link_libraries(tent_rdma_cuda PUBLIC ${IBVERBS_LIB} tent_platform tent_common CUDA::cudart)
            set_target_properties(tent_rdma_cuda PROPERTIES
                OUTPUT_NAME "tent_rdma_cuda"
                LIBRARY_OUTPUT_DIRECTORY "${PLUGIN_OUTPUT_DIR}"
            )
            install(TARGETS tent_rdma_cuda LIBRARY DESTINATION lib/tent/transport)
            message(STATUS "  RDMA: CUDA variant enabled")
        endif()

        # Ascend variant
        if (USE_ASCEND OR USE_ASCEND_DIRECT)
            add_library(tent_rdma_ascend SHARED ${XPORT_SOURCES})
            target_compile_definitions(tent_rdma_ascend PRIVATE TENT_BUILD_PLUGIN USE_RDMA USE_ASCEND)
            target_include_directories(tent_rdma_ascend PUBLIC ${IBVERBS_INCLUDE})
            target_link_libraries(tent_rdma_ascend PUBLIC ${IBVERBS_LIB} tent_platform tent_common ascendcl)
            set_target_properties(tent_rdma_ascend PROPERTIES
                OUTPUT_NAME "tent_rdma_ascend"
                LIBRARY_OUTPUT_DIRECTORY "${PLUGIN_OUTPUT_DIR}"
            )
            install(TARGETS tent_rdma_ascend LIBRARY DESTINATION lib/tent/transport)
            message(STATUS "  RDMA: Ascend variant enabled")
        endif()

        # MUSA variant
        if (USE_MUSA)
            add_library(tent_rdma_musa SHARED ${XPORT_SOURCES})
            target_compile_definitions(tent_rdma_musa PRIVATE TENT_BUILD_PLUGIN USE_RDMA USE_MUSA)
            target_include_directories(tent_rdma_musa PUBLIC ${IBVERBS_INCLUDE})
            target_link_libraries(tent_rdma_musa PUBLIC ${IBVERBS_LIB} tent_platform tent_common musart)
            set_target_properties(tent_rdma_musa PROPERTIES
                OUTPUT_NAME "tent_rdma_musa"
                LIBRARY_OUTPUT_DIRECTORY "${PLUGIN_OUTPUT_DIR}"
            )
            install(TARGETS tent_rdma_musa LIBRARY DESTINATION lib/tent/transport)
            message(STATUS "  RDMA: MUSA variant enabled")
        endif()

        # HIP variant
        if (USE_HIP)
            add_library(tent_rdma_hip SHARED ${XPORT_SOURCES})
            target_compile_definitions(tent_rdma_hip PRIVATE TENT_BUILD_PLUGIN USE_RDMA USE_HIP)
            target_include_directories(tent_rdma_hip PUBLIC ${IBVERBS_INCLUDE})
            target_link_libraries(tent_rdma_hip PUBLIC ${IBVERBS_LIB} tent_platform tent_common amdhip64)
            set_target_properties(tent_rdma_hip PROPERTIES
                OUTPUT_NAME "tent_rdma_hip"
                LIBRARY_OUTPUT_DIRECTORY "${PLUGIN_OUTPUT_DIR}"
            )
            install(TARGETS tent_rdma_hip LIBRARY DESTINATION lib/tent/transport)
            message(STATUS "  RDMA: HIP variant enabled")
        endif()

    else()
        # Static mode: Build static library with current platform
        message(STATUS "  RDMA: Static mode (current platform only)")
        add_library(tent_xport_rdma STATIC ${XPORT_SOURCES})
        target_include_directories(tent_xport_rdma PUBLIC ${IBVERBS_INCLUDE})
        target_link_libraries(tent_xport_rdma PUBLIC ${IBVERBS_LIB} tent_platform tent_common)

        # Link platform libraries in static mode
        if (CUDAToolkit_FOUND)
            target_link_libraries(tent_xport_rdma PUBLIC CUDA::cudart)
            target_compile_definitions(tent_xport_rdma PUBLIC USE_CUDA)
        endif()
        if (USE_ASCEND OR USE_ASCEND_DIRECT)
            target_link_libraries(tent_xport_rdma PUBLIC ascendcl)
            target_compile_definitions(tent_xport_rdma PUBLIC USE_ASCEND)
        endif()
        if (USE_MUSA)
            target_link_libraries(tent_xport_rdma PUBLIC musart)
            target_compile_definitions(tent_xport_rdma PUBLIC USE_MUSA)
        endif()
        if (USE_HIP)
            target_link_libraries(tent_xport_rdma PUBLIC amdhip64)
            target_compile_definitions(tent_xport_rdma PUBLIC USE_HIP)
        endif()
    endif()

else()
    message(STATUS "RDMA: Disabled (libibverbs not found)")
endif()
