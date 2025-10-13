if (CUDAToolkit_FOUND)
    file(GLOB TEV1_XPORT_NVLINK_SOURCES "*.cpp")

    add_library(transport_nvlink_static STATIC ${TEV1_XPORT_NVLINK_SOURCES})
    target_link_libraries(transport_nvlink_static PUBLIC CUDA::cuda_driver CUDA::cudart platform_cuda_static)

    add_library(transport_nvlink_shared SHARED ${TEV1_XPORT_NVLINK_SOURCES})
    target_link_libraries(transport_nvlink_shared PUBLIC CUDA::cuda_driver CUDA::cudart platform_cuda_shared)
    target_compile_definitions(transport_nvlink_shared PUBLIC WITH_PLUGIN_HOOK)

    install(TARGETS transport_nvlink_static transport_nvlink_shared
        ARCHIVE DESTINATION lib/transport
        LIBRARY DESTINATION lib/transport
        RUNTIME DESTINATION bin)
endif()