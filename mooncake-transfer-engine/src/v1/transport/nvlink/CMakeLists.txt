if (CUDAToolkit_FOUND)
    file(GLOB TEV1_XPORT_NVLINK_SOURCES "*.cpp")

    add_library(transport_nvlink_obj OBJECT ${TEV1_XPORT_NVLINK_SOURCES})

    add_library(transport_nvlink_static STATIC $<TARGET_OBJECTS:transport_nvlink_obj>)
    target_link_libraries(transport_nvlink_static PUBLIC CUDA::cuda_driver CUDA::cudart platform_cuda_static)

    add_library(transport_nvlink_shared SHARED $<TARGET_OBJECTS:transport_nvlink_obj>)
    target_link_libraries(transport_nvlink_shared PUBLIC CUDA::cuda_driver CUDA::cudart platform_cuda_shared)

    install(TARGETS transport_nvlink_static transport_nvlink_shared
        ARCHIVE DESTINATION lib/transport
        LIBRARY DESTINATION lib/transport
        RUNTIME DESTINATION bin)
endif()