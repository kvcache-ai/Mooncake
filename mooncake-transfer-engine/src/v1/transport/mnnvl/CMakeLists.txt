if (CUDAToolkit_FOUND)
    file(GLOB TEV1_XPORT_MNNVL_SOURCES "*.cpp")

    add_library(transport_mnnvl_static STATIC ${TEV1_XPORT_MNNVL_SOURCES})
    target_link_libraries(transport_mnnvl_static PUBLIC CUDA::cuda_driver CUDA::cudart)

    add_library(transport_mnnvl_shared SHARED ${TEV1_XPORT_MNNVL_SOURCES})
    target_link_libraries(transport_mnnvl_shared PUBLIC CUDA::cuda_driver CUDA::cudart)
    target_compile_definitions(transport_mnnvl_shared PUBLIC WITH_PLUGIN_HOOK)

    install(TARGETS transport_mnnvl_static transport_mnnvl_shared
        ARCHIVE DESTINATION lib/transport
        LIBRARY DESTINATION lib/transport
        RUNTIME DESTINATION bin)
endif()