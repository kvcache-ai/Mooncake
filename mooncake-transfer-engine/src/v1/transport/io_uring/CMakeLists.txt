if (URING_LIB AND URING_INCLUDE)
    file(GLOB TEV1_XPORT_URING_SOURCES "*.cpp")

    add_library(transport_uring_static STATIC ${TEV1_XPORT_URING_SOURCES})
    target_include_directories(transport_uring_static PUBLIC ${URING_INCLUDE})
    target_link_libraries(transport_uring_static PUBLIC ${URING_LIB})

    add_library(transport_uring_shared SHARED ${TEV1_XPORT_URING_SOURCES})
    target_include_directories(transport_uring_shared PUBLIC ${URING_INCLUDE})
    target_link_libraries(transport_uring_shared PUBLIC ${URING_LIB})
    target_compile_definitions(transport_uring_shared PUBLIC WITH_PLUGIN_HOOK)

    if (CUDAToolkit_FOUND)
        target_link_libraries(transport_uring_static PUBLIC 
            CUDA::cudart CUDA::cuda_driver CUDA::nvrtc)
        target_link_libraries(transport_uring_shared PUBLIC 
            CUDA::cudart CUDA::cuda_driver CUDA::nvrtc)
    endif()
    
    install(TARGETS transport_uring_static transport_uring_shared
        ARCHIVE DESTINATION lib/transport
        LIBRARY DESTINATION lib/transport
        RUNTIME DESTINATION bin)
endif()
