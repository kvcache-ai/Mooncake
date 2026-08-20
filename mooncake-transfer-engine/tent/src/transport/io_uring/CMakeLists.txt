if(TARGET Mooncake::liburing)
    file(GLOB TEV1_XPORT_URING_SOURCES "*.cpp")
    add_library(tent_xport_uring STATIC ${TEV1_XPORT_URING_SOURCES})
    target_link_libraries(tent_xport_uring PUBLIC Mooncake::liburing tent_common)
    if (CUDAToolkit_FOUND)
        target_link_libraries(tent_xport_uring PUBLIC
            CUDA::cudart CUDA::cuda_driver)
    endif()
endif()
