if (CUDAToolkit_FOUND)
    file(GLOB TENT_PLATFORM_CUDA_SOURCES "*.cpp")
    add_library(platform_cuda_static STATIC ${TENT_PLATFORM_CUDA_SOURCES})
    target_link_libraries(platform_cuda_static PUBLIC tent_common
        CUDA::cudart CUDA::cuda_driver CUDA::nvrtc)
    
    add_library(platform_cuda_shared SHARED ${TENT_PLATFORM_CUDA_SOURCES})
    target_link_libraries(platform_cuda_shared PUBLIC tent_common
        CUDA::cudart CUDA::cuda_driver CUDA::nvrtc)

    install(TARGETS platform_cuda_static platform_cuda_shared
            ARCHIVE DESTINATION lib/platform
            LIBRARY DESTINATION lib/platform
            RUNTIME DESTINATION bin)
endif()
