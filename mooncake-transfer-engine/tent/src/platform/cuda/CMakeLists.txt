if (CUDAToolkit_FOUND)
    file(GLOB TENT_PLATFORM_CUDA_SOURCES "*.cpp")
    add_library(platform_cuda STATIC ${TENT_PLATFORM_CUDA_SOURCES})
    target_link_libraries(platform_cuda PUBLIC tent_common
        CUDA::cudart CUDA::cuda_driver)
endif()
