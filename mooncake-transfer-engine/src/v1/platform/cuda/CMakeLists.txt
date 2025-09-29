find_package(CUDAToolkit)
if (CUDAToolkit_FOUND)
    message(STATUS "Found CUDA toolkit at ${CUDAToolkit_INCLUDE_DIRS}")
    file(GLOB TENT_PLATFORM_CUDA_SOURCES "*.cpp")
    add_library(platform_cuda_static STATIC ${TENT_PLATFORM_CUDA_SOURCES})
    target_link_libraries(platform_cuda_static PUBLIC tent_common
        CUDA::cudart CUDA::cuda_driver CUDA::nvrtc)
    
    add_library(platform_cuda_shared SHARED ${TENT_PLATFORM_CUDA_SOURCES})
    target_link_libraries(platform_cuda_shared PUBLIC tent_common
        CUDA::cudart CUDA::cuda_driver CUDA::nvrtc)
    add_definitions(-DUSE_CUDA)

    install(TARGETS platform_cuda_static platform_cuda_shared
            ARCHIVE DESTINATION lib
            LIBRARY DESTINATION lib
            RUNTIME DESTINATION bin)
else()
    message(STATUS "CUDA not found, skip cuda platform")
endif()
