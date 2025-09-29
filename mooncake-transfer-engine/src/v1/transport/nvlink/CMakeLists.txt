find_package(CUDAToolkit)
if (CUDAToolkit_FOUND)
    message(STATUS "CUDA found, enabling NVLink transport")
    add_definitions(-DUSE_CUDA)

    file(GLOB TEV1_XPORT_NVLINK_SOURCES "*.cpp")

    add_library(transport_nvlink_obj OBJECT ${TEV1_XPORT_NVLINK_SOURCES})
    target_include_directories(transport_nvlink_obj PUBLIC ${CUDAToolkit_INCLUDE_DIRS})

    add_library(transport_nvlink_static STATIC $<TARGET_OBJECTS:transport_nvlink_obj>)
    target_link_libraries(transport_nvlink_static PUBLIC CUDA::cuda_driver CUDA::cudart platform_cuda_static)

    add_library(transport_nvlink_shared SHARED $<TARGET_OBJECTS:transport_nvlink_obj>)
    target_link_libraries(transport_nvlink_shared PUBLIC CUDA::cuda_driver CUDA::cudart platform_cuda_shared)
else()
    message(STATUS "CUDA not found, skipping NVLink transport")
endif()