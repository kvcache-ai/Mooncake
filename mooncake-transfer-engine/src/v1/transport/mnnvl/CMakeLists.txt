find_package(CUDAToolkit)
if (CUDAToolkit_FOUND)
    message(STATUS "CUDA found, enabling MultiNode NVLink transport")
    add_definitions(-DUSE_CUDA)

    file(GLOB TEV1_XPORT_MNNVL_SOURCES "*.cpp")

    add_library(transport_mnnvl_obj OBJECT ${TEV1_XPORT_MNNVL_SOURCES})
    target_include_directories(transport_mnnvl_obj PUBLIC ${CUDAToolkit_INCLUDE_DIRS})

    add_library(transport_mnnvl_static STATIC $<TARGET_OBJECTS:transport_mnnvl_obj>)
    target_link_libraries(transport_mnnvl_static PUBLIC CUDA::cuda_driver CUDA::cudart)

    add_library(transport_mnnvl_shared SHARED $<TARGET_OBJECTS:transport_mnnvl_obj>)
    target_link_libraries(transport_mnnvl_shared PUBLIC CUDA::cuda_driver CUDA::cudart)
else()
    message(STATUS "CUDA not found, skipping MultiNode NVLink transport")
endif()