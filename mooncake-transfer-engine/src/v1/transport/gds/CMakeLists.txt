find_package(CUDAToolkit)
find_library(CUFILE_LIB cufile PATHS /usr/local/cuda/lib64)
find_path(CUFILE_INCLUDE cufile.h PATHS /usr/local/cuda/include)

if (CUDAToolkit_FOUND AND CUFILE_LIB AND CUFILE_INCLUDE)
    message(STATUS "Found GDS (cufile) at ${CUFILE_LIB}")

    file(GLOB TEV1_XPORT_GDS_SOURCES "*.cpp")
    add_library(transport_gds_obj OBJECT ${TEV1_XPORT_GDS_SOURCES})
    target_include_directories(transport_gds_obj PUBLIC ${CUFILE_INCLUDE})

    add_library(transport_gds_static STATIC $<TARGET_OBJECTS:transport_gds_obj>)
    target_link_libraries(transport_gds_static PUBLIC CUDA::cuda_driver CUDA::cudart ${CUFILE_LIB})

    add_library(transport_gds_shared SHARED $<TARGET_OBJECTS:transport_gds_obj>)
    target_link_libraries(transport_gds_shared PUBLIC CUDA::cuda_driver CUDA::cudart ${CUFILE_LIB})
else()
    message(STATUS "GDS not found, skipping transport_gds_v1")
endif()
