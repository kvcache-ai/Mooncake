if (CUDAToolkit_FOUND)
    message(STATUS "Transport mnnvl: included")

    file(GLOB TEV1_XPORT_MNNVL_SOURCES "*.cpp")

    add_library(transport_mnnvl_obj OBJECT ${TEV1_XPORT_MNNVL_SOURCES})

    add_library(transport_mnnvl_static STATIC $<TARGET_OBJECTS:transport_mnnvl_obj>)
    target_link_libraries(transport_mnnvl_static PUBLIC CUDA::cuda_driver CUDA::cudart)

    add_library(transport_mnnvl_shared SHARED $<TARGET_OBJECTS:transport_mnnvl_obj>)
    target_link_libraries(transport_mnnvl_shared PUBLIC CUDA::cuda_driver CUDA::cudart)
else()
    message(STATUS "Transport mnnvl: excluded")
endif()