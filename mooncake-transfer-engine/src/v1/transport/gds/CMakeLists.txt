if (CUDAToolkit_FOUND AND CUFILE_LIB AND CUFILE_INCLUDE)
    add_definitions(-DUSE_GDS)

    file(GLOB TEV1_XPORT_GDS_SOURCES "*.cpp")
    add_library(transport_gds_obj OBJECT ${TEV1_XPORT_GDS_SOURCES})
    target_include_directories(transport_gds_obj PUBLIC ${CUFILE_INCLUDE})

    add_library(transport_gds_static STATIC $<TARGET_OBJECTS:transport_gds_obj>)
    target_link_libraries(transport_gds_static PUBLIC CUDA::cuda_driver CUDA::cudart ${CUFILE_LIB})

    add_library(transport_gds_shared SHARED $<TARGET_OBJECTS:transport_gds_obj>)
    target_link_libraries(transport_gds_shared PUBLIC CUDA::cuda_driver CUDA::cudart ${CUFILE_LIB})

    install(TARGETS transport_gds_static transport_gds_shared
        ARCHIVE DESTINATION lib/transport
        LIBRARY DESTINATION lib/transport
        RUNTIME DESTINATION bin)
endif()
