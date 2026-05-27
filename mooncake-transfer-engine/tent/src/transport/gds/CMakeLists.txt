# GDS Transport - Platform-specific builds
# GDS (GPUDirect Storage) is CUDA-specific, but we structure it similarly

file(GLOB TEV1_XPORT_GDS_SOURCES "*.cpp")

# Find cufile (GPUDirect Storage library)
find_library(CUFILE_LIB cufile PATHS /usr/local/cuda/lib64)
find_path(CUFILE_INCLUDE cufile.h PATHS /usr/local/cuda/include)

if (CUFILE_LIB AND CUFILE_INCLUDE)
    add_definitions(-DUSE_GDS)

    # Build static library (for static linking)
    if (CUDAToolkit_FOUND)
        add_library(tent_xport_gds STATIC ${TEV1_XPORT_GDS_SOURCES})
        target_include_directories(tent_xport_gds PUBLIC ${CUFILE_INCLUDE})
        target_link_libraries(tent_xport_gds PUBLIC tent_platform tent_common CUDA::cuda_driver CUDA::cudart ${CUFILE_LIB})
        target_compile_definitions(tent_xport_gds PRIVATE USE_CUDA)
    endif()

    # Build platform-specific plugin shared libraries
    option(TENT_BUILD_TRANSPORT_PLUGINS "Build transport plugins as platform-specific .so files" ON)

    if (TENT_BUILD_TRANSPORT_PLUGINS)
        # CUDA variant (GDS is CUDA-specific)
        if (CUDAToolkit_FOUND)
            add_library(tent_gds_cuda SHARED ${TEV1_XPORT_GDS_SOURCES})
            target_compile_definitions(tent_gds_cuda PRIVATE TENT_BUILD_PLUGIN USE_CUDA USE_GDS)
            target_include_directories(tent_gds_cuda PUBLIC ${CUFILE_INCLUDE})
            target_link_libraries(tent_gds_cuda PUBLIC tent_platform tent_common CUDA::cuda_driver CUDA::cudart ${CUFILE_LIB})
            set_target_properties(tent_gds_cuda PROPERTIES
                VERSION ${PROJECT_VERSION}
                SOVERSION 0
                OUTPUT_NAME "tent_gds_cuda"
            )
            install(TARGETS tent_gds_cuda
                LIBRARY DESTINATION lib/tent/transport
                RUNTIME DESTINATION bin
            )
            message(STATUS "Transport plugin: gds_cuda (GPUDirect Storage)")
        endif()
    endif()
else()
    message(STATUS "GDS: Disabled (cufile not found)")
endif()
