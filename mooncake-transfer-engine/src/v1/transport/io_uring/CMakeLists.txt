if (URING_LIB AND URING_INCLUDE)
    file(GLOB TEV1_XPORT_URING_SOURCES "*.cpp")

    add_library(transport_uring_obj OBJECT ${TEV1_XPORT_URING_SOURCES})
    target_include_directories(transport_uring_obj PUBLIC ${URING_INCLUDE})
    if (CUDAToolkit_FOUND)
        target_link_libraries(transport_uring_obj PUBLIC 
            CUDA::cudart CUDA::cuda_driver CUDA::nvrtc)
    endif()

    add_library(transport_uring_static STATIC $<TARGET_OBJECTS:transport_uring_obj>)
    target_link_libraries(transport_uring_static PUBLIC ${URING_LIB})

    add_library(transport_uring_shared SHARED $<TARGET_OBJECTS:transport_uring_obj>)
    target_link_libraries(transport_uring_shared PUBLIC ${URING_LIB})
    
    install(TARGETS transport_uring_static transport_uring_shared
        ARCHIVE DESTINATION lib/transport
        LIBRARY DESTINATION lib/transport
        RUNTIME DESTINATION bin)
endif()
