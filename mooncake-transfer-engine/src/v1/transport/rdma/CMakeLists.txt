if (IBVERBS_LIB AND IBVERBS_INCLUDE)
    file(GLOB TEV1_XPORT_RDMA_SOURCES "*.cpp")

    add_library(transport_rdma_obj OBJECT ${TEV1_XPORT_RDMA_SOURCES})
    target_include_directories(transport_rdma_obj PUBLIC ${IBVERBS_INCLUDE})

    add_library(transport_rdma_static STATIC $<TARGET_OBJECTS:transport_rdma_obj>)
    target_link_libraries(transport_rdma_static PUBLIC ${IBVERBS_LIB})

    add_library(transport_rdma_shared SHARED $<TARGET_OBJECTS:transport_rdma_obj>)
    target_link_libraries(transport_rdma_shared PUBLIC ${IBVERBS_LIB})

    install(TARGETS transport_rdma_static transport_rdma_shared
        ARCHIVE DESTINATION lib/transport
        LIBRARY DESTINATION lib/transport
        RUNTIME DESTINATION bin)
endif()
