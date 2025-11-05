if (IBVERBS_LIB AND IBVERBS_INCLUDE)
    file(GLOB TEV1_XPORT_RDMA_SOURCES "*.cpp")

    add_library(transport_rdma_static STATIC ${TEV1_XPORT_RDMA_SOURCES})
    target_include_directories(transport_rdma_static PUBLIC ${IBVERBS_INCLUDE})
    target_link_libraries(transport_rdma_static PUBLIC ${IBVERBS_LIB})

    add_library(transport_rdma_shared SHARED ${TEV1_XPORT_RDMA_SOURCES})
    target_include_directories(transport_rdma_shared PUBLIC ${IBVERBS_INCLUDE})
    target_link_libraries(transport_rdma_shared PUBLIC ${IBVERBS_LIB} tent_runtime)
    target_compile_definitions(transport_rdma_shared PUBLIC WITH_PLUGIN_HOOK)

    install(TARGETS transport_rdma_static transport_rdma_shared
        ARCHIVE DESTINATION lib/transport
        LIBRARY DESTINATION lib/transport
        RUNTIME DESTINATION bin)
endif()
