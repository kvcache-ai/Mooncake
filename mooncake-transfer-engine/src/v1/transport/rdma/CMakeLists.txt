find_library(IBVERBS_LIB ibverbs PATHS /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)
find_path(IBVERBS_INCLUDE infiniband/verbs.h PATHS /usr/include /usr/local/include)

if (IBVERBS_LIB AND IBVERBS_INCLUDE)
    message(STATUS "Transport rdma: included")
    add_definitions(-DUSE_RDMA)

    file(GLOB TEV1_XPORT_RDMA_SOURCES "*.cpp")

    add_library(transport_rdma_obj OBJECT ${TEV1_XPORT_RDMA_SOURCES})
    target_include_directories(transport_rdma_obj PUBLIC ${IBVERBS_INCLUDE})

    add_library(transport_rdma_static STATIC $<TARGET_OBJECTS:transport_rdma_obj>)
    target_link_libraries(transport_rdma_static PUBLIC ${IBVERBS_LIB})

    add_library(transport_rdma_shared SHARED $<TARGET_OBJECTS:transport_rdma_obj>)
    target_link_libraries(transport_rdma_shared PUBLIC ${IBVERBS_LIB})
else()
    message(STATUS "Transport rdma: excluded")
endif()
