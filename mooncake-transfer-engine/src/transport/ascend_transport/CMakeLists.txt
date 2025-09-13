if (USE_ASCEND)
    file(GLOB ASCEND_SOURCES "hccl_transport/*.cpp")
    add_subdirectory(hccl_transport/ascend_transport_c)
elseif (USE_ASCEND_HETEROGENEOUS)
	file(GLOB ASCEND_SOURCES "heterogeneous_rdma_transport/*.cpp")
else ()
    file(GLOB ASCEND_SOURCES "ascend_direct_transport/*.cpp")
endif ()

add_library(ascend_transport OBJECT ${ASCEND_SOURCES})

if (USE_ASCEND)
    target_link_libraries(ascend_transport PRIVATE ascend_transport_mem)
endif()

set_target_properties(ascend_transport
                      PROPERTIES
                      PREFIX ""
                    )

target_compile_options(ascend_transport PRIVATE
                       -O2
                       -Xlinker -export-dynamic     
                    )

target_link_options(ascend_transport PRIVATE
                    -s
                    )
