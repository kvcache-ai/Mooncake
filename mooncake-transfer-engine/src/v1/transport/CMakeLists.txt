add_subdirectory(tcp)
add_subdirectory(rdma)
add_subdirectory(shm)
add_subdirectory(nvlink)
add_subdirectory(mnnvl)
add_subdirectory(gds)
add_subdirectory(io_uring)

add_library(transport_all INTERFACE)
foreach(tgt transport_gds_static
            transport_uring_static
            transport_mnnvl_static
            transport_nvlink_static
            transport_rdma_static
            transport_shm_static
            transport_tcp_static)
    if (TARGET ${tgt})
        target_link_libraries(transport_all INTERFACE ${tgt})
    endif()
endforeach()
