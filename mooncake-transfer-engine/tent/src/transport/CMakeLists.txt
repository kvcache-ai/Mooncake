add_subdirectory(tcp)
add_subdirectory(rdma)
add_subdirectory(shm)
add_subdirectory(nvlink)
add_subdirectory(mnnvl)
add_subdirectory(gds)
add_subdirectory(io_uring)
add_subdirectory(bufio)
add_subdirectory(ascend)

add_library(tent_transport_all INTERFACE)
foreach(tgt tent_xport_gds
            tent_xport_uring
            tent_xport_bufio
            tent_xport_mnnvl
            tent_xport_nvlink
            tent_xport_rdma
            tent_xport_shm
            tent_xport_tcp
            tent_xport_ascend_direct)
    if (TARGET ${tgt})
        target_link_libraries(tent_transport_all INTERFACE ${tgt})
    endif()
endforeach()
