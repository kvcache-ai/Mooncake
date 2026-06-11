if (IBVERBS_INCLUDE)
    file(GLOB XPORT_SOURCES "*.cpp")
    add_library(tent_xport_rdma STATIC ${XPORT_SOURCES})
    target_include_directories(tent_xport_rdma PUBLIC ${IBVERBS_INCLUDE})
    target_link_libraries(tent_xport_rdma PUBLIC tent_common)

    # MLX5 Direct Verbs support for UDP sport override and LAG balancing
    if(USE_MLX5DV)
        message(STATUS "TENT: Enabled USE_MLX5DV support (mlx5 direct verbs)")
        target_link_libraries(tent_xport_rdma PUBLIC mlx5)
        target_compile_definitions(tent_xport_rdma PUBLIC USE_MLX5DV)
    endif()
endif()
