if (USE_ASCEND)
    file(GLOB ASCEND_SOURCES "hccl_transport/*.cpp")
    add_subdirectory(hccl_transport/ascend_transport_c)
elseif (USE_ASCEND_HETEROGENEOUS)
	file(GLOB ASCEND_SOURCES "heterogeneous_rdma_transport/*.cpp")
elseif (USE_UBSHMEM)
    file(GLOB ASCEND_SOURCES "ubshmem_transport/*.cpp")
else ()
    file(GLOB ASCEND_SOURCES "ascend_direct_transport/*.cpp")
endif ()

if (USE_ASCEND_DIRECT)
    set(ADXL_HEADER_PATH "${ASCEND_INCLUDE_DIR}/adxl/adxl_engine.h")
    if(EXISTS "${ADXL_HEADER_PATH}")
        file(READ "${ADXL_HEADER_PATH}" ADXL_CONTENT)
        if("${ADXL_CONTENT}" MATCHES "Status TransferAsync")
            message(STATUS "Check TransferAsync method exist.")
            add_compile_definitions(EXIST_ADXL_ASYNC_METHOD)
        else()
            message(STATUS "TransferAsync method is not exist.")
        endif()
    else()
        message(WARNING "Ascend direct header file is not exist")
    endif()
    add_library(ascend_transport SHARED ${ASCEND_SOURCES})
    find_library(
            FOUND_METADEF
            NAMES libmetadef.so
            PATHS ${ASCEND_LIB_DIR}
            NO_DEFAULT_PATH
            NO_CMAKE_FIND_ROOT_PATH
    )
    if(FOUND_METADEF)
        message(STATUS "Found library: ${FOUND_METADEF}")
        target_link_libraries(ascend_transport PRIVATE ascendcl llm_datadist metadef)
    else()
        target_link_libraries(ascend_transport PRIVATE ascendcl llm_datadist graph_base graph)
    endif()
    install(TARGETS ascend_transport DESTINATION lib)
else()
    add_library(ascend_transport OBJECT ${ASCEND_SOURCES})
endif()

if (USE_ASCEND)
    target_link_libraries(ascend_transport PRIVATE ascend_transport_mem)
endif()

if (USE_UBSHMEM)
    target_link_libraries(ascend_transport PRIVATE ascendcl)
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
