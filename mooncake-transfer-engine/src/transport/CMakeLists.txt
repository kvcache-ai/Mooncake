file(GLOB XPORT_SOURCES "*.cpp")
set(BUILTIN_TRANSPORT)

add_subdirectory(rdma_transport)
add_subdirectory(rdma_twosided)
add_subdirectory(rpc_communicator)
add_library(transport OBJECT ${XPORT_SOURCES}
                             $<TARGET_OBJECTS:rpc_communicator>)
target_link_libraries(
  transport PRIVATE JsonCpp::JsonCpp yalantinglibs::yalantinglibs glog::glog
                    pthread)

function(mooncake_add_builtin_transport target transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:${target}>)
  list(APPEND BUILTIN_TRANSPORT ${transport})
  set(BUILTIN_TRANSPORT ${BUILTIN_TRANSPORT} PARENT_SCOPE)
endfunction()

function(mooncake_register_builtin_transport transport)
  list(APPEND BUILTIN_TRANSPORT ${transport})
  set(BUILTIN_TRANSPORT ${BUILTIN_TRANSPORT} PARENT_SCOPE)
endfunction()

mooncake_add_builtin_transport(rdma_transport rdma)
mooncake_add_builtin_transport(rdma_twosided rdma_twosided)

if(USE_UB)
  add_subdirectory(kunpeng_transport)
  mooncake_add_builtin_transport(ub_transport ub)
endif()

if(USE_TCP)
  add_subdirectory(tcp_transport)
  mooncake_add_builtin_transport(tcp_transport tcp)
endif()

if(USE_BAREX)
  add_subdirectory(barex_transport)
  mooncake_add_builtin_transport(barex_transport barex)
endif()

if(USE_NVMEOF)
  add_subdirectory(nvmeof_transport)
  mooncake_add_builtin_transport(nvmeof_transport nvmeof)
endif()

if(USE_NCCL_HOST)
  add_subdirectory(nccl_transport)
  mooncake_add_builtin_transport(nccl_host_transport nccl)
endif()

if(USE_CXL)
  add_subdirectory(cxl_transport)
  mooncake_add_builtin_transport(cxl_transport cxl)
endif()

add_subdirectory(shm_transport)
mooncake_add_builtin_transport(shm_transport shm)


if(USE_ASCEND_DIRECT)
  add_subdirectory(ascend_transport)
  mooncake_register_builtin_transport(ascend_direct)
endif()

if(USE_ASCEND_HETEROGENEOUS)
  add_subdirectory(ascend_transport)
  mooncake_add_builtin_transport(ascend_transport ascend_heterogeneous)
endif()

if(USE_HIP)
  add_subdirectory(hip_transport)
  mooncake_add_builtin_transport(hip_transport hip)
endif()

if(USE_HYLINK)
  add_subdirectory(hylink_transport)
  mooncake_add_builtin_transport(hylink_transport hylink)
endif()

if(USE_MACA)
  add_subdirectory(maca_transport)
  mooncake_add_builtin_transport(maca_transport maca)
endif()

if(USE_MUSA)
  add_subdirectory(musa_transport)
  mooncake_add_builtin_transport(musa_transport musa)
endif()

if(USE_SUNRISE)
  add_subdirectory(sunrise_link)
  mooncake_add_builtin_transport(sunrise_link_transport sunrise_link)
endif()

if(USE_MUSA OR (USE_MNNVL AND NOT USE_HIP))
  add_subdirectory(nvlink_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:nvlink_transport>)
endif()
if(USE_MNNVL)
  mooncake_register_builtin_transport(nvlink)
endif()

if(USE_INTRA_NVLINK)
  add_subdirectory(intranode_nvlink_transport)
  mooncake_add_builtin_transport(intranode_nvlink_transport nvlink_intra)
endif()

if(USE_UBSHMEM)
  add_subdirectory(ascend_transport)
  mooncake_add_builtin_transport(ascend_transport ubshmem)
endif()

if(USE_EFA)
  add_subdirectory(efa_transport)
  mooncake_add_builtin_transport(efa_transport efa)
  target_link_libraries(transport PRIVATE ${CMAKE_DL_LIBS})
endif()

if(USE_CXI)
  add_subdirectory(cxi_transport)
  mooncake_add_builtin_transport(cxi_transport cxi)
  target_link_libraries(transport PRIVATE fabric)
endif()

if(USE_CUDA
   OR USE_MUSA
   OR USE_MACA)
  add_subdirectory(device)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:device_transport>)
  # device_transport (ibgda_device_transport.cpp / mlx5gda.cpp) calls libmlx5
  # DevX symbols (mlx5dv_devx_*, mlx5dv_init_obj) directly.
  if((USE_CUDA OR USE_MUSA OR USE_MACA) AND NOT USE_CXI)
    target_link_libraries(transport PUBLIC mlx5)
  endif()
  if(USE_CUDA AND MOONCAKE_HAVE_MLX5_DMABUF_UMEM)
    target_compile_definitions(device_transport PRIVATE _GNU_SOURCE)
    target_link_libraries(transport PUBLIC ${CMAKE_DL_LIBS})
  endif()
endif()

if(USE_FLAGCX)
  add_subdirectory(flagcx_transport)
  mooncake_add_builtin_transport(flagcx_transport flagcx)
  target_link_libraries(transport PRIVATE FlagCX::flagcx)
endif()

string(JOIN ")\nMOONCAKE_BUILTIN_TRANSPORT(" _builtin_transport_list
       ${BUILTIN_TRANSPORT})
set(BUILTIN_TRANSPORT_LIST
    "MOONCAKE_BUILTIN_TRANSPORT(${_builtin_transport_list})")
configure_file(builtin_transport_list.inc.in
               ${CMAKE_CURRENT_BINARY_DIR}/../builtin_transport_list.inc
               @ONLY)
