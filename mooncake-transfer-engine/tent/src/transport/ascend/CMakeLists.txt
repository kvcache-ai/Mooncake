if(USE_ASCEND OR USE_ASCEND_DIRECT)
  file(GLOB TEV1_XPORT_ASCEND_DIRECT_SOURCES "*.cpp")
  add_library(tent_xport_ascend_direct STATIC
              ${TEV1_XPORT_ASCEND_DIRECT_SOURCES})
  target_link_libraries(tent_xport_ascend_direct PUBLIC tent_common)
  target_link_libraries(tent_xport_ascend_direct PRIVATE ascendcl cann_hixl
                                                         metadef)
  if(USE_ASCEND_DIRECT OR USE_UBSHMEM)
    # The store allocator's fabric path uses adxl (weak in adxl_compat.h), the
    # same dependency the classic transport keeps for its own allocator.
    target_link_libraries(tent_xport_ascend_direct PRIVATE llm_datadist)
  endif()
  # The affinity plan/thread pool/runner registry live in ascend_allocator.so,
  # shared with the classic transport; TENT keeps no private copy. A standalone
  # TENT build without that target compiles the same sources locally instead.
  if(TARGET ascend_allocator)
    if(USE_ASCEND_DIRECT OR USE_UBSHMEM)
      target_compile_definitions(tent_xport_ascend_direct
                                 PRIVATE TENT_HAS_ASCEND_ALLOCATOR)
    endif()
    target_link_libraries(tent_xport_ascend_direct PRIVATE ascend_allocator)
  else()
    target_sources(
      tent_xport_ascend_direct
      PRIVATE
        ${TENT_REPOSITORY_ROOT}/mooncake-transfer-engine/src/transport/ascend_transport/ascend_allocator/ascend_affinity.cpp
        ${TENT_REPOSITORY_ROOT}/mooncake-transfer-engine/src/transport/ascend_transport/ascend_allocator/allocation_affinity_pools.cpp
    )
    target_include_directories(
      tent_xport_ascend_direct PRIVATE
      ${TENT_REPOSITORY_ROOT}/mooncake-transfer-engine/include)
  endif()
  #
  # The direct-VMM classification for HIXL exports is compiled only when the
  # ACL runtime exposes the VMM API, detected the same way as the TENT Ascend
  # platform library and the classic transport.
  if(DEFINED ASCEND_INCLUDE_DIR)
    set(ACL_RUNTIME_HEADER_PATH "${ASCEND_INCLUDE_DIR}/acl/acl_rt.h")
    if(EXISTS "${ACL_RUNTIME_HEADER_PATH}")
      file(READ "${ACL_RUNTIME_HEADER_PATH}" ACL_RUNTIME_CONTENT)
      if("${ACL_RUNTIME_CONTENT}" MATCHES "aclrtMemRetainAllocationHandle")
        target_compile_definitions(tent_xport_ascend_direct
                                   PRIVATE ASCEND_SUPPORT_FABRIC_MEM)
      endif()
    endif()
  endif()
endif()
