# Ascend host allocator, the store-side ContextManager and the CPU-affinity
# bound-runner registry. They live in one shared library so the classic Ascend
# transport, the store and TENT all use the same allocator bookkeeping, and a
# TENT-only consumer (tentpy) can load the allocator without pulling the
# transfer engine: the library has no undefined mooncake TE symbols. The TE
# side injects globalConfig() access through a config provider callback (see
# src/ascend_allocator_bridge.cpp).
add_library(ascend_allocator SHARED ascend_allocator.cpp context_manager.cpp
                                   allocation_affinity_pools.cpp ascend_affinity.cpp)
set_target_properties(ascend_allocator PROPERTIES PREFIX ""
                                                 POSITION_INDEPENDENT_CODE ON)
target_include_directories(
  ascend_allocator
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../../../include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../../../tent/include)
target_link_libraries(ascend_allocator PRIVATE glog::glog ${CMAKE_DL_LIBS}
                                                 pthread ascendcl)

# Same fabric-memory probe as the classic transport and the TENT Ascend
# platform: the direct ACL VMM path is compiled only when the ACL runtime
# exposes it.
if(DEFINED ASCEND_INCLUDE_DIR)
  set(ACL_RUNTIME_HEADER_PATH "${ASCEND_INCLUDE_DIR}/acl/acl_rt.h")
  if(EXISTS "${ACL_RUNTIME_HEADER_PATH}")
    file(READ "${ACL_RUNTIME_HEADER_PATH}" ACL_RUNTIME_CONTENT)
    if("${ACL_RUNTIME_CONTENT}" MATCHES "aclrtMemRetainAllocationHandle")
      target_compile_definitions(ascend_allocator
                                 PRIVATE ASCEND_SUPPORT_FABRIC_MEM)
    endif()
  endif()
endif()

install(TARGETS ascend_allocator DESTINATION lib)
