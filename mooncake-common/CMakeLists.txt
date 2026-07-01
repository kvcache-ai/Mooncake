if((USE_ETCD AND NOT USE_ETCD_LEGACY)
   OR STORE_USE_ETCD
   OR STORE_USE_K8S_LEASE)
  add_subdirectory(ha-wrapper)
  set(HA_WRAPPER_INCLUDE
      ${HA_WRAPPER_INCLUDE}
      PARENT_SCOPE)
  set(HA_WRAPPER_LIB
      ${HA_WRAPPER_LIB}
      PARENT_SCOPE)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_subdirectory(src)

if(BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()
