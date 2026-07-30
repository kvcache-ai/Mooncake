if((USE_ETCD AND NOT USE_ETCD_LEGACY) OR STORE_USE_ETCD)
  add_subdirectory(etcd)
endif()

if(STORE_USE_K8S_LEASE)
  add_subdirectory(k8s-lease)
endif()

add_subdirectory(src)

if(BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()
