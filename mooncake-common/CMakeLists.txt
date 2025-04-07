if (USE_ETCD AND NOT USE_ETCD_LEGACY)
  add_subdirectory(etcd)
endif()