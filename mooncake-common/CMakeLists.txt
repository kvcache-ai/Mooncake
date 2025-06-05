if ((USE_ETCD AND NOT USE_ETCD_LEGACY) OR WITH_STORE)
    add_subdirectory(etcd)
endif()