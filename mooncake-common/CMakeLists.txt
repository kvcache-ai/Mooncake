if ((USE_ETCD AND NOT USE_ETCD_LEGACY) OR STORE_USE_ETCD)
    add_subdirectory(etcd)
endif()