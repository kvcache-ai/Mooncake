# PXN OBJECT library - CUDA + USE_PXN only
#
# Depends on: mooncake-transfer-engine/include, CUDA headers, ibverbs, glog.
if (USE_CUDA AND USE_PXN)
    file(GLOB PXN_SOURCES "*.cpp")
    add_library(rdma_transport_pxn OBJECT ${PXN_SOURCES})

    target_include_directories(rdma_transport_pxn PUBLIC
        "/usr/local/cuda/include"
    )
    target_link_libraries(rdma_transport_pxn PRIVATE
        glog::glog
        JsonCpp::JsonCpp
    )
endif()
