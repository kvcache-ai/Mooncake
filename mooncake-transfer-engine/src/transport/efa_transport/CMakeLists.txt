file(GLOB EFA_XPORT_SOURCES "*.cpp")
add_library(efa_transport OBJECT ${EFA_XPORT_SOURCES})
target_link_libraries(efa_transport PRIVATE ibverbs glog::glog)
