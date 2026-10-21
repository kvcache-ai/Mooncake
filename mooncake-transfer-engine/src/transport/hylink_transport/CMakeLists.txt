file(GLOB HYLINK_TRANSPORT_SOURCES "*.cpp")

add_library(hylink_transport OBJECT ${HYLINK_TRANSPORT_SOURCES})
target_link_libraries(hylink_transport PRIVATE glog::glog)

target_include_directories(hylink_transport PUBLIC ${HIP_INCLUDE_DIRS})

target_compile_options(hylink_transport PRIVATE -Werror)
