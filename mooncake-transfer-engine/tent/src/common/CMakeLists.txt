file(GLOB TENT_COMMON_SOURCES "*.cpp")
add_library(tent_common OBJECT ${TENT_COMMON_SOURCES})
target_link_libraries(tent_common PUBLIC glog pthread numa tent_interface)