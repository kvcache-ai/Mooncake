# TENT Metrics Example
add_executable(tent_metrics_example tent_metrics_example.cpp)
target_link_libraries(tent_metrics_example PRIVATE tent_metrics tent_common glog ibverbs)
target_include_directories(tent_metrics_example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
