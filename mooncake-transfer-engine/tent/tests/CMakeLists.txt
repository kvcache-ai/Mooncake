# TENT Tests and Examples

# TENT Metrics Example
add_executable(tent_metrics_example tent_metrics_example.cpp)
# asio_shared must be linked explicitly for executables: ASIO_SEPARATE_COMPILATION mode requires
# that any binary using ASIO symbols links against the shared asio implementation directly.
# Libraries (tent_metrics, tent_interface) get it transitively; executables need it explicit.
target_link_libraries(tent_metrics_example PRIVATE tent_metrics tent_common asio_shared glog ibverbs)
target_include_directories(tent_metrics_example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

# TENT Metrics Config Loader Unit Test
add_executable(metrics_config_loader_test metrics_config_loader_test.cpp)
target_link_libraries(metrics_config_loader_test PRIVATE tent_metrics tent_common gtest gtest_main glog)
target_include_directories(metrics_config_loader_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME metrics_config_loader_test COMMAND metrics_config_loader_test)

add_executable(request_merge_test request_merge_test.cpp)
target_link_libraries(request_merge_test PRIVATE tent gtest gtest_main)
target_include_directories(request_merge_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME request_merge_test COMMAND request_merge_test)

add_executable(transfer_engine_config_override_test
               transfer_engine_config_override_test.cpp)
target_link_libraries(transfer_engine_config_override_test
                      PRIVATE tent gtest gtest_main)
if (TARGET asio_shared)
    target_link_libraries(transfer_engine_config_override_test
                          PRIVATE asio_shared)
endif()
target_include_directories(transfer_engine_config_override_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME transfer_engine_config_override_test
         COMMAND transfer_engine_config_override_test)
