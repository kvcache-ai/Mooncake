# TENT Tests and Examples

# TENT Metrics Example
add_executable(tent_metrics_example tent_metrics_example.cpp)
# asio_shared must be linked explicitly for executables:
# ASIO_SEPARATE_COMPILATION mode requires that any binary using ASIO symbols
# links against the shared asio implementation directly. Libraries
# (tent_metrics, tent_interface) get it transitively; executables need it
# explicit.
target_link_libraries(tent_metrics_example PRIVATE tent_metrics tent_common
                                                   asio_shared glog ibverbs)
target_include_directories(tent_metrics_example
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

# TENT Metrics Config Loader Unit Test
add_executable(metrics_config_loader_test metrics_config_loader_test.cpp)
target_link_libraries(metrics_config_loader_test
                      PRIVATE tent_metrics tent_common gtest gtest_main glog)
target_include_directories(metrics_config_loader_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME metrics_config_loader_test COMMAND metrics_config_loader_test)

add_executable(request_merge_test request_merge_test.cpp)
target_link_libraries(request_merge_test PRIVATE gtest gtest_main
                                                 tent_link_group)
target_include_directories(request_merge_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME request_merge_test COMMAND request_merge_test)

add_executable(transfer_engine_config_override_test
               transfer_engine_config_override_test.cpp)
target_link_libraries(transfer_engine_config_override_test
                      PRIVATE gtest gtest_main tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(transfer_engine_config_override_test
                        PRIVATE asio_shared)
endif()
target_include_directories(transfer_engine_config_override_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME transfer_engine_config_override_test
         COMMAND transfer_engine_config_override_test)

add_executable(tent_tcp_transport_test tcp_transport_test.cpp)
target_link_libraries(tent_tcp_transport_test PRIVATE gtest gtest_main
                                                      tent_link_group)
target_include_directories(tent_tcp_transport_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_tcp_transport_test COMMAND tent_tcp_transport_test)

add_executable(tent_failover_test failover_test.cpp)
target_link_libraries(tent_failover_test PRIVATE gtest gtest_main
                                                 tent_link_group)
target_include_directories(tent_failover_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_failover_test COMMAND tent_failover_test)

add_executable(tent_endpoint_lifecycle_test endpoint_lifecycle_test.cpp)
target_link_libraries(tent_endpoint_lifecycle_test PRIVATE gtest gtest_main)
add_test(NAME tent_endpoint_lifecycle_test COMMAND tent_endpoint_lifecycle_test)

if(USE_SUNRISE)
  add_executable(tent_sunrise_link_transport_test
                 sunrise_link_transport_test.cpp)
  target_link_libraries(tent_sunrise_link_transport_test PRIVATE gtest
                                                                 gtest_main
                                                                 tent_link_group)
  target_include_directories(
    tent_sunrise_link_transport_test
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${CMAKE_CURRENT_SOURCE_DIR}/../../include)
  add_test(NAME tent_sunrise_link_transport_test
           COMMAND tent_sunrise_link_transport_test)
endif()
add_executable(tent_fault_proxy_test fault_proxy_test.cpp)
target_link_libraries(tent_fault_proxy_test PRIVATE gtest gtest_main
                                                     tent_link_group)
target_include_directories(tent_fault_proxy_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_fault_proxy_test COMMAND tent_fault_proxy_test)

add_executable(tent_rail_monitor_test rail_monitor_test.cpp)
target_link_libraries(tent_rail_monitor_test PRIVATE gtest gtest_main
                                                      tent_link_group)
target_include_directories(tent_rail_monitor_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_rail_monitor_test COMMAND tent_rail_monitor_test)

# End-to-end failover test: drives real TransferEngineImpl with
# FaultProxyTransport-wrapped fakes to exercise resubmitTransferTask.
add_executable(tent_engine_failover_e2e_test engine_failover_e2e_test.cpp)
target_link_libraries(tent_engine_failover_e2e_test
                      PRIVATE gtest gtest_main tent_link_group)
if(TARGET asio_shared)
  target_link_libraries(tent_engine_failover_e2e_test PRIVATE asio_shared)
endif()
target_include_directories(tent_engine_failover_e2e_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
add_test(NAME tent_engine_failover_e2e_test
         COMMAND tent_engine_failover_e2e_test)
