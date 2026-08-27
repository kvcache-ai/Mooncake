add_library(tent_xport_hp_tcp STATIC
  high_performance_tcp_protocol.cpp
  high_performance_tcp_workers.cpp
  high_performance_tcp_task.cpp
  high_performance_tcp_buffer_registry.cpp
  high_performance_tcp_client.cpp
  high_performance_tcp_server.cpp
  high_performance_tcp_transport.cpp)
target_link_libraries(tent_xport_hp_tcp PUBLIC tent_rpc tent_common
                                              tent_interface)

add_library(tent_xport_tcp STATIC tcp_transport.cpp)
target_link_libraries(tent_xport_tcp PUBLIC tent_rpc tent_common)
