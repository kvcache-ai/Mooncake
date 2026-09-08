add_library(
  tent_xport_hp_tcp_core STATIC
  hp_tcp_protocol.cpp hp_tcp_workers.cpp hp_tcp_task.cpp
  hp_tcp_buffer_registry.cpp hp_tcp_client.cpp hp_tcp_server.cpp)
target_link_libraries(tent_xport_hp_tcp_core PUBLIC tent_common tent_interface)

add_library(tent_xport_hp_tcp STATIC hp_tcp_transport.cpp)
target_link_libraries(tent_xport_hp_tcp PUBLIC tent_rpc tent_common
                                               tent_xport_hp_tcp_core)
