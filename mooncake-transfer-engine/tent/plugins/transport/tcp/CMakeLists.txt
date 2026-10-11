# The core's TCP transport source compiled as a plugin that claims the built-in
# "tcp" slot.
tent_add_transport_plugin(
  tent_xport_tcp_plugin tcp
  ${CMAKE_CURRENT_SOURCE_DIR}/../../../src/transport/tcp/tcp_transport.cpp
  plugin_entry.cpp)
