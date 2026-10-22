# TENT's public headers, for applications and for transport plugins built out of
# tree; with plugins/sdk they form the component tent_plugin_sdk.
install(
  DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
  DESTINATION include/tent
  COMPONENT tent_plugin_sdk
  FILES_MATCHING
  PATTERN "*.h"
  PATTERN "*.hpp")
