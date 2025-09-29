add_subdirectory(common)
add_subdirectory(rpc)
add_subdirectory(metastore)
add_subdirectory(runtime)
add_subdirectory(platform)
add_subdirectory(transport)

SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

file(GLOB TEV1_ENGINE_SOURCES "*.cpp")
add_library(transfer_engine_v1 STATIC ${TEV1_ENGINE_SOURCES})
target_link_libraries(
  transfer_engine_v1
  PUBLIC
  tent_runtime
)

add_library(transfer_engine_shared SHARED ${TEV1_ENGINE_SOURCES})
target_link_libraries(
  transfer_engine_shared
  PUBLIC
  tent_runtime
)
