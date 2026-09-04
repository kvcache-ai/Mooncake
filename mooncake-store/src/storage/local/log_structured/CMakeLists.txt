add_library(mooncake_log_structured_storage STATIC record_format.cpp segment.cpp
                                                     index.cpp wal.cpp metadata.cpp storage_directory.cpp store.cpp)

target_include_directories(
  mooncake_log_structured_storage
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
         $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)

target_link_libraries(mooncake_log_structured_storage
                      PUBLIC yalantinglibs::yalantinglibs)
