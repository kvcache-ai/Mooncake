add_library(mooncake_log_structured_storage STATIC record_format.cpp segment.cpp
                                                     uring_batch_io.cpp index.cpp wal.cpp metadata.cpp storage_directory.cpp store.cpp)

target_include_directories(
  mooncake_log_structured_storage
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
         $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)

target_link_libraries(mooncake_log_structured_storage
                      PUBLIC yalantinglibs::yalantinglibs)

if(TARGET Mooncake::liburing)
  target_compile_definitions(mooncake_log_structured_storage PRIVATE USE_URING)
  target_include_directories(mooncake_log_structured_storage
                             PRIVATE ${PROJECT_SOURCE_DIR}/src)
  target_link_libraries(mooncake_log_structured_storage
                        PRIVATE Mooncake::liburing)
endif()
