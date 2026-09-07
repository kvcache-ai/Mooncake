set(MOONCAKE_LOG_STRUCTURED_STORAGE_SOURCES
    record_format.cpp
    segment.cpp
    uring_batch_io.cpp
    index.cpp
    wal.cpp
    metadata.cpp
    storage_directory.cpp
    store.cpp)

add_library(mooncake_log_structured_storage_objects OBJECT
            ${MOONCAKE_LOG_STRUCTURED_STORAGE_SOURCES})
set_target_properties(mooncake_log_structured_storage_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(
  mooncake_log_structured_storage_objects
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
         $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)

target_link_libraries(mooncake_log_structured_storage_objects
                      PUBLIC yalantinglibs::yalantinglibs)

if(TARGET Mooncake::liburing)
  target_compile_definitions(mooncake_log_structured_storage_objects
                             PRIVATE USE_URING)
  target_include_directories(mooncake_log_structured_storage_objects
                             PRIVATE ${PROJECT_SOURCE_DIR}/src)
endif()

add_library(
  mooncake_log_structured_storage STATIC
  $<TARGET_OBJECTS:mooncake_log_structured_storage_objects>)
target_include_directories(
  mooncake_log_structured_storage
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
         $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)
target_link_libraries(mooncake_log_structured_storage
                      PUBLIC yalantinglibs::yalantinglibs)

if(TARGET Mooncake::liburing)
  target_link_libraries(mooncake_log_structured_storage
                        PRIVATE Mooncake::liburing)
endif()
