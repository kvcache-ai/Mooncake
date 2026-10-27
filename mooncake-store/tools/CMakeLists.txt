add_executable(oplog_batch_inspector oplog_batch_inspector.cpp
                                     oplog_batch_auditor.cpp)
target_include_directories(oplog_batch_inspector
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(oplog_batch_inspector PRIVATE mooncake_store
                                                    JsonCpp::JsonCpp)
