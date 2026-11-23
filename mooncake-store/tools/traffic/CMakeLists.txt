find_package(Threads REQUIRED)

# The session reader is a library so the replay tool and its tests read session
# files through the same code.
add_library(traffic_session STATIC session_reader.cpp)
target_compile_features(traffic_session PUBLIC cxx_std_20)
target_include_directories(
  traffic_session PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
                         ${CMAKE_CURRENT_SOURCE_DIR}/../../include
                         ${CMAKE_CURRENT_SOURCE_DIR}/../../src/traffic)
target_link_libraries(traffic_session PUBLIC Threads::Threads)

add_executable(traffic_replay traffic_replay.cpp)
target_compile_features(traffic_replay PRIVATE cxx_std_20)
target_include_directories(
  traffic_replay PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include
                         ${CMAKE_CURRENT_SOURCE_DIR}/../../src/traffic)
target_link_libraries(traffic_replay PRIVATE Threads::Threads
                                             JsonCpp::JsonCpp
                                             traffic_session)

# The session reader feeds three tools that answer different questions about
# the same recording: what it contains, what happens when it is replayed, and
# how much load the server sustains.
add_executable(traffic_profile traffic_profile.cpp)
target_compile_features(traffic_profile PRIVATE cxx_std_20)
target_include_directories(
  traffic_profile PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include
                          ${CMAKE_CURRENT_SOURCE_DIR}/../../src/traffic)
target_link_libraries(traffic_profile PRIVATE Threads::Threads
                                               JsonCpp::JsonCpp
                                               traffic_session)

add_executable(traffic_load traffic_load.cpp)
target_compile_features(traffic_load PRIVATE cxx_std_20)
target_include_directories(
  traffic_load PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include
                       ${CMAKE_CURRENT_SOURCE_DIR}/../../src/traffic)
target_link_libraries(traffic_load PRIVATE Threads::Threads
                                              JsonCpp::JsonCpp
                                              traffic_session)
