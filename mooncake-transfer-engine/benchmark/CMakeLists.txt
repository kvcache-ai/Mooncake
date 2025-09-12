file(GLOB TEBENCH_SOURCES "*.cpp")
add_executable(tebench ${TEBENCH_SOURCES})
target_link_libraries(tebench PUBLIC transfer_engine)
