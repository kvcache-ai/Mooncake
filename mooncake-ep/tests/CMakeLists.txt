# Host-only tests of Mooncake EP code shared by every backend. They need no GPU
# and no EP build, so the top level adds them whenever unit tests are on.
add_executable(ep_dispatch_slots_test dispatch_slots_test.cpp)
target_include_directories(ep_dispatch_slots_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(ep_dispatch_slots_test PRIVATE gtest gtest_main pthread)
add_test(NAME ep_dispatch_slots_test COMMAND ep_dispatch_slots_test)
