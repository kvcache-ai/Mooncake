if(YLT_HAVE_IBVERBS)
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests)


  add_executable(ibverbs_test
    test_device.cpp
    test_ib_socket.cpp
    main.cpp
  )

  add_executable(ibverbs_pressure_test
    ib_socket_pressure_test.cpp     
    main.cpp)
  
  target_link_libraries(ibverbs_test -libverbs)
  target_link_libraries(ibverbs_pressure_test -libverbs)
  add_test(NAME ibverbs_test COMMAND ibverbs_test)
  add_test(NAME ibverbs_pressure_test COMMAND ibverbs_pressure_test)
endif()
