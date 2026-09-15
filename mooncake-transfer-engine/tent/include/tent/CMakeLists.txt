install(FILES
    transfer_engine.h
    DESTINATION include/tent
)

if(CMAKE_PROJECT_NAME STREQUAL "mc_tent")
  install(FILES
      ${CMAKE_CURRENT_SOURCE_DIR}/../../../include/task_congestion_status.h
      ${CMAKE_CURRENT_SOURCE_DIR}/../../../include/task_congestion_status_c.h
      DESTINATION include
  )
endif()

install(FILES
    common/status.h
    common/types.h
    DESTINATION include/tent/common
)
