if(USE_ASCEND OR USE_ASCEND_DIRECT)
  file(GLOB TENT_PLATFORM_ASCEND_SOURCES "*.cpp")
  add_library(platform_ascend STATIC ${TENT_PLATFORM_ASCEND_SOURCES})
  target_link_libraries(platform_ascend PUBLIC tent_common ascendcl)
  if(USE_ASCEND_DIRECT OR USE_UBSHMEM)
    # adxl::AdxlEngine::MallocMem / FreeMem live in llm_datadist (weak in
    # adxl_compat.h). Keep this dependency out of classic USE_ASCEND builds.
    target_link_libraries(platform_ascend PRIVATE llm_datadist)

    set(ACL_RUNTIME_HEADER_PATH "${ASCEND_INCLUDE_DIR}/acl/acl_rt.h")
    if(EXISTS "${ACL_RUNTIME_HEADER_PATH}")
      file(READ "${ACL_RUNTIME_HEADER_PATH}" ACL_RUNTIME_CONTENT)
      if("${ACL_RUNTIME_CONTENT}" MATCHES "aclrtMemRetainAllocationHandle")
        message(STATUS "TENT Ascend platform: fabric mem supported")
        target_compile_definitions(platform_ascend
                                   PRIVATE ASCEND_SUPPORT_FABRIC_MEM)
      else()
        message(STATUS "TENT Ascend platform: fabric mem not supported")
      endif()
    else()
      message(WARNING "Acl runtime header file is not exist")
    endif()
  endif()

  install(
    TARGETS platform_ascend
    ARCHIVE DESTINATION lib/platform
    LIBRARY DESTINATION lib/platform
    RUNTIME DESTINATION bin)
endif()
