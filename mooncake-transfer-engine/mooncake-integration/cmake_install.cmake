# Install script for directory: /root/Mooncake/mooncake-integration

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "RelWithDebInfo")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "1")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

# Set default install directory permissions.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/usr/bin/objdump")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/root/install/miniconda3/lib/python3.13/site-packages/mooncake/async_store.py")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  file(INSTALL DESTINATION "/root/install/miniconda3/lib/python3.13/site-packages/mooncake" TYPE FILE FILES "/root/Mooncake/mooncake-integration/store/async_store.py")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/root/install/miniconda3/lib/python3.13/site-packages/mooncake/http_metadata_server.py;/root/install/miniconda3/lib/python3.13/site-packages/mooncake/cli_bench.py;/root/install/miniconda3/lib/python3.13/site-packages/mooncake/transfer_engine_topology_dump.py;/root/install/miniconda3/lib/python3.13/site-packages/mooncake/mooncake_connector_v1.py;/root/install/miniconda3/lib/python3.13/site-packages/mooncake/vllm_v1_proxy_server.py")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  file(INSTALL DESTINATION "/root/install/miniconda3/lib/python3.13/site-packages/mooncake" TYPE FILE FILES
    "/root/Mooncake/mooncake-integration/../mooncake-wheel/mooncake/http_metadata_server.py"
    "/root/Mooncake/mooncake-integration/../mooncake-wheel/mooncake/cli_bench.py"
    "/root/Mooncake/mooncake-integration/../mooncake-wheel/mooncake/transfer_engine_topology_dump.py"
    "/root/Mooncake/mooncake-integration/../mooncake-wheel/mooncake/mooncake_connector_v1.py"
    "/root/Mooncake/mooncake-integration/../mooncake-wheel/mooncake/vllm_v1_proxy_server.py"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/root/install/miniconda3/lib/python3.13/site-packages/mooncake/mooncake_store_service.py;/root/install/miniconda3/lib/python3.13/site-packages/mooncake/mooncake_config.py;/root/install/miniconda3/lib/python3.13/site-packages/mooncake/cli.py")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  file(INSTALL DESTINATION "/root/install/miniconda3/lib/python3.13/site-packages/mooncake" TYPE FILE FILES
    "/root/Mooncake/mooncake-integration/../mooncake-wheel/mooncake/mooncake_store_service.py"
    "/root/Mooncake/mooncake-integration/../mooncake-wheel/mooncake/mooncake_config.py"
    "/root/Mooncake/mooncake-integration/../mooncake-wheel/mooncake/cli.py"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/root/install/miniconda3/lib/python3.13/site-packages/mooncake/")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  file(INSTALL DESTINATION "/root/install/miniconda3/lib/python3.13/site-packages/mooncake" TYPE DIRECTORY FILES "/root/Mooncake/mooncake-transfer-engine/mooncake-integration/mooncake/")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  
        execute_process(COMMAND chmod 767 "/root/install/miniconda3/lib/python3.13/site-packages/mooncake")
        execute_process(COMMAND chmod 766 "/root/install/miniconda3/lib/python3.13/site-packages/mooncake/__init__.py")
    
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/root/install/miniconda3/lib/python3.13/site-packages/mooncake/store.cpython-313-x86_64-linux-gnu.so")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  file(INSTALL DESTINATION "/root/install/miniconda3/lib/python3.13/site-packages/mooncake" TYPE MODULE FILES "/root/Mooncake/mooncake-transfer-engine/mooncake-integration/store.cpython-313-x86_64-linux-gnu.so")
  if(EXISTS "$ENV{DESTDIR}/root/install/miniconda3/lib/python3.13/site-packages/mooncake/store.cpython-313-x86_64-linux-gnu.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}/root/install/miniconda3/lib/python3.13/site-packages/mooncake/store.cpython-313-x86_64-linux-gnu.so")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}/root/install/miniconda3/lib/python3.13/site-packages/mooncake/store.cpython-313-x86_64-linux-gnu.so")
    endif()
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  include("/root/Mooncake/mooncake-transfer-engine/mooncake-integration/CMakeFiles/store.dir/install-cxx-module-bmi-RelWithDebInfo.cmake" OPTIONAL)
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/root/install/miniconda3/lib/python3.13/site-packages/mooncake/engine.cpython-313-x86_64-linux-gnu.so")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  file(INSTALL DESTINATION "/root/install/miniconda3/lib/python3.13/site-packages/mooncake" TYPE MODULE FILES "/root/Mooncake/mooncake-transfer-engine/mooncake-integration/engine.cpython-313-x86_64-linux-gnu.so")
  if(EXISTS "$ENV{DESTDIR}/root/install/miniconda3/lib/python3.13/site-packages/mooncake/engine.cpython-313-x86_64-linux-gnu.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}/root/install/miniconda3/lib/python3.13/site-packages/mooncake/engine.cpython-313-x86_64-linux-gnu.so")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}/root/install/miniconda3/lib/python3.13/site-packages/mooncake/engine.cpython-313-x86_64-linux-gnu.so")
    endif()
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  include("/root/Mooncake/mooncake-transfer-engine/mooncake-integration/CMakeFiles/engine.dir/install-cxx-module-bmi-RelWithDebInfo.cmake" OPTIONAL)
endif()

