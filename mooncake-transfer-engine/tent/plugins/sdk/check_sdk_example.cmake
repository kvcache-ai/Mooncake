# Installs the plugin SDK of a build (the component tent_plugin_sdk and the
# yalantinglibs it fetched) under WORK/root, copies the SDK example out of the
# source tree so that it builds the sample's sources from the SDK, builds it and
# loads the result through HOST, which uses libtent_shared.so. DESTDIR keeps an
# absolute install destination inside WORK as well.
#
# ~~~
#   cmake -DBUILD_DIR=<build> -DYLT_BUILD_DIR=<dir> -DEXAMPLE=<dir>
#         -DWORK=<dir> -DGENERATOR=<generator> -DMAKE_PROGRAM=<build tool>
#         -DCXX=<compiler> -DHOST=<host app> -DPROBE_DIR=<dir> -P <this>
# ~~~

function(run what)
  execute_process(
    COMMAND ${ARGN}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE output)
  if(NOT result EQUAL 0)
    message(FATAL_ERROR "${what} failed (${result}):\n${output}")
  endif()
  set(output
      "${output}"
      PARENT_SCOPE)
endfunction()

file(REMOVE_RECURSE ${WORK})
set(prefix ${WORK}/root/usr/local)
foreach(what SDK yalantinglibs)
  if(what STREQUAL "SDK")
    set(args ${BUILD_DIR} --component tent_plugin_sdk)
  else()
    set(args ${YLT_BUILD_DIR})
  endif()
  run("Installing ${what}"
      ${CMAKE_COMMAND}
      -E
      env
      DESTDIR=${WORK}/root
      ${CMAKE_COMMAND}
      --install
      ${args}
      --prefix
      /usr/local)
endforeach()
file(COPY ${EXAMPLE}/ DESTINATION ${WORK}/src)
run("Configuring the example"
    ${CMAKE_COMMAND}
    -S
    ${WORK}/src
    -B
    ${WORK}/build
    -G
    ${GENERATOR}
    -DCMAKE_MAKE_PROGRAM=${MAKE_PROGRAM}
    -DCMAKE_CXX_COMPILER=${CXX}
    -DCMAKE_PREFIX_PATH=${prefix}
    -DCMAKE_BUILD_TYPE=Release)
run("Building the example" ${CMAKE_COMMAND} --build ${WORK}/build)
file(COPY ${WORK}/build/libtent_xport_loopback.so DESTINATION ${WORK}/plugins)
run("Loading the example" ${HOST} ${WORK}/plugins ${PROBE_DIR})
message(STATUS "${output}")
