# Fails if the RDMA plugin imports a libibverbs or libnuma function without a
# symbol version (see CMakeLists.txt). A plugin that fails the check, or cannot
# be checked, is deleted, so it is neither loaded from the plugin directory nor
# taken as up to date by the next build.
#
# cmake -DOBJDUMP=<objdump> -DPLUGIN=<libtent_xport_rdma.so> -P <this>

execute_process(
  COMMAND "${OBJDUMP}" -T "${PLUGIN}"
  OUTPUT_VARIABLE symbols
  RESULT_VARIABLE result)
if(NOT result EQUAL 0)
  file(REMOVE "${PLUGIN}")
  message(FATAL_ERROR "objdump failed on ${PLUGIN} "
                      "(CMAKE_OBJDUMP '${OBJDUMP}'): ${result}")
endif()
# Imports read "*UND*<tab><size> <version> <name>", the version in parentheses
# with recent binutils; an unversioned import shows "Base", "(Base)" or nothing
# as its version.
string(
  REGEX MATCHALL
        "\\*UND\\*\t[0-9a-f]+ +(\\(?Base\\)? +)?(ibv_|numa_)[A-Za-z0-9_]+\n"
        unversioned "${symbols}")
if(unversioned)
  string(REGEX REPLACE "\\*UND\\*\t[0-9a-f]+ +(\\(?Base\\)? +)?" "" unversioned
                       "${unversioned}")
  string(REGEX REPLACE "\n;?" " " unversioned "${unversioned}")
  file(REMOVE "${PLUGIN}")
  message(
    FATAL_ERROR
      "${PLUGIN} imports without a symbol version: ${unversioned}\n"
      "Link libibverbs and libnuma so the dynamic linker binds the current "
      "versions instead of the oldest ones.")
endif()
# Make sure the output has the layout read above: the plugin's C library imports
# always carry a version.
string(REGEX MATCH
             "\\*UND\\*\t[0-9a-f]+ +\\(?GLIBC_[0-9.]+\\)? +[A-Za-z0-9_]+\n"
             versioned "${symbols}")
if(NOT versioned)
  file(REMOVE "${PLUGIN}")
  message(FATAL_ERROR "${OBJDUMP} -T shows no versioned C library import in "
                      "${PLUGIN}; its output was not understood")
endif()
