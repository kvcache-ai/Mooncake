message(STATUS "-------------YLT INSTALL SETTING------------")
option(INSTALL_THIRDPARTY "Install thirdparty" ON)
option(INSTALL_STANDALONE "Install standalone" ON)
message(STATUS "INSTALL_THIRDPARTY: " ${INSTALL_THIRDPARTY})

message(STATUS "INSTALL_STANDALONE: " ${INSTALL_STANDALONE})
option(INSTALL_INDEPENDENT_THIRDPARTY "Install independent thirdparty" ON)
option(INSTALL_INDEPENDENT_STANDALONE "Install independent standalone" ON)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
        "${yaLanTingLibs_BINARY_DIR}/cmake/yalantinglibsConfigVersion.cmake"
        VERSION ${yaLanTingLibs_VERSION}
        COMPATIBILITY AnyNewerVersion
)
set(ConfigPackageLocation lib/cmake/yalantinglibs)

add_library(yalantinglibs INTERFACE)
add_library(yalantinglibs::yalantinglibs ALIAS yalantinglibs)

target_compile_features(yalantinglibs INTERFACE cxx_std_20)

target_include_directories(yalantinglibs INTERFACE 
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}/../include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}/../include/ylt/thirdparty>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}/../include/ylt/standalone>
)
install(TARGETS yalantinglibs
       EXPORT yalantinglibsTargets
       LIBRARY DESTINATION lib
       ARCHIVE DESTINATION lib
       RUNTIME DESTINATION bin
       )

file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/yalantinglibsConfig.cmake"
       "include(\$\{CMAKE_CURRENT_LIST_DIR\}/yalantinglibsConfigImpl.cmake)\n"
       "set(YLT_IS_IMPORTED_BY_FIND_PACKAGE true)\n"
       "include(\$\{CMAKE_CURRENT_LIST_DIR\}/config.cmake)\n"
)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/yalantinglibsConfig.cmake"
        DESTINATION ${ConfigPackageLocation})

install(FILES "${yaLanTingLibs_SOURCE_DIR}/cmake/config.cmake"
        DESTINATION ${ConfigPackageLocation}
        )

install(EXPORT yalantinglibsTargets
       FILE yalantinglibsConfigImpl.cmake
       NAMESPACE yalantinglibs::
       DESTINATION ${ConfigPackageLocation}
       )

install(DIRECTORY "${yaLanTingLibs_SOURCE_DIR}/include/" DESTINATION include REGEX "${yaLanTingLibs_SOURCE_DIR}/include/ylt/thirdparty" EXCLUDE REGEX "${yaLanTingLibs_SOURCE_DIR}/include/ylt/standalone" EXCLUDE)

if (INSTALL_THIRDPARTY)
        message(STATUS "INSTALL_INDEPENDENT_THIRDPARTY: " ${INSTALL_INDEPENDENT_THIRDPARTY})
        if (INSTALL_INDEPENDENT_THIRDPARTY)
                install(DIRECTORY "${yaLanTingLibs_SOURCE_DIR}/include/ylt/thirdparty/" DESTINATION include)
        else()
                install(DIRECTORY "${yaLanTingLibs_SOURCE_DIR}/include/ylt/thirdparty/" DESTINATION include/ylt/thirdparty)
                target_include_directories(yalantinglibs INTERFACE
                $<INSTALL_INTERFACE:include/ylt/thirdparty>
                )
        endif()
endif()
if(INSTALL_STANDALONE)
        message(STATUS "INSTALL_INDEPENDENT_STANDALONE: " ${INSTALL_INDEPENDENT_STANDALONE})
        if (INSTALL_INDEPENDENT_STANDALONE)        
                install(DIRECTORY "${yaLanTingLibs_SOURCE_DIR}/include/ylt/standalone/" DESTINATION include)
        else()
                install(DIRECTORY "${yaLanTingLibs_SOURCE_DIR}/include/ylt/standalone/" DESTINATION include/ylt/standalone)
                target_include_directories(yalantinglibs INTERFACE
                $<INSTALL_INTERFACE:include/ylt/standalone>)
        endif()
endif()
message(STATUS "--------------------------------------------")