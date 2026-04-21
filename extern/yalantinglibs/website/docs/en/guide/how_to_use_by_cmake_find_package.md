# Use by CMake find_package

To use CMake find_package, CMake and yaLanTingLibs must be installed first.

And you have to use CMake to manage your project.

## Install yaLanTingLibs

Currently, only build from sources supported. 
Install from package manager (e.g. conan and vcpkg) is WIP.

use the following instructions

- clone yaLanTingLibs
```shell
git clone https://github.com/alibaba/yalantinglibs.git
cd yalantinglibs
```

- build yaLanTingLibs
```shell
mkdir build && cd build
cmake ..
make -j
```
- install yaLanTingLibs

```shell
# sudo if needed
make install 
```

## Use yaLanTingLibs

Here is an example about how to use cmake to import ylt in your project.

```shell
cmake_minimum_required(VERSION 3.15)
project(file_transfer)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release")
endif()
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_INCLUDE_CURRENT_DIR ON)
find_package(Threads REQUIRED)
link_libraries(Threads::Threads)
find_package(yalantinglibs REQUIRED)
link_libraries(yalantinglibs::yalantinglibs)

add_executable(coro_io_example main.cpp)
```

the full demo is here: https://github.com/PikachuHyA/yalantinglibs_find_package_demo

## Reference 

- [CMake find_package Document](https://cmake.org/cmake/help/latest/command/find_package.html?highlight=find_package)
- [use yaLanTingLibs with CMake find_package demo code](https://github.com/PikachuHyA/yalantinglibs_find_package_demo)
- [use yaLanTingLibs as Git Submodule demo code](https://github.com/PikachuHyA/yalantinglibs_as_submodule_demo)
