cmake_minimum_required(VERSION 3.28.0)

project(pydwarfs)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  add_compile_options(-fdiagnostics-color=always)
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  add_compile_options(-fcolor-diagnostics)
endif()

find_package(dwarfs CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(_pydwarfs src/pydwarfs.cpp)
target_link_libraries(_pydwarfs PRIVATE dwarfs::dwarfs_reader fmt::fmt)
