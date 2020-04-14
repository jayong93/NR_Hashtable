cmake_minimum_required(VERSION 3.0)

project("NR_Hashtable")
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_VERBOSE_MAKEFILE true)

set(OUTPUT_NAME "${CMAKE_PROJECT_NAME}")
set(SRC_FILES
    main.cpp
    NR.cpp
    )

add_compile_options(-g -ggdb -std=c++17)
set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin)

set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3")
add_executable(${OUTPUT_NAME} ${SRC_FILES})