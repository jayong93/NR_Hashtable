cmake_minimum_required(VERSION 3.10)

project("NR_Hashtable")
set(CMAKE_VERBOSE_MAKEFILE true)

set(OUTPUT_NAME "${CMAKE_PROJECT_NAME}")
set(SRC_FILES
    main.cpp
    )

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
if (NOT WRITE_RATIO)
    set(WRITE_RATIO 30)
endif()
if (RANGE_LIMIT)
    add_definitions(-DRANGE_LIMIT)
endif()


add_compile_options(-g -ggdb -std=c++17)
add_definitions(-DWRITE_RATIO=${WRITE_RATIO})
link_libraries(pthread numa)
set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin)

set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3")
add_executable(${OUTPUT_NAME} ${SRC_FILES})
