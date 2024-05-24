# debian oldstable (bullseye)
cmake_minimum_required(VERSION 3.18)

project(LUCX_DHT)

find_package(ucx REQUIRED)
find_package(MPI)


add_subdirectory(src)
add_subdirectory(test)

install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
)

option(DHT_BUILD_DOCS "Build documentation" OFF)

if (DHT_BUILD_DOCS)
    add_subdirectory(docs)
endif()