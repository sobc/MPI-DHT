# debian oldstable (bullseye)
cmake_minimum_required(VERSION 3.18)

project(DHT_UCX)

find_package(ucx REQUIRED)
find_package(MPI REQUIRED)

option(DHT_WITH_UCX "Build DHT based on Unified Communication X" ON)

add_subdirectory(src)
add_subdirectory(test)
