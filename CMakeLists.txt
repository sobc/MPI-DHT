# debian oldstable (bullseye)
cmake_minimum_required(VERSION 3.18)

project(DHT_UCX)

find_package(ucx REQUIRED)
find_package(MPI REQUIRED)

option(DHT_WITH_UCX "Build DHT based on Unified Communication X" ON)
option(DHT_WITH_MPI "Build DHT based on Message-Passing Interface" OFF)

option(DHT_COLLECT_DISTRIBUTION "Collect access distribution of entries accross proccesses" OFF)

add_subdirectory(src)
add_subdirectory(test)
