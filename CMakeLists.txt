# debian oldstable (bullseye)
cmake_minimum_required(VERSION 3.18)

project(DHT_UCX)

find_package(ucx REQUIRED)
find_package(MPI REQUIRED)

add_subdirectory(src)
add_subdirectory(test)
