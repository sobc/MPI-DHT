# LUCX-DHT

This project is a distributed hash table (DHT) implementation with lock-free
insertions and updates based on [UCX](https://openucx.org/).

## Table of Contents

- [LUCX-DHT](#lucx-dht)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Remarks](#remarks)

## Installation

The easiest way to integrate this project into your own project is to download
this repository or add it as a `git submodule` and use CMake as build system. In
your `CMakeLists.txt` file, you can add the following lines:

```cmake
add_subdirectory(/path/to/lucx-dht EXCLUDE_FROM_ALL)

# add your desired target
add_executable(your_target your_target.cpp)

# link the LUCX_DHT library to your target
target_link_libraries(your_target LUCX_DHT)
```

Then configure your project with CMake and build it. The LUCX-DHT library will
mostly work out of the box, but you may need to adjust your UCX settings. More
on this in the [Remarks](#remarks) section.

## Usage

We provided a simple example which serves also a a test for the library in
`test/testReadWrite.cpp`. This shows how to create a DHT, insert and read data
and finally destroy the DHT. If [xxHash](https://github.com/Cyan4973/xxHash) is
installed on your system, the example will automatically build if you build the
LUCX-DHT project with CMake.

Otherwise, you can substitute the hash function with your own. The hash function
is defined at the top of the source file.

Run the example using `mpirun ./ReadWrite`.

## Remarks

In most cases, you will use the DHT besides and already set up MPI runtime. In
the current state of the DHT implementation, even when using UCX for your MPI
runtime, LUCX-DHT will set up its own UCX context. This is not ideal and will be
hopefully fixed in the future.

LUCX-DHT is desired to be used with one-sided RDMA operations and so should not
interfere with your MPI runtime. However, we discovered deadlocks when having a
wrong UCX configuration, especially when communication is done over the network
interface/inter node communication. You should set your `UCX_NET_DEVICES`
explicitly to the device capable for RDMA one-sided operations. For example, if
you have a Mellanox Infiniband card, you should set `UCX_NET_DEVICES=mlx4_0:1`
or similar. Additionally, UCX will use `UD` as transport mode when using more
than 256 endpoints by default. Thus, you should set `UCX_TLS` to `self,sm,rc` to
restrict network communication transport modes to reliable connection (RC). 

If you do not force UCX to use RC and so one-sided operations, you may encounter
deadlocks when issuing DHT calls on one process and MPI calls on another
process.
