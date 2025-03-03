set(XXHASH_ROOT "" CACHE PATH "Path to the xxhash installation directory")

find_library(XXHASH_LIB xxhash PATH ${XXHASH_ROOT}/lib)
find_path(XXHASH_INCLUDE xxhash.h PATH ${XXHASH_ROOT}/include)

if (NOT XXHASH_LIB AND NOT XXHASH_INCLUDE)
  message("LUCX-DHT: xxhash not found, skipping building tests")
elseif(NOT MPI_C_FOUND)
  message("LUCX-DHT: MPI not found, skipping build tests")
else()

add_library(xxhashlib INTERFACE IMPORTED)
set_target_properties(xxhashlib PROPERTIES
INTERFACE_LINK_LIBRARIES "${XXHASH_LIB}"
INTERFACE_INCLUDE_DIRECTORIES "${XXHASH_INCLUDE}")

  add_executable(ReadWrite testReadWrite.c)
  target_link_libraries(ReadWrite PRIVATE LUCX::UCX xxhashlib MPI::MPI_C)
  
  add_executable(ReadWriteOld testReadWriteMPI.c)
  target_link_libraries(ReadWriteOld PRIVATE MPIDHT::coarse xxhashlib MPI::MPI_C)

  add_executable(ReadWriteNoLock testReadWriteMPI.c)
  target_link_libraries(ReadWriteNoLock PRIVATE MPIDHT::nolock xxhashlib MPI::MPI_C)

  add_executable(ReadWriteAcc testReadWriteMPI.c)
  target_link_libraries(ReadWriteAcc PRIVATE MPIDHT::accumulate xxhashlib MPI::MPI_C)
  
  add_executable(ReadWriteFineLock testReadWriteMPI.c)
  target_link_libraries(ReadWriteFineLock PRIVATE MPIDHT::fine xxhashlib MPI::MPI_C)
endif()
