file(GLOB UCX_SRC "ucx/ucx_*.c")
add_library(ucx_communication ${UCX_SRC})
target_link_libraries(ucx_communication PUBLIC ucx::ucp ucx::ucs)
target_include_directories(ucx_communication PUBLIC ${PROJECT_SOURCE_DIR}/include)

# empty list of sources
set(BSTRAP_SRC "")

if (MPI_C_FOUND)
  list(APPEND BSTRAP_SRC "bootstrap_funcs/mpi.c")
else()
  message(WARNING "MPI not found, disabling MPI bootstrap function")
endif()

add_library(bstrap_functions ${BSTRAP_SRC})
if (MPI_C_FOUND)
  target_compile_definitions(bstrap_functions PUBLIC DHT_USE_MPI)
  target_link_libraries(bstrap_functions PUBLIC MPI::MPI_C ucx::ucp ucx::ucs)
endif()

target_include_directories(bstrap_functions PUBLIC ${PROJECT_SOURCE_DIR}/include)

set(DHT_UCX_SRC 
  access.c
  accumulated.c
  cleanup.c
  collectives.c
  file.c
  init.c
)

add_library(ucxdht ${DHT_UCX_SRC})

target_link_libraries(ucxdht PUBLIC ucx_communication bstrap_functions -lm)
target_include_directories(ucxdht PUBLIC ${PROJECT_SOURCE_DIR}/include)

set(DHT_MPI_SRC
  mpi/access.c
  mpi/accumulated.c
  mpi/cleanup.c
  mpi/collectives.c
  mpi/file.c
  mpi/init.c
)

add_library(coarse ${DHT_MPI_SRC})
target_link_libraries(coarse PUBLIC MPI::MPI_C -lm)
target_compile_options(coarse PUBLIC -DLUCX_MPI_IMPL PRIVATE -DLUCX_MPI_OLD)
target_include_directories(coarse PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(nolock ${DHT_MPI_SRC})
target_link_libraries(nolock PUBLIC MPI::MPI_C -lm)
target_compile_options(nolock PUBLIC -DLUCX_MPI_IMPL PRIVATE -DLUCX_MPI_NO_LOCK)
target_include_directories(nolock PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(accumulate ${DHT_MPI_SRC})
target_link_libraries(accumulate PUBLIC MPI::MPI_C -lm)
target_compile_options(accumulate PUBLIC -DLUCX_MPI_IMPL PRIVATE -DLUCX_MPI_ACC)
target_include_directories(accumulate PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(fine ${DHT_MPI_SRC})
target_link_libraries(fine PUBLIC MPI::MPI_C -lm)
target_compile_options(fine PUBLIC -DLUCX_MPI_IMPL PRIVATE -DLUCX_MPI_FINE_LOCK)
target_include_directories(fine PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(LUCX::UCX ALIAS ucxdht)
add_library(MPIDHT::coarse ALIAS coarse)
add_library(MPIDHT::nolock ALIAS nolock)
add_library(MPIDHT::accumulate ALIAS accumulate)
add_library(MPIDHT::fine ALIAS fine)

option(DHT_STATISTICS "Build with collection of statistics" OFF)

if (DHT_STATISTICS)
  target_compile_definitions(LUCX_DHT PRIVATE DHT_STATISTICS)
endif()
