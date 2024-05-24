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
  target_link_libraries(bstrap_functions PRIVATE MPI::MPI_C)
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

add_library(LUCX_DHT ${DHT_UCX_SRC})

target_link_libraries(LUCX_DHT PUBLIC ucx_communication bstrap_functions -lm)
target_include_directories(LUCX_DHT PUBLIC ${PROJECT_SOURCE_DIR}/include)

install(
  TARGETS LUCX_DHT 
  DESTINATION lib
)

option(DHT_STATISTICS "Build with collection of statistics" OFF)

if (DHT_STATISTICS)
  target_compile_definitions(LUCX_DHT PRIVATE DHT_STATISTICS)
endif()