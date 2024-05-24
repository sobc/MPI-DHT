file(GLOB UCX_SRC "ucx/ucx_*.c")
add_library(ucx_communication ${UCX_SRC})
target_link_libraries(ucx_communication PUBLIC MPI::MPI_C ucx::ucp ucx::ucs)
target_include_directories(ucx_communication PUBLIC ${PROJECT_SOURCE_DIR}/include)

file(GLOB BSTRAP_SRC "bootstrap_funcs/*.c")
add_library(bstrap_functions ${BSTRAP_SRC})
target_link_libraries(bstrap_functions PUBLIC MPI::MPI_C ucx::ucp)
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

target_link_libraries(LUCX_DHT PUBLIC MPI::MPI_C ucx_communication bstrap_functions -lm)
target_include_directories(LUCX_DHT PUBLIC ${PROJECT_SOURCE_DIR}/include)

install(
  TARGETS LUCX_DHT 
  DESTINATION lib
)

option(DHT_STATISTICS "Build with collection of statistics" OFF)

if (DHT_STATISTICS)
  target_compile_definitions(LUCX_DHT PRIVATE DHT_STATISTICS)
endif()