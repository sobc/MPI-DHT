#include <LUCX/DHT.h>
#include <mpi.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <ucp/api/ucp_compat.h>
#include <ucs/type/status.h>
#include <xxhash.h>

#include <ucp/api/ucp.h>
#include <unistd.h>

const int target = 0;

uint64_t xxhashWrapper(int len, const void *key) {
  return XXH3_64bits(key, len);
}

int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);

  uint32_t key;
  uint32_t data;
  int rank;
  int comm_size;
  int status;

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

  const ucx_ep_args_mpi_t mpi_bcast_params = {.comm = MPI_COMM_WORLD};
  DHT_init_t init_params = {.data_size = sizeof(uint32_t),
                            .key_size = sizeof(uint32_t),
                            .bucket_count = 50,
                            .hash_func = xxhashWrapper,
                            .bcast_func = UCX_INIT_BSTRAP_MPI,
                            .bcast_func_args = &mpi_bcast_params};

  DHT *object = DHT_create(&init_params);

  if (object == NULL) {
    fprintf(stderr, "Error while creating DHT. Aborting ...\n");
    MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
  }

  key = rank;
  data = rank;

  uint32_t evictions = 0;
  uint32_t read_misses = 0;

  status = DHT_write(object, &key, &data, NULL, NULL);

  switch (status) {
  case DHT_UCX_ERROR:
    fprintf(stderr, "Error during write. Aborting ...\n");
    MPI_Abort(MPI_COMM_WORLD, status);
  case DHT_WRITE_SUCCESS_WITH_EVICTION:
    evictions++;
  default:;
  }

  DHT_barrier(object);

  key = (rank + 1) % comm_size;

  status = DHT_read(object, &key, &data);

  switch (status) {
  case DHT_UCX_ERROR:
    fprintf(stderr, "Error during read. Aborting ...\n");
    MPI_Abort(MPI_COMM_WORLD, status);
  case DHT_READ_INVALID:
  case DHT_READ_CORRUPT:
  case DHT_READ_MISS:
    read_misses++;
  default:;
  }

  if (key != data) {
    fprintf(stdout, "Rank %d: FAILURE!\n", rank);
  } else {
    fprintf(stdout, "Rank %d: OK!\n", rank);
  }

  uint32_t checksum;
  status = DHT_free(object, &checksum);
  if (DHT_SUCCESS != status) {
    fprintf(stderr, "Error during free. Aborting ...\n");
    MPI_Abort(MPI_COMM_WORLD, status);
  }

  uint32_t evic, read;

  MPI_Reduce(&evictions, &evic, 1, MPI_UINT32_T, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&read_misses, &read, 1, MPI_UINT32_T, MPI_SUM, 0, MPI_COMM_WORLD);

  if (rank == 0) {
    printf("\nEvicted: %u\nRead: %u\nChecksum: %u\n\n", evic, read, checksum);
  }

  MPI_Finalize();
}
