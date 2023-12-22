#include <DHT_ucx/DHT.h>
#include <DHT_ucx/UCX_bcast_functions.h>
#include <inttypes.h>
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
                            .bcast_func = UCX_INIT_BCAST_MPI,
                            .bcast_func_args = &mpi_bcast_params};

  DHT *object = DHT_create(&init_params);
  printf("%d: DHT created\n", rank);
  fflush(stdout);

  if (object == NULL) {
    fprintf(stderr, "Error while creating DHT. Aborting ...\n");
    MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
  }

  key = rank;
  data = rank;

  status = DHT_write(object, &key, &data, NULL, NULL);

  if (DHT_UCX_ERROR == status) {
    fprintf(stderr, "Error during write. Aborting ...\n");
    MPI_Abort(MPI_COMM_WORLD, status);
  }

  DHT_barrier(object);
  printf("Leaving second barrier.\n");
  fflush(stdout);

  MPI_Barrier(MPI_COMM_WORLD);

  key = (rank + 1) % comm_size;

  status = DHT_read(object, &key, &data);

  if (DHT_UCX_ERROR == status) {
    fprintf(stderr, "Error during read. Aborting ...\n");
    MPI_Abort(MPI_COMM_WORLD, status);
    return 1;
  }

  if (key != data) {
    fprintf(stdout, "Rank %d: FAILURE!\n", rank);
  } else {
    fprintf(stdout, "Rank %d: OK!\n", rank);
  }

  uint64_t read, evic, checksum;
  status = DHT_free(object, &evic, &read, &checksum);
  if (DHT_SUCCESS != status) {
    fprintf(stderr, "Error during free. Aborting ...\n");
    MPI_Abort(MPI_COMM_WORLD, status);
  }

  if (rank == 0) {
    printf("\nEvicted: %" PRIu64 "\nRead: %" PRIu64 "\nChecksum: %" PRIu64
           "\n\n",
           evic, read, checksum);
  }

  MPI_Finalize();
}
