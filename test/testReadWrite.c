#include <DHT_ucx/DHT.h>
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

  DHT *object = DHT_create(MPI_COMM_WORLD, 50, sizeof(uint32_t),
                           sizeof(uint32_t), xxhashWrapper);

  if (object == NULL) {
    fprintf(stderr, "Error while creating DHT. Aborting ...\n");
    MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
  }

  key = rank;
  data = rank;

  status = DHT_write(object, &key, &data, NULL, NULL);

  if (DHT_SUCCESS != status) {
    fprintf(stderr, "Error during write. Aborting ...\n");
    MPI_Abort(MPI_COMM_WORLD, status);
  }

  DHT_barrier(object);

  key = (rank + 1) % comm_size;

  status = DHT_read(object, &key, &data);

  if (DHT_MPI_ERROR == status) {
    fprintf(stderr, "Error during read. Aborting ...\n");
    MPI_Abort(MPI_COMM_WORLD, status);
    return 1;
  }

  if (key != data) {
    fprintf(stdout, "Rank %d: FAILURE!\n", rank);
  } else {
    fprintf(stdout, "Rank %d: OK!\n", rank);
  }

  status = DHT_free(object, NULL, NULL, NULL);
  if (DHT_SUCCESS != status) {
    fprintf(stderr, "Error during free. Aborting ...\n");
    MPI_Abort(MPI_COMM_WORLD, status);
  }

  MPI_Finalize();
}
