#include <DHT/DHT.h>
#include <mpi.h>
#include <stdint.h>
#include <stdio.h>
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

  int rank;

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  DHT *object = DHT_create(MPI_COMM_WORLD, 50, 4, 4, xxhashWrapper);

  fprintf(stdout, "Rank %d: Initialization done!\n", rank);
  fflush(stdout);

  sleep(5);

  if (rank == 0) {
    uint32_t key = 123;
    uint32_t data = 456;

    if (DHT_SUCCESS != DHT_write(object, &key, &data, NULL, NULL)) {
      fprintf(stderr, "Calling write\n");
      fflush(stderr);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  sleep(2);

  if (rank == 1) {

    uint32_t key = 123;
    uint32_t data;

    int status = DHT_read(object, &key, &data);

    if (DHT_MPI_ERROR == status) {
      fprintf(stderr, "ret = %d\n", status);
      return 1;
    }

    printf("Read data is: %d\n", data);
  }


  sleep(2);

  DHT_free(object, NULL, NULL);

  MPI_Finalize();
}
