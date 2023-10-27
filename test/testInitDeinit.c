#include <mpi.h>

#include <DHT/DHT.h>

int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);

  DHT *object = DHT_create(MPI_COMM_WORLD, 50, 20, 20, NULL);

  DHT_free(object, NULL, NULL);

  MPI_Finalize();
}
