#include "../macros.h"

#include <LUCX/UCX_DataTypes.h>
#include <LUCX/UCX_bcast_functions.h>
#include <mpi.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int ucx_worker_bootstrap_mpi(ucp_address_t *worker_addr_self,
                             uint64_t worker_addr_self_len,
                             const void *func_args,
                             ucx_ep_info_t *endpoint_info) {
  int status;
  int comm_size;
  int rank;

  const ucx_ep_args_mpi_t *mpi_args = (ucx_ep_args_mpi_t *)func_args;
  MPI_Comm_size(mpi_args->comm, &comm_size);
  MPI_Comm_rank(mpi_args->comm, &rank);

  int addr_lengths[comm_size];
  int worker_addr_len_casted = (int)worker_addr_self_len;

  status = MPI_Allgather(&worker_addr_len_casted, 1, MPI_INT, addr_lengths, 1,
                         MPI_INT, mpi_args->comm);
  if (unlikely(status != MPI_SUCCESS)) {
    return UCX_BCAST_ERR;
  }

  endpoint_info->worker_addr = malloc(comm_size * sizeof(ucp_address_t *));
  if (unlikely(endpoint_info->worker_addr == NULL)) {
    return UCX_BCAST_ERR;
  }

  for (int i = 0; i < comm_size; i++) {
    endpoint_info->worker_addr[i] = malloc(addr_lengths[i]);
    if (unlikely(endpoint_info->worker_addr[i] == NULL)) {
      return UCX_BCAST_ERR;
    }

    if (i == rank) {
      memcpy(endpoint_info->worker_addr[i], worker_addr_self,
             worker_addr_len_casted);
    }

    status = MPI_Bcast(endpoint_info->worker_addr[i], addr_lengths[i], MPI_BYTE,
                       i, mpi_args->comm);
    if (unlikely(status != MPI_SUCCESS)) {
      return UCX_BCAST_ERR;
    }
  }

  endpoint_info->comm_size = comm_size;
  endpoint_info->self_rank = rank;

  return UCX_BCAST_OK;
}
