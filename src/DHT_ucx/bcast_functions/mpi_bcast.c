#include "../macros.h"

#include <DHT_ucx/UCX_bcast_functions.h>
#include <DHT_ucx/UCX_init.h>
#include <mpi.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int ucx_worker_bcast_mpi(ucp_address_t *worker_addr_self,
                         uint64_t worker_addr_self_len, void *func_args,
                         ucx_ep_info_t *endpoint_info) {
  int status;
  const ucx_ep_args_mpi_t *mpi_args = (ucx_ep_args_mpi_t *)func_args;
  int addr_lengths[mpi_args->comm_size];
  int worker_addr_len_casted = (int)worker_addr_self_len;

  status = MPI_Allgather(&worker_addr_len_casted, 1, MPI_INT, addr_lengths, 1,
                         MPI_INT, mpi_args->comm);
  if (unlikely(status != MPI_SUCCESS)) {
    return UCX_BCAST_ERR;
  }

  endpoint_info->worker_addr =
      malloc(mpi_args->comm_size * sizeof(ucp_address_t *));
  if (unlikely(endpoint_info->worker_addr == NULL)) {
    return UCX_BCAST_ERR;
  }

  for (int i = 0; i < mpi_args->comm_size; i++) {
    endpoint_info->worker_addr[i] = malloc(addr_lengths[i]);
    if (unlikely(endpoint_info->worker_addr[i] == NULL)) {
      return UCX_BCAST_ERR;
    }

    if (i == mpi_args->rank) {
      memcpy(endpoint_info->worker_addr[i], worker_addr_self,
             worker_addr_len_casted);
    }

    status = MPI_Bcast(endpoint_info->worker_addr[i], worker_addr_len_casted,
                       MPI_BYTE, i, mpi_args->comm);
    if (unlikely(status != MPI_SUCCESS)) {
      return UCX_BCAST_ERR;
    }
  }

  endpoint_info->comm_size = mpi_args->comm_size;
  endpoint_info->self_rank = mpi_args->rank;

  return UCX_BCAST_OK;
}
