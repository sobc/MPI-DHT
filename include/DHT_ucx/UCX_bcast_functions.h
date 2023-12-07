#ifndef UCX_BCAST_FUNCTIONS_H_
#define UCX_BCAST_FUNCTIONS_H_

#include "DHT.h"

#include <mpi.h>
#include <stdint.h>
#include <stdlib.h>
#include <ucp/api/ucp.h>

typedef struct ucx_ep_args_mpi {
  MPI_Comm comm;
  int comm_size;
  int rank;
} ucx_ep_args_mpi_t;

int ucx_worker_bcast_mpi(ucp_address_t *worker_addr_self,
                         uint64_t worker_addr_self_len, const void *func_args,
                         ucx_ep_info_t *endpoint_info);

#define UCX_INIT_BCAST_MPI ucx_worker_bcast_mpi

#endif // UCX_BCAST_FUNCTIONS_H_