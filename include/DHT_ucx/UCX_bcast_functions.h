#ifndef UCX_BCAST_FUNCTIONS_H_
#define UCX_BCAST_FUNCTIONS_H_

#include "UCX_init.h"

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
                         uint64_t worker_addr_self_len, void *func_args,
                         ucx_ep_info_t *endpoint_info);

#endif // UCX_BCAST_FUNCTIONS_H_