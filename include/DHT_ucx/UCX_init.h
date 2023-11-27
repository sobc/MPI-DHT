#ifndef UCX_INIT_H_
#define UCX_INIT_H_

#include <mpi.h>
#include <stdint.h>
#include <stdlib.h>
#include <ucp/api/ucp_def.h>

#define UCX_BCAST_NOT_RUN -1
#define UCX_BCAST_OK 0
#define UCX_BCAST_ERR 1

typedef struct ucx_ep_info {
  ucp_address_t **worker_addr;
  uint32_t comm_size;
  uint32_t self_rank;
} ucx_ep_info_t;

static inline void ucx_free_ep_info(ucx_ep_info_t *ep_info) {
  for (uint32_t i = 0; i < ep_info->comm_size; i++) {
    free(ep_info->worker_addr[i]);
  }
  free(ep_info->worker_addr);
}

typedef int (*ucx_worker_addr_bcast)(ucp_address_t *worker_addr_self,
                                     uint64_t worker_addr_self_len,
                                     void *func_args,
                                     ucx_ep_info_t *endpoint_info);

typedef struct ucx_ep_args_mpi {
  MPI_Comm comm;
  int comm_size;
  int rank;
} ucx_ep_args_mpi_t;

int ucx_worker_bcast_mpi(ucp_address_t *worker_addr_self,
                         uint64_t worker_addr_self_len, void *func_args,
                         ucx_ep_info_t *endpoint_info);

#endif // UCX_INIT_H_
