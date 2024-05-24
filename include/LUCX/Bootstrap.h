#ifndef UCX_BCAST_FUNCTIONS_H_
#define UCX_BCAST_FUNCTIONS_H_

#include <stdint.h>
#include <ucp/api/ucp.h>

#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

#define UCX_BCAST_NOT_RUN -1
#define UCX_BCAST_OK 0
#define UCX_BCAST_ERR 1

typedef struct ucx_ep_info {
  ucp_address_t **worker_addr;
  uint32_t comm_size;
  uint32_t self_rank;
} ucx_ep_info_t;

typedef int (*ucx_worker_addr_bootstrap)(ucp_address_t *worker_addr_self,
                                         uint64_t worker_addr_self_len,
                                         const void *func_args,
                                         ucx_ep_info_t *endpoint_info);

#ifdef DHT_USE_MPI
#include <mpi.h>
// define init function by MPI here
typedef struct ucx_ep_args_mpi {
  MPI_Comm comm;
} ucx_ep_args_mpi_t;

int ucx_worker_bootstrap_mpi(ucp_address_t *worker_addr_self,
                             uint64_t worker_addr_self_len,
                             const void *func_args,
                             ucx_ep_info_t *endpoint_info);

#define UCX_INIT_BSTRAP_MPI ucx_worker_bootstrap_mpi

#endif

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

#endif // UCX_BCAST_FUNCTIONS_H_