#ifndef UCX_INIT_H_
#define UCX_INIT_H_

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

typedef int (*ucx_worker_addr_bcast)(ucp_address_t *worker_addr_self,
                                     uint64_t worker_addr_self_len,
                                     void *func_args,
                                     ucx_ep_info_t *endpoint_info);

#endif // UCX_INIT_H_
