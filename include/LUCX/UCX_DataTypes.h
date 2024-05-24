#ifndef __UCX_DATATYPES_H__
#define __UCX_DATATYPES_H__

#include <stdint.h>
#include <ucp/api/ucp.h>

struct ucx_handle_lock {
  uint64_t lock_rem_addr;
  int rank;
};

struct ucx_c_w_ep_handle {
  ucp_context_h ucp_context;
  ucp_worker_h ucp_worker;
  ucp_ep_h *ep_list;
};

struct ucx_rma_handle {
  struct ucx_c_w_ep_handle c_w_ep_h;
  ucp_mem_h mem_h;
  uint64_t local_mem_addr;
  uint64_t *remote_addr;
  void **rkey_buffer;
  ucp_rkey_h *rkey_handles;
};

typedef struct ucx_handle {
  struct ucx_c_w_ep_handle ptp_h;
  struct ucx_rma_handle rma_h;
  struct ucx_handle_lock lock_h;
  uint32_t lock_size;
  uint32_t comm_size;
  uint32_t self_rank;
} ucx_handle_t;

#endif // __UCX_DATATYPES_H__