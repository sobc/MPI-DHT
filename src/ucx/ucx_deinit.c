#include "ucx_lib.h"

#include <stdlib.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

static inline void ucx_releaseRKeys(ucp_rkey_h *rkey_handles,
                                    void **rkey_buffer, uint64_t *rem_addresses,
                                    int rkey_count) {
  for (int i = 0; i < rkey_count; i++) {
    ucp_rkey_destroy(rkey_handles[i]);
    ucp_rkey_buffer_release(rkey_buffer[i]);
  }

  free(rkey_handles);
  free(rkey_buffer);
  free(rem_addresses);
}

ucs_status_t ucx_free_mem(ucx_handle_t *ucx_h) {
  ucx_releaseRKeys(ucx_h->rma_h.rkey_handles, ucx_h->rma_h.rkey_buffer,
                   ucx_h->rma_h.remote_addr, ucx_h->comm_size);

  return ucp_mem_unmap(ucx_h->rma_h.c_w_ep_h.ucp_context, ucx_h->rma_h.mem_h);
}

void ucx_releaseEndpoints(struct ucx_c_w_ep_handle *c_w_ep_h,
                          uint32_t comm_size) {

  ucs_status_t status;
  ucs_status_ptr_t request;
  ucp_request_param_t req_param = {.op_attr_mask = 0};

  for (uint32_t i = 0; i < comm_size; i++) {
    request = ucp_ep_close_nbx(c_w_ep_h->ep_list[i], &req_param);

    if (UCS_PTR_IS_PTR(request)) {
      do {
        ucp_worker_progress(c_w_ep_h->ucp_worker);
        status = ucp_request_check_status(request);
      } while (status == UCS_INPROGRESS);
      ucp_request_free(request);
    }
  }

  free(c_w_ep_h->ep_list);
}

void ucx_finalize(ucx_handle_t *ucx_h) {
  ucp_worker_destroy(ucx_h->ptp_h.ucp_worker);
  ucp_cleanup(ucx_h->ptp_h.ucp_context);

  ucp_worker_destroy(ucx_h->rma_h.c_w_ep_h.ucp_worker);
  ucp_cleanup(ucx_h->rma_h.c_w_ep_h.ucp_context);

  free(ucx_h);
}