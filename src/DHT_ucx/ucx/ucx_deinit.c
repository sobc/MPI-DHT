#include "DHT_ucx/DHT.h"
#include "ucx_lib.h"

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
  ucx_releaseRKeys(ucx_h->rkey_handles, ucx_h->rkey_buffer, ucx_h->remote_addr,
                   ucx_h->comm_size);

  return ucp_mem_unmap(ucx_h->ucp_context, ucx_h->mem_h);
}

void ucx_releaseEndpoints(ucx_handle_t *ucx_h) {

  ucs_status_t status;
  ucs_status_ptr_t request;
  ucp_request_param_t req_param = {.op_attr_mask = 0};

  for (int i = 0; i < ucx_h->comm_size; i++) {
    request = ucp_ep_close_nbx(ucx_h->ep_list[i], &req_param);

    if (UCS_PTR_IS_PTR(request)) {
      do {
        ucp_worker_progress(ucx_h->ucp_worker);
        status = ucp_request_check_status(request);
      } while (status == UCS_INPROGRESS);
      ucp_request_free(request);
    }
  }

  free(ucx_h->ep_list);
}

void ucx_finalize(ucx_handle_t *ucx_h) {
  ucp_worker_destroy(ucx_h->ucp_worker);
  ucp_cleanup(ucx_h->ucp_context);

  free(ucx_h);
}