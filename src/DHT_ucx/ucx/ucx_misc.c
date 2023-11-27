#include "ucx_lib.h"

#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

ucs_status_t ucx_check_and_wait_completion(const ucx_handle_t *ucx_h,
                                           ucs_status_ptr_t *request, int imm) {
  if (unlikely(UCS_PTR_IS_ERR(request))) {
    return UCS_PTR_STATUS(request);
  }
  if (request != NULL) {
    ucs_status_t status = UCS_OK;
    if (!!imm) {
      do {
        ucp_worker_progress(ucx_h->ucp_worker);
        status = ucp_request_check_status(request);
      } while (status == UCS_INPROGRESS);
    }
    ucp_request_free(request);

    if (status != UCS_OK) {
      return status;
    }
  }

  return UCS_OK;
}

ucs_status_t ucx_flush_ep(const ucx_handle_t *ucx_h, int rank) {
  ucs_status_ptr_t *req;
  ucp_request_param_t flush_param = {.op_attr_mask = 0};

  req = ucp_ep_flush_nbx(ucx_h->ep_list[rank], &flush_param);

  return ucx_check_and_wait_completion(ucx_h, req, CHECK_WAIT);
}

ucs_status_t ucx_flush_worker(const ucx_handle_t *ucx_h) {
  ucs_status_ptr_t *req;
  ucp_request_param_t flush_param = {.op_attr_mask = 0};

  req = ucp_worker_flush_nbx(ucx_h->ucp_worker, &flush_param);

  return ucx_check_and_wait_completion(ucx_h, req, CHECK_WAIT);
}

ucs_status_t ucx_barrier(const ucx_handle_t *ucx_h) {
  uint32_t msg = 0;

  ucs_status_t status;

  status = ucx_flush_worker(ucx_h);
  if (status != UCS_OK) {
    return status;
  }

  for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
    if (i == ucx_h->self_rank) {
      msg = i;
    }

    status = ucx_broadcast(ucx_h, i, &msg, sizeof(msg), i);
    if (status != UCS_OK) {
      return status;
    }

    if (msg != i) {
      return UCS_ERR_INVALID_PARAM;
    }
  }

  // Maybe there are some outstanding operations after all processes
  // synchronized ...
  int processed = 0;

  do {
    processed = ucp_worker_progress(ucx_h->ucp_worker);
  } while (processed != 0);

  return UCS_OK;
}