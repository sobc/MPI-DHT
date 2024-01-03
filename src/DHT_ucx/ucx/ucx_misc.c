#include "DHT_ucx/DHT.h"
#include "ucx_lib.h"

#include <stdint.h>
#include <stdio.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

#define PROGRESS_BOTH_WORKER(ucx_h)                                            \
  {                                                                            \
    ucp_worker_progress(ucx_h->ptp_h.ucp_worker);                              \
    ucp_worker_progress(ucx_h->rma_h.c_w_ep_h.ucp_worker);                     \
  }

ucs_status_t ucx_check_and_wait_completion(const ucx_handle_t *ucx_h,
                                           ucs_status_ptr_t *request, int imm,
                                           const ucp_worker_h *worker,
                                           uint8_t worker_arr_size) {
  if (unlikely(UCS_PTR_IS_ERR(request))) {
    return UCS_PTR_STATUS(request);
  }
  if (request != NULL) {
    ucs_status_t status = UCS_OK;
    if (!!imm) {
      do {
        for (uint8_t i = 0; i < worker_arr_size; i++) {
          ucp_worker_progress(worker[i]);
        }
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

ucs_status_t ucx_flush_ep(const ucx_handle_t *ucx_h, const ucp_ep_h *ep_list,
                          int rank, const ucp_worker_h *worker,
                          uint8_t worker_arr_size) {

  ucs_status_ptr_t *req;
  ucp_request_param_t flush_param = {.op_attr_mask = 0};

  req = ucp_ep_flush_nbx(ep_list[rank], &flush_param);

  return ucx_check_and_wait_completion(ucx_h, req, CHECK_WAIT, worker,
                                       worker_arr_size);
}

// ucs_status_t ucx_flush_worker(const ucx_handle_t *ucx_h,
//                               const ucp_worker_h *worker) {
//   ucs_status_ptr_t *req;
//   ucp_request_param_t flush_param = {.op_attr_mask = 0};

//   req = ucp_worker_flush_nbx(*worker, &flush_param);

//   return ucx_check_and_wait_completion(ucx_h, req, CHECK_WAIT);
// }