#ifndef UCX_LIB_H_
#define UCX_LIB_H_

#include "../macros.h"
#include "LUCX/Bootstrap.h"
#include "LUCX/DataTypes.h"

#include <stdint.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

#define UCX_PTP_REQ_FEAT (UCP_FEATURE_TAG)
#define UCX_RMA_REQ_FEAT (UCP_FEATURE_RMA)

#define BUCKET_LOCK ((uint64_t)0x0000000000000001UL)
#define BUCKET_UNLOCK ((uint64_t)0x0000000000000000UL)

#define CHECK_NO_WAIT 0
#define CHECK_WAIT 1

#define UCX_PROGRESS_RMA 0
#define UCX_PROGRESS_TAG 1

ucx_handle_t *ucx_init(ucx_worker_addr_bootstrap func_bcast,
                       const void *func_args, int *func_ret);

ucs_status_t ucx_init_rma(ucx_handle_t *ucx_h, uint64_t mem_size);

ucs_status_t ucx_free_mem(ucx_handle_t *ucx_h);

void ucx_releaseEndpoints(struct ucx_c_w_ep_handle *c_w_ep_h,
                          uint32_t comm_size);

void ucx_finalize(ucx_handle_t *ucx_h);

ucs_status_t ucx_write_acquire_lock(ucx_handle_t *ucx_h, int rank,
                                    uint64_t offset);

ucs_status_ptr_t *ucx_put_nonblocking(const ucx_handle_t *ucx_h, int rank,
                                      uint64_t offset, const void *buffer,
                                      uint64_t count);

ucs_status_ptr_t *ucx_get_nonblocking(const ucx_handle_t *ucx_h, int rank,
                                      uint64_t offset, void *buffer,
                                      uint64_t count);

ucs_status_t ucx_put_blocking(const ucx_handle_t *ucx_h, int rank,
                              uint64_t offset, const void *buffer,
                              uint64_t count);

ucs_status_t ucx_get_blocking(const ucx_handle_t *ucx_h, int rank,
                              uint64_t offset, void *buffer, uint64_t count);

// ucs_status_t ucx_check_and_wait_completion(const ucx_handle_t *ucx_h,
//                                            ucs_status_ptr_t *request, int
//                                            imm, const ucp_worker_h *worker,
//                                            uint8_t worker_arr_size);

ucs_status_t ucx_flush_ep(const ucx_handle_t *ucx_h, const ucp_ep_h *ep_list,
                          int rank, const ucp_worker_h *worker,
                          uint8_t worker_arr_size);

ucs_status_t ucx_flush_worker(const ucx_handle_t *ucx_h,
                              const ucp_worker_h *worker);

ucs_status_t ucx_write_release_lock(const ucx_handle_t *ucx_h);

ucs_status_t ucx_broadcast(const ucx_handle_t *ucx_h, uint64_t root, void *msg,
                           uint64_t msg_size, uint32_t msg_tag);

ucs_status_t ucx_reduce_sum(const ucx_handle_t *ucx_h, int64_t *buf,
                            uint32_t root);

ucs_status_t ucx_gather(const ucx_handle_t *ucx_h, void *send_buf,
                        uint64_t count, void *recv_buf, uint32_t root);

ucs_status_t ucx_barrier(const ucx_handle_t *ucx_h);

ucs_status_t ucx_tagged_send(const ucx_handle_t *ucx_h, uint32_t dest,
                             void *msg, uint64_t msg_size, uint32_t msg_tag);

ucs_status_t ucx_tagged_recv(const ucx_handle_t *ucx_h, uint32_t src, void *buf,
                             uint64_t msg_size, uint32_t msg_tag);

static inline ucs_status_t
ucx_check_and_wait_completion(ucs_status_ptr_t *request, int imm,
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

#endif // UCX_LIB_H_
