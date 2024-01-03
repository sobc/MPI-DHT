#include "../macros.h"
#include "DHT_ucx/DHT.h"
#include "ucx_lib.h"

#include <stdint.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

#define UNUSED __attribute__((unused))

void empty_callback(UNUSED void *request, UNUSED ucs_status_t status,
                    UNUSED void *user_data) {}

const ucp_request_param_t putget_param = {
    .op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE | UCP_OP_ATTR_FIELD_CALLBACK,
    .datatype = ucp_dt_make_contig(1),
    .cb.send = empty_callback};

ucs_status_ptr_t *ucx_put_nonblocking(const ucx_handle_t *ucx_h, int rank,
                                      uint64_t offset, const void *buffer,
                                      uint64_t count) {

  const uint64_t remote_addr = ucx_h->rma_h.remote_addr[rank] + offset;

  return ucp_put_nbx(ucx_h->rma_h.c_w_ep_h.ep_list[rank], buffer, count,
                     remote_addr, ucx_h->rma_h.rkey_handles[rank],
                     &putget_param);
}

ucs_status_ptr_t *ucx_get_nonblocking(const ucx_handle_t *ucx_h, int rank,
                                      uint64_t offset, void *buffer,
                                      uint64_t count) {

  const uint64_t remote_addr = ucx_h->rma_h.remote_addr[rank] + offset;

  return ucp_get_nbx(ucx_h->rma_h.c_w_ep_h.ep_list[rank], buffer, count,
                     remote_addr, ucx_h->rma_h.rkey_handles[rank],
                     &putget_param);
}

ucs_status_t ucx_put_blocking(const ucx_handle_t *ucx_h, int rank,
                              uint64_t offset, const void *buffer,
                              uint64_t count) {
  ucs_status_ptr_t req =
      ucx_put_nonblocking(ucx_h, rank, offset, buffer, count);

  ucs_status_t status;

  status = ucx_check_and_wait_completion(ucx_h, req, CHECK_WAIT,
                                         &ucx_h->rma_h.c_w_ep_h.ucp_worker, 1);

  if (UCS_OK != status) {
    return status;
  }

  return status;

  // return ucx_flush_ep(ucx_h, ucx_h->rma_h.c_w_ep_h.ep_list, rank,
  //                     UCX_PROGRESS_RMA);
}

ucs_status_t ucx_get_blocking(const ucx_handle_t *ucx_h, int rank,
                              uint64_t offset, void *buffer, uint64_t count) {
  ucs_status_ptr_t req =
      ucx_get_nonblocking(ucx_h, rank, offset, buffer, count);

  ucs_status_t status;

  status = ucx_check_and_wait_completion(ucx_h, req, CHECK_WAIT,
                                         &ucx_h->rma_h.c_w_ep_h.ucp_worker, 1);

  if (UCS_OK != status) {
    return status;
  }

  return status;

  // return ucx_flush_ep(ucx_h, ucx_h->rma_h.c_w_ep_h.ep_list, rank,
  //                     UCX_PROGRESS_RMA);
}

ucs_status_t ucx_write_acquire_lock(ucx_handle_t *ucx_h, int rank,
                                    uint64_t offset) {
  const uint32_t unacquired = BUCKET_UNLOCK;
  uint32_t cswap_val;

  ucx_h->lock_h.lock_rem_addr = ucx_h->rma_h.remote_addr[rank] + offset;

  ucx_h->lock_h.rank = rank;

  ucp_request_param_t cswap_param;
  cswap_param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE |
                             UCP_OP_ATTR_FIELD_REPLY_BUFFER |
                             UCP_OP_ATTR_FIELD_CALLBACK;
  cswap_param.datatype = ucp_dt_make_contig(4);
  cswap_param.reply_buffer = &cswap_val;
  cswap_param.cb.send = empty_callback;

  do {
    cswap_val = BUCKET_LOCK;

    ucs_status_ptr_t request = ucp_atomic_op_nbx(
        ucx_h->rma_h.c_w_ep_h.ep_list[rank], UCP_ATOMIC_OP_CSWAP, &unacquired,
        1, ucx_h->lock_h.lock_rem_addr, ucx_h->rma_h.rkey_handles[rank],
        &cswap_param);

    ucs_status_t status = ucx_check_and_wait_completion(
        ucx_h, request, CHECK_WAIT, &ucx_h->rma_h.c_w_ep_h.ucp_worker, 1);
    if (unlikely(status != UCS_OK)) {
      return status;
    }

    // ucs_status_t status_flush = ucx_flush_ep(
    //     ucx_h, ucx_h->rma_h.c_w_ep_h.ep_list, rank, UCX_PROGRESS_RMA);
    // if (unlikely(status_flush != UCS_OK)) {
    //   return status_flush;
    // }

  } while (cswap_val != BUCKET_LOCK);

  return UCS_OK;
}

ucs_status_t ucx_write_release_lock(const ucx_handle_t *ucx_h) {
  const uint32_t unlock = BUCKET_UNLOCK;
  uint32_t cswap_val;

  ucp_request_param_t add_param;
  add_param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE |
                           UCP_OP_ATTR_FIELD_CALLBACK |
                           UCP_OP_ATTR_FIELD_REPLY_BUFFER;
  add_param.datatype = ucp_dt_make_contig(4);
  add_param.cb.send = empty_callback;
  add_param.reply_buffer = &cswap_val;

  ucs_status_ptr_t request = ucp_atomic_op_nbx(
      ucx_h->rma_h.c_w_ep_h.ep_list[ucx_h->lock_h.rank], UCP_ATOMIC_OP_SWAP,
      &unlock, 1, ucx_h->lock_h.lock_rem_addr,
      ucx_h->rma_h.rkey_handles[ucx_h->lock_h.rank], &add_param);

  ucs_status_t status = ucx_check_and_wait_completion(
      ucx_h, request, CHECK_WAIT, &ucx_h->rma_h.c_w_ep_h.ucp_worker, 1);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  // ucs_status_t status_flush =
  //     ucx_flush_ep(ucx_h, ucx_h->rma_h.c_w_ep_h.ep_list, ucx_h->lock_h.rank);
  // if (unlikely(status_flush != UCS_OK)) {
  //   return status_flush;
  // }

  return UCS_OK;
}
