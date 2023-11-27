#include "../dht_macros.h"
#include "DHT_ucx/DHT.h"
#include "ucx_lib.h"

#include <alloca.h>
#include <stdint.h>
#include <stdlib.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

const ucp_request_param_t putget_param = {.op_attr_mask =
                                              UCP_OP_ATTR_FIELD_DATATYPE,
                                          .datatype = ucp_dt_make_contig(1)};

ucs_status_ptr_t *ucx_put_nonblocking(const ucx_handle_t *ucx_h, int rank,
                                      uint64_t index, uint32_t displacement,
                                      uint64_t count, const void *buffer) {
  const uint64_t remote_address =
      ucx_h->remote_addr[rank] + (ucx_h->offset * index) + displacement;

  return ucp_put_nbx(ucx_h->ep_list[rank], buffer, count, remote_address,
                     ucx_h->rkey_handles[rank], &putget_param);
}

ucs_status_ptr_t *ucx_get_nonblocking(const ucx_handle_t *ucx_h, int rank,
                                      uint64_t index, uint32_t displacement,
                                      uint64_t count, void *buffer) {
  const uint64_t remote_address =
      ucx_h->remote_addr[rank] + (ucx_h->offset * index) + displacement;

  return ucp_get_nbx(ucx_h->ep_list[rank], buffer, count, remote_address,
                     ucx_h->rkey_handles[rank], &putget_param);
}

ucs_status_t ucx_put_blocking(const ucx_handle_t *ucx_h, int rank,
                              uint64_t index, uint32_t displacement,
                              const void *buffer, uint64_t count) {
  ucs_status_ptr_t req =
      ucx_put_nonblocking(ucx_h, rank, index, displacement, count, buffer);

  ucs_status_t status;

  status = ucx_check_and_wait_completion(ucx_h, req, CHECK_NO_WAIT);

  if (UCS_OK != status) {
    return status;
  }

  return ucx_flush_ep(ucx_h, rank);
}

ucs_status_t ucx_get_blocking(const ucx_handle_t *ucx_h, int rank,
                              uint64_t index, uint32_t displacement,
                              void *buffer, uint64_t count) {
  ucs_status_ptr_t req =
      ucx_get_nonblocking(ucx_h, rank, index, displacement, count, buffer);

  ucs_status_t status;

  status = ucx_check_and_wait_completion(ucx_h, req, CHECK_NO_WAIT);

  if (UCS_OK != status) {
    return status;
  }

  return ucx_flush_ep(ucx_h, rank);
}

ucs_status_t ucx_write_acquire_lock(ucx_handle_t *ctx_h, uint64_t index,
                                    int rank, uint8_t lock_displacement) {
  const uint32_t unacquired = BUCKET_UNLOCK;
  uint32_t cswap_val;

  ctx_h->lock_h.lock_rem_addr =
      ctx_h->remote_addr[rank] + (ctx_h->offset * index) + lock_displacement;

  ctx_h->lock_h.rank = rank;

  ucp_request_param_t cswap_param;
  cswap_param.op_attr_mask =
      UCP_OP_ATTR_FIELD_DATATYPE | UCP_OP_ATTR_FIELD_REPLY_BUFFER;
  cswap_param.datatype = ucp_dt_make_contig(4);
  cswap_param.reply_buffer = &cswap_val;

  do {
    cswap_val = BUCKET_LOCK;

    ucs_status_ptr_t request = ucp_atomic_op_nbx(
        ctx_h->ep_list[rank], UCP_ATOMIC_OP_CSWAP, &unacquired, 1,
        ctx_h->lock_h.lock_rem_addr, ctx_h->rkey_handles[rank], &cswap_param);

    ucs_status_t status =
        ucx_check_and_wait_completion(ctx_h, request, CHECK_NO_WAIT);
    if (unlikely(status != UCS_OK)) {
      return status;
    }

    ucs_status_t status_flush = ucx_flush_ep(ctx_h, rank);
    if (unlikely(status_flush != UCS_OK)) {
      return status_flush;
    }

  } while (cswap_val != BUCKET_LOCK);

  return UCS_OK;
}

ucs_status_t ucx_write_release_lock(const ucx_handle_t *ucx_h) {
  const uint32_t unlock = BUCKET_UNLOCK;
  uint32_t reply_buffer;

  ucp_request_param_t add_param;
  add_param.op_attr_mask =
      UCP_OP_ATTR_FIELD_DATATYPE | UCP_OP_ATTR_FIELD_REPLY_BUFFER;
  add_param.datatype = ucp_dt_make_contig(4);
  add_param.reply_buffer = &reply_buffer;

  ucs_status_ptr_t request =
      ucp_atomic_op_nbx(ucx_h->ep_list[ucx_h->lock_h.rank], UCP_ATOMIC_OP_SWAP,
                        &unlock, 1, ucx_h->lock_h.lock_rem_addr,
                        ucx_h->rkey_handles[ucx_h->lock_h.rank], &add_param);

  ucs_status_t status =
      ucx_check_and_wait_completion(ucx_h, request, CHECK_NO_WAIT);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  ucs_status_t status_flush = ucx_flush_ep(ucx_h, ucx_h->lock_h.rank);
  if (unlikely(status_flush != UCS_OK)) {
    return status_flush;
  }

  return UCS_OK;
}
