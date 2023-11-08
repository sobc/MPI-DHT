#include "ucx_communication.h"
#include "dht_macros.h"

#include <stdint.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

ucs_status_t ucx_write_acquire_lock(ucx_handle_t *ctx_h, uint64_t index,
                                    int rank) {
  const uint32_t unacquired = BUCKET_UNLOCK;
  uint32_t cswap_val;

  ctx_h->lock_h.lock_rem_addr =
      ctx_h->remote_addr[rank] + (ctx_h->offset * index);

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

    ucs_status_t status = ucx_check_and_wait_completion(ctx_h, request);
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

ucs_status_ptr_t *ucx_put_data(const ucx_handle_t *ucx_h, int rank,
                               uint64_t index, uint64_t count,
                               const void *buffer) {
  ucp_request_param_t put_param;
  put_param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
  put_param.datatype = ucp_dt_make_contig(1);

  return ucp_put_nbx(ucx_h->ep_list[rank], buffer, count,
                     ucx_h->remote_addr[rank] + (ucx_h->offset * index) +
                         ucx_h->lock_size + ucx_h->flag_padding,
                     ucx_h->rkey_handles[rank], &put_param);
}

ucs_status_ptr_t *ucx_get_data(const ucx_handle_t *ucx_h, int rank,
                               uint64_t index, uint64_t count, void *buffer) {
  ucp_request_param_t get_param;
  get_param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
  get_param.datatype = ucp_dt_make_contig(1);

  return ucp_get_nbx(ucx_h->ep_list[rank], buffer, count,
                     ucx_h->remote_addr[rank] + (ucx_h->offset * index) +
                         ucx_h->lock_size + ucx_h->flag_padding,
                     ucx_h->rkey_handles[rank], &get_param);
}

ucs_status_t ucx_put(const ucx_handle_t *ucx_h, int rank, uint64_t index,
                     const void *buffer, uint64_t count) {
  ucs_status_ptr_t req = ucx_put_data(ucx_h, rank, index, count, buffer);

  ucs_status_t status;

  status = ucx_check_and_wait_completion(ucx_h, req);

  if (UCS_OK != status) {
    return status;
  }

  return ucx_flush_ep(ucx_h, rank);
}

ucs_status_t ucx_get(const ucx_handle_t *ucx_h, int rank, uint64_t index,
                     void *buffer, uint64_t count) {
  ucs_status_ptr_t req = ucx_get_data(ucx_h, rank, index, count, buffer);

  ucs_status_t status;

  status = ucx_check_and_wait_completion(ucx_h, req);

  if (UCS_OK != status) {
    return status;
  }

  return ucx_flush_ep(ucx_h, rank);
}

ucs_status_t ucx_check_and_wait_completion(const ucx_handle_t *ucx_h,
                                           ucs_status_ptr_t *request) {
  if (unlikely(UCS_PTR_IS_ERR(request))) {
    return UCS_PTR_STATUS(request);
  }

  if (request != NULL) {
    /* do { */
    /*   ucp_worker_progress(ucx_h->ucp_worker); */
    /*   status = ucp_request_check_status(request); */
    /* } while (status == UCS_INPROGRESS); */
    ucp_request_free(request);
  }

  return UCS_OK;
}

ucs_status_t ucx_flush_ep(const ucx_handle_t *ucx_h, int rank) {
  ucs_status_ptr_t *req;
  ucp_request_param_t flush_param;
  flush_param.op_attr_mask = 0;

  req = ucp_worker_flush_nbx(ucx_h->ucp_worker, &flush_param);

  if (req == NULL) {
    return UCS_OK;
  }
  if (UCS_PTR_IS_ERR(req)) {
    return UCS_PTR_STATUS(req);
  }
  ucs_status_t status;
  do {
    ucp_worker_progress(ucx_h->ucp_worker);
    status = ucp_request_check_status(req);
  } while (status == UCS_INPROGRESS);
  ucp_request_free(req);
  return status;
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

  ucs_status_t status = ucx_check_and_wait_completion(ucx_h, request);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  ucs_status_t status_flush = ucx_flush_ep(ucx_h, ucx_h->lock_h.rank);
  if (unlikely(status_flush != UCS_OK)) {
    return status_flush;
  }

  return UCS_OK;
}

ucs_status_t ucx_initPostRecv(const ucx_handle_t *ucx_h, int size) {
  uint32_t data;
  ucs_status_t status;

  for (int i = 0; i < size; i++) {
    status = ucx_get(ucx_h, i, 0, &data, sizeof(data));
    if (UCS_OK != status) {
      return status;
    }
  }

  return UCS_OK;
}
