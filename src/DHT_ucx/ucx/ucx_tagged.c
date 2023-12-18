#include "ucx_lib.h"

#include <stdint.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

const ucp_request_param_t tag_param = {.op_attr_mask = 0};

static ucs_status_t send_msg_w_tag(const ucx_handle_t *ucx_h, uint32_t dest,
                                   void *msg, uint64_t msg_size,
                                   uint64_t msg_tag) {
  ucs_status_t status;
  ucs_status_ptr_t request;

  request = ucp_tag_send_sync_nbx(ucx_h->ep_list[dest], msg, msg_size, msg_tag,
                                  &tag_param);

  status = ucx_check_and_wait_completion(ucx_h, request, CHECK_WAIT);
  if (UCS_OK != status) {
    return status;
  }

  return UCS_OK;
}

static ucs_status_t recv_msg_w_tag(const ucx_handle_t *ucx_h, void *buf,
                                   uint64_t msg_tag, uint64_t tag_mask) {
  ucs_status_t status;
  ucs_status_ptr_t request;
  ucp_tag_message_h msg_h;
  ucp_tag_recv_info_t msg_info;

  do {
    ucp_worker_progress(ucx_h->ucp_worker);

    msg_h =
        ucp_tag_probe_nb(ucx_h->ucp_worker, msg_tag, tag_mask, 1, &msg_info);
  } while (msg_h == NULL);

  request = ucp_tag_msg_recv_nbx(ucx_h->ucp_worker, buf, msg_info.length, msg_h,
                                 &tag_param);

  status = ucx_check_and_wait_completion(ucx_h, request, CHECK_WAIT);
  if (unlikely(UCS_OK != status)) {
    return status;
  }

  return UCS_OK;
}

ucs_status_t ucx_broadcast(const ucx_handle_t *ucx_h, uint64_t root, void *msg,
                           uint64_t msg_size, uint32_t msg_tag) {
  ucs_status_t status = UCS_OK;
  uint64_t expected_tag = root << 32 | msg_tag;

  if (root == ucx_h->self_rank) {

    for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
      if (i == ucx_h->self_rank) {
        continue;
      }

      status = send_msg_w_tag(ucx_h, i, msg, msg_size, expected_tag);
      if (UCS_OK != status) {
        return status;
      }
    }
  } else {

    status = recv_msg_w_tag(ucx_h, msg, expected_tag, UINT64_MAX);

    if (UCS_OK != status) {
      return status;
    }
  }

  return status;
}

ucs_status_t ucx_reduce_sum(const ucx_handle_t *ucx_h, int64_t *buf,
                            uint32_t root) {
  ucs_status_t status = UCS_OK;

  status = ucx_flush_worker(ucx_h);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  if (root == ucx_h->self_rank) {
    int64_t sum = 0;
    int64_t rem_buf;
    for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
      if (i == ucx_h->self_rank) {
        sum += *buf;
        continue;
      }

      status = recv_msg_w_tag(ucx_h, &rem_buf, i, UINT32_MAX);
      if (unlikely(UCS_OK != status)) {
        return status;
      }

      sum += rem_buf;
    }

    *buf = sum;
  } else {
    status =
        send_msg_w_tag(ucx_h, root, buf, sizeof(int64_t), ucx_h->self_rank);

    if (unlikely(UCS_OK != status)) {
      return status;
    }
  }

  return status;
}