#include "DHT_ucx/DHT.h"
#include "ucx_lib.h"

#include <alloca.h>
#include <stdint.h>
#include <stdio.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

#define UCX_TAG_BARRIER 5
#define UCX_TAG_REDUCE 6

#define UCX_GEN_TAG(msg_tag, src) (ucp_tag_t)(src) << 32 | msg_tag

const ucp_request_param_t tag_param = {.op_attr_mask = 0};

ucs_status_t ucx_tagged_send(const ucx_handle_t *ucx_h, uint32_t dest,
                             void *msg, uint64_t msg_size, uint32_t msg_tag) {
  ucs_status_t status;
  ucs_status_ptr_t request;

  const ucp_tag_t expected_tag = UCX_GEN_TAG(msg_tag, ucx_h->self_rank);

  request = ucp_tag_send_nbx(ucx_h->ptp_h.ep_list[dest], msg, msg_size,
                             expected_tag, &tag_param);

  status = ucx_check_and_wait_completion(ucx_h, request, CHECK_WAIT);
  if (UCS_OK != status) {
    return status;
  }

  return UCS_OK;
}

ucs_status_t ucx_tagged_recv(const ucx_handle_t *ucx_h, uint32_t src, void *buf,
                             uint64_t msg_size, uint32_t msg_tag) {
  ucs_status_t status;
  ucs_status_ptr_t request;
  ucp_tag_message_h msg_h;
  ucp_tag_recv_info_t msg_info;

  ucp_context_attr_t attr;

  attr.field_mask = UCP_ATTR_FIELD_REQUEST_SIZE;

  status = ucp_context_query(ucx_h->ptp_h.ucp_context, &attr);
  if (UCS_OK != status) {
    return status;
  }

  void *req = ((char *)alloca(attr.request_size) + attr.request_size);

  const ucp_request_param_t recv_param = {
      .op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST, .request = req};

  const ucp_tag_t expected_tag = UCX_GEN_TAG(msg_tag, src);

  // do {
  //   ucp_worker_progress(ucx_h->ptp_h.ucp_worker);

  //   msg_h = ucp_tag_probe_nb(ucx_h->ptp_h.ucp_worker, msg_tag, tag_mask, 1,
  //                            &msg_info);
  // } while (msg_h == NULL);

  request = ucp_tag_recv_nbx(ucx_h->ptp_h.ucp_worker, buf, msg_size,
                             expected_tag, UINT64_MAX, &recv_param);

  do {
    ucp_worker_progress(ucx_h->ptp_h.ucp_worker);
    status = ucp_request_check_status(req);
  } while (status == UCS_INPROGRESS);

  // ucp_request_free(request);

  // status = ucx_check_and_wait_completion(&ucx_h->ptp_h.ucp_worker, request,
  //                                        CHECK_WAIT);
  if (unlikely(UCS_OK != status)) {
    return status;
  }

  return UCS_OK;
}

// ucs_status_t ucx_tagged_send(const ucx_handle_t *ucx_h, uint32_t dest,
//                              void *msg, uint64_t msg_size, uint64_t msg_tag)
//                              {
//   ucs_status_t status;
//   ucs_status_ptr_t request;

//   request = ucp_tag_send_nbx(ucx_h->ptp_h.ep_list[dest], msg, msg_size,
//   msg_tag,
//                              &tag_param);

//   status = ucx_check_and_wait_completion(&ucx_h->ptp_h.ucp_worker, request,
//                                          CHECK_WAIT);
//   if (UCS_OK != status) {
//     return status;
//   }

//   return UCS_OK;
// }

// ucs_status_t ucx_tagged_recv(const ucx_handle_t *ucx_h, void *buf,
//                              uint64_t msg_tag, uint64_t tag_mask) {
//   ucs_status_t status;
//   ucs_status_ptr_t request;
//   ucp_tag_message_h msg_h;
//   ucp_tag_recv_info_t msg_info;

//   ucp_context_attr_t attr;

//   attr.field_mask = UCP_ATTR_FIELD_REQUEST_SIZE;

//   status = ucp_context_query(ucx_h->ptp_h.ucp_context, &attr);
//   if (UCS_OK != status) {
//     return status;
//   }

//   void *req = ((char *)alloca(attr.request_size) + attr.request_size);

//   const ucp_request_param_t recv_param = {
//       .op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST, .request = req};

//   do {
//     ucp_worker_progress(ucx_h->ptp_h.ucp_worker);

//     msg_h = ucp_tag_probe_nb(ucx_h->ptp_h.ucp_worker, msg_tag, tag_mask, 1,
//                              &msg_info);
//   } while (msg_h == NULL);

//   request = ucp_tag_msg_recv_nbx(ucx_h->ptp_h.ucp_worker, buf,
//   msg_info.length,
//                                  msg_h, &recv_param);

//   do {
//     status = ucp_request_check_status(req);
//   } while (status == UCS_INPROGRESS);

//   // status = ucx_check_and_wait_completion(&ucx_h->ptp_h.ucp_worker,
//   request,
//   //                                        CHECK_WAIT);
//   if (unlikely(UCS_OK != status)) {
//     return status;
//   }

//   return UCS_OK;
// }

ucs_status_t ucx_broadcast(const ucx_handle_t *ucx_h, uint64_t root, void *msg,
                           uint64_t msg_size, uint32_t msg_tag) {
  ucs_status_t status = UCS_OK;
  uint64_t expected_tag = root << 32 | msg_tag;

  if (root == ucx_h->self_rank) {

    ucs_status_ptr_t requests[ucx_h->comm_size];

    for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
      if (i == ucx_h->self_rank) {
        continue;
      }

      status = ucx_tagged_send(ucx_h, i, msg, msg_size, msg_tag);
      if (UCS_OK != status) {
        return status;
      }

      // requests[i] = ucp_tag_send_nbx(ucx_h->ptp_h.ep_list[i], msg, msg_size,
      //                                expected_tag, &tag_param);

      // if (UCS_PTR_IS_ERR(requests[i])) {
      //   return UCS_PTR_STATUS(requests[i]);
      // }
      // status = send_msg_w_tag(ucx_h, i, msg, msg_size, expected_tag);
      // if (UCS_OK != status) {
      //   return status;
      // }
    }

    // for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
    //   if (i == ucx_h->self_rank) {
    //     continue;
    //   }

    //   status = ucx_check_and_wait_completion(&ucx_h->ptp_h.ucp_worker,
    //                                          requests[i], CHECK_WAIT);
    //   if (UCS_OK != status) {
    //     return status;
    //   }

    // status = send_msg_w_tag(ucx_h, i, msg, msg_size, expected_tag);
    // if (UCS_OK != status) {
    //   return status;
    // }

  } else {

    status = ucx_tagged_recv(ucx_h, root, msg, msg_size, msg_tag);

    // printf("rank %d : recvd\n", ucx_h->self_rank);
    // fflush(stdout);

    if (UCS_OK != status) {
      return status;
    }
  }

  return status;
}

ucs_status_t ucx_reduce_sum(const ucx_handle_t *ucx_h, int64_t *buf,
                            uint32_t root) {
  ucs_status_t status = UCS_OK;

  // status = ucx_flush_worker(ucx_h, &ucx_h->ptp_h.ucp_worker);
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

      status =
          ucx_tagged_recv(ucx_h, i, &rem_buf, sizeof(uint64_t), UCX_TAG_REDUCE);
      if (unlikely(UCS_OK != status)) {
        return status;
      }

      sum += rem_buf;
    }

    *buf = sum;
  } else {
    status =
        ucx_tagged_send(ucx_h, root, buf, sizeof(uint64_t), UCX_TAG_REDUCE);

    if (unlikely(UCS_OK != status)) {
      return status;
    }
  }

  return status;
}

static ucs_status_t barrier_ring(const ucx_handle_t *ucx_h, const uint32_t rank,
                                 const uint32_t comm_size) {
  const uint32_t left = (comm_size + rank - 1) % comm_size;
  const uint32_t right = (rank + 1) % comm_size;

  ucs_status_t status = UCS_OK;

  if (rank > 0) {
    status = ucx_tagged_recv(ucx_h, left, NULL, 0, UCX_TAG_BARRIER);
    if (UCS_OK != status) {
      return status;
    }
  }

  status = ucx_tagged_send(ucx_h, right, NULL, 0, UCX_TAG_BARRIER);
  if (UCS_OK != status) {
    return status;
  }

  if (rank == 0) {
    status = ucx_tagged_recv(ucx_h, left, NULL, 0, UCX_TAG_BARRIER);
    if (UCS_OK != status) {
      return status;
    }
  }

  return status;
}

ucs_status_t ucx_barrier(const ucx_handle_t *ucx_h) {
  const uint32_t rank = ucx_h->self_rank;
  const uint32_t size = ucx_h->comm_size;

  ucs_status_t status;

  // printf("%d: Flushing RDMA worker\n", ucx_h->self_rank);
  // fflush(stdout);
  status = ucx_flush_worker(ucx_h, &ucx_h->rma_h.c_w_ep_h.ucp_worker);
  if (status != UCS_OK) {
    return status;
  }

  // status = ucx_flush_worker(ucx_h, &ucx_h->ptp_h.ucp_worker);
  // if (status != UCS_OK) {
  //   return status;
  // }

  // printf("%d: Flushed RDMA worker\n", ucx_h->self_rank);
  // fflush(stdout);

  // first barrier ring to synchronize all processes
  status = barrier_ring(ucx_h, rank, size);
  if (UCS_OK != status) {
    return status;
  }

  // after all processes synchronized, start second barrier ring
  status = barrier_ring(ucx_h, rank, size);

  // for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
  //   if (i == ucx_h->self_rank) {
  //     msg = i;
  //   }

  //   status = ucx_broadcast(ucx_h, i, &msg, sizeof(msg), i);
  //   if (status != UCS_OK) {
  //     return status;
  //   }

  //   if (msg != i) {
  //     return UCS_ERR_INVALID_PARAM;
  //   }
  // }

  // Maybe there are some outstanding operations after all processes
  // synchronized ...
  // int processed = 0;

  // do {
  //   processed = ucp_worker_progress(ucx_h->ptp_h.ucp_worker);
  // } while (processed != 0);

  return UCS_OK;
}