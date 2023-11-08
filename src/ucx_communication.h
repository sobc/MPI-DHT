#ifndef UCX_COMMUNICATION_H_
#define UCX_COMMUNICATION_H_

#include "DHT/DHT.h"
#include <stdbool.h>
#include <stdint.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

#define BUCKET_LOCK ((uint64_t)0x0000000000000001UL)
#define BUCKET_UNLOCK ((uint64_t)0x0000000000000000UL)

typedef struct ucx_request_handle {
  ucp_worker_h worker;
  ucp_ep_h endpoint;
  uint64_t rem_base_addr;
  uint32_t offset;
  ucp_rkey_h rkey_target;
  uint64_t lock_rem_addr;
  uint32_t lock_size;
} ucx_request_context_t;

ucs_status_t ucx_initPostRecv(const ucx_handle_t *ucx_h, int size);

ucs_status_t ucx_write_acquire_lock(ucx_handle_t *ctx_h, uint64_t index,
                                    int rank);

ucs_status_ptr_t *ucx_put_data(const ucx_handle_t *ucx_h, int rank,
                               uint64_t index, uint64_t count,
                               const void *buffer);

ucs_status_ptr_t *ucx_get_data(const ucx_handle_t *ucx_h, int rank,
                               uint64_t index, uint64_t count, void *buffer);

ucs_status_t ucx_put(const ucx_handle_t *ucx_h, int rank, uint64_t index,
                     const void *buffer, uint64_t count);

ucs_status_t ucx_get(const ucx_handle_t *ucx_h, int rank, uint64_t index,
                     void *buffer, uint64_t count);

ucs_status_t ucx_check_and_wait_completion(const ucx_handle_t *ucx_h,
                                           ucs_status_ptr_t *request);

ucs_status_t ucx_flush_ep(const ucx_handle_t *ucx_h, int rank);

ucs_status_t ucx_write_release_lock(const ucx_handle_t *ucx_h);

#endif // UCX_COMMUNICATION_H_
