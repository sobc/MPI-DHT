#ifndef UCX_LIB_H_
#define UCX_LIB_H_

#include "DHT_ucx/DHT.h"
#include "DHT_ucx/UCX_init.h"
#include "dht_macros.h"

#include <stdint.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

#define UCX_REQ_FEAT (UCP_FEATURE_RMA | UCP_FEATURE_AMO32 | UCP_FEATURE_TAG)

#define BUCKET_LOCK ((uint64_t)0x0000000000000001UL)
#define BUCKET_UNLOCK ((uint64_t)0x0000000000000000UL)

#define CHECK_NO_WAIT 0
#define CHECK_WAIT 1

ucs_status_t ucx_init_endpoints(ucx_handle_t *ucx_h,
                                ucx_worker_addr_bcast func_bcast,
                                void *func_args);

ucs_status_t ucx_init_remote_memory(ucx_handle_t *ucx_h, uint64_t mem_size);
/* ucs_status_t ucx_initContext(ucp_context_h *context); */

/* ucs_status_t ucx_initWorker(ucp_context_h context, ucp_worker_h *worker, */
/*                             ucp_address_t **local_address, */
/*                             uint64_t *local_addr_len); */

/* ucs_status_t ucx_createEndpoints(ucp_worker_h worker, ucp_address_t
 * *local_addr, */
/*                                  uint64_t local_addr_len, ucp_ep_h **ep_list,
 */
/*                                  void *func_args); */

// ucs_status_t ucx_createMemory(ucp_context_h context, uint64_t size,
//                               ucp_mem_h *mem_h, uint64_t *local_mem);

// ucs_status_t ucx_exchangeRKeys(ucx_handle_t *ucx_h);

void ucx_releaseRKeys(ucp_rkey_h *rkey_handles, void **rkey_buffer,
                      uint64_t *rem_addresses, int rkey_count);

ucs_status_t ucx_releaseEndpoints(ucp_ep_h *endpoint_handles,
                                  int endpoint_count, ucp_worker_h worker);

ucs_status_t ucx_releaseLocalMemory(ucp_context_h context,
                                    ucp_mem_h memory_handle);

void ucx_cleanup(ucp_context_h context, ucp_worker_h worker_handle);

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
                                           ucs_status_ptr_t *request, int imm);

ucs_status_t ucx_flush_ep(const ucx_handle_t *ucx_h, int rank);

ucs_status_t ucx_write_release_lock(const ucx_handle_t *ucx_h);

ucs_status_t ucx_broadcast(const ucx_handle_t *ucx_h, uint64_t root, void *msg,
                           uint64_t msg_size, uint32_t msg_tag);

ucs_status_t ucx_barrier(const ucx_handle_t *ucx_h);

#endif // UCX_LIB_H_
