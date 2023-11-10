#ifndef UCX_FUNCTIONS_H_
#define UCX_FUNCTIONS_H_

#include <stdint.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

#include "DHT_ucx/DHT.h"
#include "dht_macros.h"

#define UCX_REQ_FEAT (UCP_FEATURE_RMA | UCP_FEATURE_AMO32 | UCP_FEATURE_TAG)

struct ucx_request {
  int completed;
};

ucs_status_t ucx_initContext(ucp_context_h *context);

ucs_status_t ucx_initWorker(ucp_context_h context, ucp_worker_h *worker,
                            ucp_address_t **local_address,
                            uint64_t *local_addr_len);

ucs_status_t ucx_createEndpoints(ucp_worker_h worker, ucp_address_t *local_addr,
                                 uint64_t local_addr_len, ucp_ep_h **ep_list,
                                 void *func_args);

ucs_status_t ucx_createMemory(ucp_context_h context, uint64_t size,
                              ucp_mem_h *mem_h, uint64_t *local_mem);

ucs_status_t ucx_exchangeRKeys(ucx_handle_t *ucx_h);

void ucx_releaseRKeys(ucp_rkey_h *rkey_handles, void **rkey_buffer,
                      uint64_t *rem_addresses, int rkey_count);

ucs_status_t ucx_releaseEndpoints(ucp_ep_h *endpoint_handles,
                                  int endpoint_count, ucp_worker_h worker);

ucs_status_t ucx_releaseLocalMemory(ucp_context_h context,
                                    ucp_mem_h memory_handle);

void ucx_cleanup(ucp_context_h context, ucp_worker_h worker_handle);

#endif // UCX_FUNCTIONS_H_
