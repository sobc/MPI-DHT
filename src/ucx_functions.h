#ifndef UCX_FUNCTIONS_H_
#define UCX_FUNCTIONS_H_

#include <ucp/api/ucp.h>

ucs_status_t ucx_initContext(ucp_context_h *context);

ucs_status_t ucx_initWorker(ucp_context_h context, ucp_worker_h *worker,
                        ucp_address_t **local_address,
                        uint64_t *local_addr_len);

ucs_status_t ucx_exchangeWorkerMemory(ucp_worker_h worker,
                                  ucp_address_t *local_addr,
                                  uint64_t local_addr_len, ucp_ep_h **ep_list,
                                  void *func_args);

ucs_status_t ucx_createMemory(ucp_context_h context, uint64_t size,
                          ucp_mem_h *mem_h, void *local_mem);

ucs_status_t ucx_exchangeRKeys(const ucp_context_h context, const ucp_mem_h mem_h,
                           const ucp_ep_h **ep_list, const uint64_t local_addr,
                           uint64_t *rem_addr, void **rkey_buffer,
                           ucp_rkey_h *rkey_handles, void *func_arg);

#endif // UCX_FUNCTIONS_H_
