#ifndef UCX_LIB_H_
#define UCX_LIB_H_

#include "../macros.h"
#include "DHT_ucx/DHT.h"

#include <stdint.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

#define UCX_REQ_FEAT (UCP_FEATURE_RMA | UCP_FEATURE_AMO32 | UCP_FEATURE_TAG)

#define BUCKET_LOCK ((uint64_t)0x0000000000000001UL)
#define BUCKET_UNLOCK ((uint64_t)0x0000000000000000UL)

#define CHECK_NO_WAIT 0
#define CHECK_WAIT 1

ucx_handle_t *ucx_init(ucx_worker_addr_bcast func_bcast, const void *func_args,
                       int *func_ret);

ucs_status_t ucx_init_remote_memory(ucx_handle_t *ucx_h, uint64_t mem_size);

ucs_status_t ucx_free_mem(ucx_handle_t *ucx_h);

void ucx_releaseEndpoints(ucx_handle_t *ucx_h);

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

ucs_status_t ucx_check_and_wait_completion(const ucx_handle_t *ucx_h,
                                           ucs_status_ptr_t *request, int imm);

ucs_status_t ucx_flush_ep(const ucx_handle_t *ucx_h, int rank);

ucs_status_t ucx_flush_worker(const ucx_handle_t *ucx_h);

ucs_status_t ucx_write_release_lock(const ucx_handle_t *ucx_h);

ucs_status_t ucx_broadcast(const ucx_handle_t *ucx_h, uint64_t root, void *msg,
                           uint64_t msg_size, uint32_t msg_tag);

ucs_status_t ucx_reduce_sum(const ucx_handle_t *ucx_h, int64_t *buf,
                            uint32_t root);

ucs_status_t ucx_barrier(const ucx_handle_t *ucx_h);

#endif // UCX_LIB_H_
