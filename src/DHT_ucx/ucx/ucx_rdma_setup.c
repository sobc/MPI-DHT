#include "../macros.h"
#include "DHT_ucx/DHT.h"
#include "ucx_lib.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_compat.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

#define SOME_RANDOM_OFFSET 123

static ucs_status_t ucx_createMemory(ucp_context_h context, uint64_t size,
                                     ucp_mem_h *mem_h, uint64_t *local_mem) {
  ucs_status_t status;

  ucp_mem_map_params_t mem_params;
  ucp_mem_attr_t mem_attr;

  mem_params.field_mask =
      UCP_MEM_MAP_PARAM_FIELD_LENGTH | UCP_MEM_MAP_PARAM_FIELD_FLAGS;
  mem_params.length = size;
  mem_params.flags = UCP_MEM_MAP_ALLOCATE;
  status = ucp_mem_map(context, &mem_params, mem_h);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  mem_attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS;
  status = ucp_mem_query(*mem_h, &mem_attr);
  if (unlikely(status != UCS_OK)) {
    ucp_mem_unmap(context, *mem_h);
    return status;
  }

  *local_mem = (uint64_t)mem_attr.address;

  return UCS_OK;
}

#define FREE_ALREADY_ALLOCATED_ENTRIES(last, self, arr)                        \
  do {                                                                         \
    for (uint32_t j = last; j >= 0; j--) {                                     \
      if (j != self) {                                                         \
        free(arr[j]);                                                          \
      }                                                                        \
    }                                                                          \
  } while (0)

static ucs_status_t ucx_exchangeRKeys(ucx_handle_t *ucx_h) {
  ucs_status_t status;

  uint64_t curr_rkey_size;
  size_t local_rkey_size;

  void **local_rkey_buffer;
  uint64_t *local_rem_addr;
  ucp_rkey_h *local_rkey_handles;

  local_rem_addr = (uint64_t *)malloc(sizeof(uint64_t) * ucx_h->comm_size);
  if (unlikely(local_rem_addr == NULL)) {
    status = UCS_ERR_NO_MEMORY;
    goto err_local_rem_addr;
  }

  local_rkey_buffer = (void **)malloc(sizeof(void *) * ucx_h->comm_size);
  if (unlikely(local_rkey_buffer == NULL)) {
    status = UCS_ERR_NO_MEMORY;
    goto err_local_rkey_buffer;
  }

  local_rkey_handles =
      (ucp_rkey_h *)malloc(sizeof(ucp_rkey_h) * ucx_h->comm_size);
  if (unlikely(local_rkey_handles == NULL)) {
    status = UCS_ERR_NO_MEMORY;
    goto err_local_rkey_handles;
  }

  status =
      ucp_rkey_pack(ucx_h->rma_h.c_w_ep_h.ucp_context, ucx_h->rma_h.mem_h,
                    &local_rkey_buffer[ucx_h->self_rank], &local_rkey_size);
  if (unlikely(status != UCS_OK)) {
    goto err_after_allocation;
  }

  for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
    if (i == ucx_h->self_rank) {
      curr_rkey_size = local_rkey_size;
      local_rem_addr[i] = ucx_h->rma_h.local_mem_addr;
    }

    status = ucx_broadcast(ucx_h, i, &curr_rkey_size, sizeof(uint64_t), 0);
    if (status != UCS_OK) {
      goto err_after_allocation;
    }

    status = ucx_broadcast(ucx_h, i, &local_rem_addr[i], sizeof(uint64_t), 1);
    if (status != UCS_OK) {
      goto err_after_allocation;
    }

    if (i != ucx_h->self_rank) {
      local_rkey_buffer[i] = malloc(curr_rkey_size);
      if (unlikely(local_rkey_buffer[i] == NULL)) {
        FREE_ALREADY_ALLOCATED_ENTRIES(i - 1, ucx_h->self_rank,
                                       local_rkey_buffer);
        status = UCS_ERR_NO_MEMORY;
        goto err_after_allocation;
      }
    }

    status = ucx_broadcast(ucx_h, i, local_rkey_buffer[i], curr_rkey_size, 2);
    if (unlikely(status != UCS_OK)) {
      FREE_ALREADY_ALLOCATED_ENTRIES(i - 1, ucx_h->self_rank,
                                     local_rkey_buffer);
      goto err_after_allocation;
    }

    status = ucp_ep_rkey_unpack(ucx_h->rma_h.c_w_ep_h.ep_list[i],
                                local_rkey_buffer[i], &local_rkey_handles[i]);
    if (unlikely(status != UCS_OK)) {
      FREE_ALREADY_ALLOCATED_ENTRIES(i - 1, ucx_h->self_rank,
                                     local_rkey_buffer);
      goto err_after_allocation;
    }
  }

  ucx_h->rma_h.remote_addr = local_rem_addr;
  ucx_h->rma_h.rkey_buffer = local_rkey_buffer;
  ucx_h->rma_h.rkey_handles = local_rkey_handles;

  return UCS_OK;

err_after_allocation:
  free(local_rkey_handles);
err_local_rkey_handles:
  free(local_rkey_buffer);
err_local_rkey_buffer:
  free(local_rem_addr);
err_local_rem_addr:
  return status;
}

static ucs_status_t ucx_initPostRecv(const ucx_handle_t *ucx_h) {
  uint32_t data;
  ucs_status_t status;

  uint32_t *my_mem = (uint32_t *)(ucx_h->rma_h.local_mem_addr);

  for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
    my_mem[i] = i + SOME_RANDOM_OFFSET;
  }

  for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
    uint32_t validate = 0;
    // printf("%d: %d\n", ucx_h->self_rank, i);
    do {
      status = ucx_get_blocking(ucx_h, i, ucx_h->self_rank * sizeof(uint32_t),
                                &validate, sizeof(uint32_t));
      if (UCS_OK != status) {
        return status;
      }
    } while (validate != my_mem[ucx_h->self_rank]);
  }

  // printf("%d: Left first post-recv loop\n", ucx_h->self_rank);
  // fflush(stdout);

  my_mem += ucx_h->comm_size;

  const uint32_t val_to_write = ucx_h->self_rank + SOME_RANDOM_OFFSET * 2;

  // for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
  //   my_mem[i] = i + SOME_RANDOM_OFFSET * 2;
  // }

  for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
    // printf("%d: %d\n", ucx_h->self_rank, i);
    status = ucx_put_blocking(ucx_h, i,
                              ucx_h->self_rank * sizeof(uint32_t) +
                                  (sizeof(uint32_t) * (ucx_h->comm_size)),
                              &val_to_write, sizeof(uint32_t));
    if (UCS_OK != status) {
      return status;
    }
  }

  // loop over comm_size
  for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
    while (my_mem[i] != i + SOME_RANDOM_OFFSET * 2) {
      ucp_worker_progress(ucx_h->rma_h.c_w_ep_h.ucp_worker);
    }
  }

  // for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
  //   uint32_t *validate = (uint32_t *)(ucx_h->rma_h.local_mem_addr) + i;
  //   do {
  //     ucp_worker_progress(ucx_h->rma_h.c_w_ep_h.ucp_worker);
  //   } while (*validate != i);
  // }

  return UCS_OK;
}

ucs_status_t ucx_init_rma(ucx_handle_t *ucx_h, uint64_t mem_size) {
  ucs_status_t status = UCS_OK;

  status = ucx_createMemory(ucx_h->rma_h.c_w_ep_h.ucp_context, mem_size,
                            &ucx_h->rma_h.mem_h, &ucx_h->rma_h.local_mem_addr);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  status = ucx_exchangeRKeys(ucx_h);
  if (unlikely(status != UCS_OK)) {
    ucp_mem_unmap(ucx_h->rma_h.c_w_ep_h.ucp_context, ucx_h->rma_h.mem_h);
    return status;
  }

  // printf("After exchange of rkeys\n");
  // fflush(stdout);

  status = ucx_barrier(ucx_h);
  if (unlikely(status != UCS_OK)) {
    goto err_release_memory;
  }

  // printf("After first barrier\n");
  // fflush(stdout);

  status = ucx_initPostRecv(ucx_h);
  if (unlikely(status != UCS_OK)) {
    goto err_release_memory;
  }

  memset((void *)ucx_h->rma_h.local_mem_addr, '\0', mem_size);

  // printf("After post-recv\n");
  // fflush(stdout);

  // status = ucx_barrier(ucx_h);
  // if (unlikely(status != UCS_OK)) {
  //   goto err_release_memory;
  // }

  return status;

err_release_memory:
  ucx_free_mem(ucx_h);
  return status;
}
