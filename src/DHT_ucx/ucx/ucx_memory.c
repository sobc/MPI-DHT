#include "../dht_macros.h"
#include "ucx_lib.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_compat.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

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
  CHK_UNLIKELY_RETURN(status != UCS_OK, "Allocation and registration of memory",
                      status);

  mem_attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS;
  status = ucp_mem_query(*mem_h, &mem_attr);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "Query memory handle", status);

  memset(mem_attr.address, '\0', size);
  *local_mem = (uint64_t)mem_attr.address;

  return UCS_OK;
}

static ucs_status_t ucx_exchangeRKeys(ucx_handle_t *ucx_h) {
  ucs_status_t status;

  uint64_t curr_rkey_size;
  size_t local_rkey_size;

  void **local_rkey_buffer;
  uint64_t *local_rem_addr;
  ucp_rkey_h *local_rkey_handles;

  local_rem_addr = (uint64_t *)malloc(sizeof(uint64_t) * ucx_h->comm_size);
  CHK_UNLIKELY_RETURN(local_rem_addr == NULL,
                      "allocating remote addresses array", UCS_ERR_NO_MEMORY);

  local_rkey_buffer = (void **)malloc(sizeof(void *) * ucx_h->comm_size);
  CHK_UNLIKELY_RETURN(local_rkey_buffer == NULL, "allocating rkey buffer",
                      UCS_ERR_NO_MEMORY);

  local_rkey_handles =
      (ucp_rkey_h *)malloc(sizeof(ucp_rkey_h) * ucx_h->comm_size);
  CHK_UNLIKELY_RETURN(local_rkey_handles == NULL,
                      "allocating rkey handle array", UCS_ERR_NO_MEMORY);

  status =
      ucp_rkey_pack(ucx_h->ucp_context, ucx_h->mem_h,
                    &local_rkey_buffer[ucx_h->self_rank], &local_rkey_size);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "packing rkey", status);

  for (uint32_t i = 0; i < ucx_h->comm_size; i++) {
    if (i == ucx_h->self_rank) {
      curr_rkey_size = local_rkey_size;
      local_rem_addr[i] = ucx_h->local_mem_addr;
    }

    status = ucx_broadcast(ucx_h, i, &curr_rkey_size, sizeof(uint64_t), 0);
    if (status != UCS_OK) {
      return status;
    }

    status = ucx_broadcast(ucx_h, i, &local_rem_addr[i], sizeof(uint64_t), 1);
    if (status != UCS_OK) {
      return status;
    }

    if (i != ucx_h->self_rank) {
      local_rkey_buffer[i] = malloc(curr_rkey_size);
      CHK_UNLIKELY_RETURN(local_rkey_buffer[i] == NULL,
                          "Allocating rkey buffer element", UCS_ERR_NO_MEMORY);
    }

    status = ucx_broadcast(ucx_h, i, local_rkey_buffer[i], curr_rkey_size, 2);
    if (status != UCS_OK) {
      return status;
    }

    status = ucp_ep_rkey_unpack(ucx_h->ep_list[i], local_rkey_buffer[i],
                                &local_rkey_handles[i]);
    CHK_UNLIKELY_RETURN(status != UCS_OK, "unpacking rkey", status);
  }

  ucx_h->remote_addr = local_rem_addr;
  ucx_h->rkey_buffer = local_rkey_buffer;
  ucx_h->rkey_handles = local_rkey_handles;

  return UCS_OK;
}

static ucs_status_t ucx_initPostRecv(const ucx_handle_t *ucx_h) {
  uint32_t data;
  ucs_status_t status;

  for (int i = 0; i < ucx_h->comm_size; i++) {
    status = ucx_get(ucx_h, i, 0, &data, sizeof(data));
    if (UCS_OK != status) {
      return status;
    }
  }

  return UCS_OK;
}

ucs_status_t ucx_init_remote_memory(ucx_handle_t *ucx_h, uint64_t bucket_size,
                                    uint64_t count) {
  ucs_status_t status;

#ifdef DHT_WITH_LOCKING
  uint8_t padding = bucket_size % sizeof(uint32_t);
  if (!!padding) {
    padding = sizeof(uint32_t) - padding;
  }
#else
  uint8_t padding = 0;
#endif

  bucket_size += padding;

  uint64_t mem_size = count * bucket_size;

  ucx_h->offset = bucket_size;
  ucx_h->flag_padding = padding;
  ucx_h->lock_size = sizeof(uint32_t);

  status = ucx_createMemory(ucx_h->ucp_context, mem_size, &ucx_h->mem_h,
                            &ucx_h->local_mem_addr);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  status = ucx_exchangeRKeys(ucx_h);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  status = ucx_barrier(ucx_h);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  status = ucx_initPostRecv(ucx_h);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  return UCS_OK;
}
