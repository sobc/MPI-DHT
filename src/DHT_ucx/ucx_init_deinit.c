#include "DHT_ucx/DHT.h"
#include "dht_macros.h"
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

ucs_status_t ucx_initContext(ucp_context_h *context) {
  ucs_status_t status;

  ucp_config_t *config;
  ucp_params_t ucp_params = {.field_mask = UCP_PARAM_FIELD_FEATURES,
                             .features = UCX_REQ_FEAT};

  status = ucp_config_read(NULL, NULL, &config);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "ucp_config_read", status);

  status = ucp_init(&ucp_params, config, context);

  ucp_config_release(config);

  return status;
}

ucs_status_t ucx_initWorker(ucp_context_h context, ucp_worker_h *worker,
                            ucp_address_t **local_address,
                            uint64_t *local_addr_len) {
  ucs_status_t status;

  ucp_worker_params_t worker_params;
  ucp_worker_attr_t worker_attr;

  worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;
  status = ucp_worker_create(context, &worker_params, worker);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "ucp_worker_create", status);

  worker_attr.field_mask = UCP_WORKER_ATTR_FIELD_ADDRESS;
  status = ucp_worker_query(*worker, &worker_attr);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "ucp_worker_query", status);

  *local_addr_len = worker_attr.address_length;
  *local_address = worker_attr.address;

  return status;
}

ucs_status_t ucx_createEndpoints(ucp_worker_h worker, ucp_address_t *local_addr,
                                 uint64_t local_addr_len, ucp_ep_h **ep_list,
                                 void *func_args) {
  ucs_status_t status;

  MPI_exchange *params = (MPI_exchange *)func_args;
  ucp_address_t *current_ep_addr = NULL;
  uint64_t current_ep_addr_len;

  ucp_ep_params_t ep_params;
  ep_params.field_mask =
      UCP_EP_PARAM_FIELD_REMOTE_ADDRESS | UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
  ep_params.err_mode = UCP_ERR_HANDLING_MODE_NONE;

  *ep_list = (ucp_ep_h *)malloc(sizeof(ucp_ep_h) * params->size);
  CHK_UNLIKELY_RETURN(*ep_list == NULL, "allocating ep list",
                      UCS_ERR_NO_MEMORY);

  for (int i = 0; i < params->size; i++) {
    if (i == params->rank) {
      current_ep_addr = local_addr;
      current_ep_addr_len = local_addr_len;
    }

    MPI_Bcast(&current_ep_addr_len, 1, MPI_UINT64_T, i, params->comm);

    if (i != params->rank) {
      current_ep_addr = (ucp_address_t *)malloc(current_ep_addr_len);
      CHK_UNLIKELY_RETURN(current_ep_addr == NULL, "allocating ep address",
                          UCS_ERR_NO_MEMORY);
    }

    MPI_Bcast(current_ep_addr, current_ep_addr_len, MPI_BYTE, i, params->comm);

    ep_params.address = current_ep_addr;
    status = ucp_ep_create(worker, &ep_params, &(*ep_list)[i]);

    if (i != params->rank) {
      free(current_ep_addr);
    }
  }

  ucp_worker_release_address(worker, local_addr);

  return UCS_OK;
}

ucs_status_t ucx_createMemory(ucp_context_h context, uint64_t size,
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

ucs_status_t ucx_exchangeRKeys(ucx_handle_t *ucx_h) {
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

void ucx_releaseRKeys(ucp_rkey_h *rkey_handles, void **rkey_buffer,
                      uint64_t *rem_addresses, int rkey_count) {
  for (int i = 0; i < rkey_count; i++) {
    ucp_rkey_destroy(rkey_handles[i]);
    ucp_rkey_buffer_release(rkey_buffer[i]);
  }

  free(rkey_handles);
  free(rkey_buffer);
  free(rem_addresses);
}

ucs_status_t ucx_releaseEndpoints(ucp_ep_h *endpoint_handles,
                                  int endpoint_count, ucp_worker_h worker) {

  ucs_status_t status;
  ucs_status_ptr_t request;
  ucp_request_param_t req_param = {.op_attr_mask = 0};

  for (int i = 0; i < endpoint_count; i++) {
    request = ucp_ep_close_nbx(endpoint_handles[i], &req_param);

    if (UCS_PTR_IS_PTR(request)) {
      do {
        ucp_worker_progress(worker);
        status = ucp_request_check_status(request);
      } while (status == UCS_INPROGRESS);
      ucp_request_free(request);

      if (!(status == UCS_OK || status == UCS_ERR_CONNECTION_RESET)) {
        return status;
      }
    }
  }

  free(endpoint_handles);

  return UCS_OK;
}

ucs_status_t ucx_releaseLocalMemory(ucp_context_h context,
                                    ucp_mem_h memory_handle) {
  return ucp_mem_unmap(context, memory_handle);
}

void ucx_cleanup(ucp_context_h context, ucp_worker_h worker_handle) {
  int processed = 0;

  ucp_worker_destroy(worker_handle);
  ucp_cleanup(context);
}
