#include <stdlib.h>

#include "ucx_functions.h"
#include "DHT/DHT.h"
#include "dht_macros.h"

ucs_status_t ucx_initContext(ucp_context_h *context) {
  ucs_status_t status;

  ucp_config_t *config;
  ucp_params_t ucp_params;

  status = ucp_config_read(NULL, NULL, &config);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "ucp_config_read", status);

  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
  ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_RMA | UCP_FEATURE_AMO64;

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

ucs_status_t ucx_exchangeWorkerMemory(ucp_worker_h worker,
                                  ucp_address_t *local_addr,
                                  uint64_t local_addr_len, ucp_ep_h **ep_list,
                                  void *func_args) {
  ucs_status_t status;

  MPI_exchange *params = (MPI_exchange *)func_args;
  ucp_address_t *current_ep_addr = NULL;
  uint64_t current_ep_addr_len;
  uint64_t last_addr_len = 0;

  ucp_ep_params_t ep_params;
  ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;

  ep_list = (ucp_ep_h **)malloc(sizeof(ucp_ep_h *) * params->size);
  CHK_UNLIKELY_RETURN(ep_list == NULL, "allocating ep list", UCS_ERR_NO_MEMORY);

  for (int i = 0; i < params->size; i++) {
    if (i == params->rank) {
      current_ep_addr = local_addr;
      current_ep_addr_len = local_addr_len;
    }

    MPI_Bcast(&current_ep_addr_len, 1, MPI_UINT64_T, i, params->comm);

    if (i != params->rank && last_addr_len != current_ep_addr_len) {
      if (current_ep_addr != NULL) {
        free(current_ep_addr);
      }
      current_ep_addr = (ucp_address_t *)malloc(current_ep_addr_len);
      CHK_UNLIKELY_RETURN(current_ep_addr == NULL, "allocating ep address",
                          UCS_ERR_NO_MEMORY);
      last_addr_len = current_ep_addr_len;
    }

    MPI_Bcast(&current_ep_addr, current_ep_addr_len, MPI_BYTE, i, params->comm);

    ep_params.address = current_ep_addr;
    status = ucp_ep_create(worker, &ep_params, ep_list[i]);
  }

  if (current_ep_addr != NULL) {
    free(current_ep_addr);
  }

  return UCS_OK;
}

ucs_status_t ucx_createMemory(ucp_context_h context, uint64_t size,
                          ucp_mem_h *mem_h, void *local_mem) {
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

  local_mem = mem_attr.address;

  return UCS_OK;
}

ucs_status_t ucx_exchangeRKeys(const ucp_context_h context, const ucp_mem_h mem_h,
                           const ucp_ep_h **ep_list, const uint64_t local_addr,
                           uint64_t *rem_addr, void **rkey_buffer,
                           ucp_rkey_h *rkey_handles, void *func_arg) {
  ucs_status_t status;

  MPI_exchange *params = (MPI_exchange *)func_arg;
  void *curr_rkey;
  uint64_t curr_rkey_size;
  size_t local_rkey_size;

  rem_addr = (uint64_t *)malloc(sizeof(uint64_t) * params->size);
  CHK_UNLIKELY_RETURN(rem_addr == NULL, "allocating remote addresses array",
                      UCS_ERR_NO_MEMORY);

  rkey_buffer = (void **)malloc(sizeof(void *) * params->size);
  CHK_UNLIKELY_RETURN(rkey_buffer == NULL, "allocating rkey buffer",
                      UCS_ERR_NO_MEMORY);

  rkey_handles = (ucp_rkey_h *)malloc(sizeof(ucp_rkey_h) * params->size);
  CHK_UNLIKELY_RETURN(rkey_handles == NULL, "allocating rkey handle array",
                      UCS_ERR_NO_MEMORY);

  status = ucp_rkey_pack(context, mem_h, rkey_buffer[params->rank],
                         &local_rkey_size);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "packing rkey", status);

  for (int i = 0; i < params->size; i++) {
    curr_rkey = rkey_buffer[i];

    if (i == params->rank) {
      curr_rkey_size = local_rkey_size;
      rem_addr[i] = local_addr;
    }

    MPI_Bcast(&curr_rkey_size, 1, MPI_UINT64_T, i, params->comm);

    MPI_Bcast(&rem_addr[i], 1, MPI_UINT64_T, i, params->comm);

    if (i != params->rank) {
      rkey_buffer[i] = malloc(curr_rkey_size);
      CHK_UNLIKELY_RETURN(rkey_buffer[i] == NULL,
                          "Allocating rkye buffer element", UCS_ERR_NO_MEMORY);
    }

    MPI_Bcast(rkey_buffer[i], curr_rkey_size, MPI_BYTE, i, params->comm);

    status = ucp_ep_rkey_unpack(*ep_list[i], rkey_buffer[i], &rkey_handles[i]);
    CHK_UNLIKELY_RETURN(status != UCS_OK, "unpacking rkey", status);
  }

  return UCS_OK;
}
