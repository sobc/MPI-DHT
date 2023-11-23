#include "DHT_ucx/UCX_init.h"
#include "dht_macros.h"
#include "ucx_lib.h"

#include <stdint.h>
#include <stdlib.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_compat.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>
#include <ucs/type/thread_mode.h>

const ucp_params_t ucp_params = {.field_mask = UCP_PARAM_FIELD_FEATURES,
                                 .features = UCX_REQ_FEAT};

const ucp_worker_params_t worker_params = {
    .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
    .thread_mode = UCS_THREAD_MODE_SINGLE};

static inline ucs_status_t ucx_initContext(ucp_context_h *context) {
  ucs_status_t status;

  ucp_config_t *config;

  status = ucp_config_read(NULL, NULL, &config);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  status = ucp_init(&ucp_params, config, context);

  ucp_config_release(config);

  return status;
}

static inline ucs_status_t ucx_initWorker(ucp_context_h context,
                                          ucp_worker_h *worker,
                                          ucp_address_t **local_address,
                                          uint64_t *local_addr_len) {
  ucs_status_t status;

  ucp_worker_attr_t worker_attr = {.field_mask = UCP_WORKER_ATTR_FIELD_ADDRESS};

  status = ucp_worker_create(context, &worker_params, worker);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  status = ucp_worker_query(*worker, &worker_attr);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  *local_addr_len = worker_attr.address_length;
  *local_address = worker_attr.address;

  return status;
}

static inline ucs_status_t ucx_createEndpoints(ucp_worker_h worker,
                                               ucx_ep_info_t *ep_info,
                                               ucp_ep_h **ep_list) {
  ucs_status_t status;
  ucp_ep_params_t ep_params = {.field_mask =
                                   UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
                                   UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE,
                               .err_mode = UCP_ERR_HANDLING_MODE_NONE};

  *ep_list = (ucp_ep_h *)malloc(sizeof(ucp_ep_h) * ep_info->comm_size);
  if (unlikely(*ep_list == NULL)) {
    return UCS_ERR_NO_MEMORY;
  }

  for (uint32_t i = 0; i < ep_info->comm_size; i++) {
    ep_params.address = ep_info->worker_addr[i];
    status = ucp_ep_create(worker, &ep_params, &(*ep_list)[i]);
    if (unlikely(status != UCS_OK)) {
      return status;
    }
  }

  return UCS_OK;
}

ucs_status_t ucx_init_endpoints(ucx_handle_t *ucx_h,
                                ucx_worker_addr_bcast func_bcast,
                                void *func_args) {
  ucs_status_t status;
  ucp_address_t *local_address;
  uint64_t local_address_len;
  ucx_ep_info_t ep_info;
  int bcast_status;

  status = ucx_initContext(&ucx_h->ucp_context);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  status = ucx_initWorker(ucx_h->ucp_context, &ucx_h->ucp_worker,
                          &local_address, &local_address_len);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  bcast_status =
      (*func_bcast)(local_address, local_address_len, func_args, &ep_info);
  if (unlikely(bcast_status != UCX_BCAST_OK)) {
    return UCS_ERR_NO_DEVICE;
  }

  status = ucx_createEndpoints(ucx_h->ucp_worker, &ep_info, &ucx_h->ep_list);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  ucx_h->comm_size = ep_info.comm_size;
  ucx_h->self_rank = ep_info.self_rank;

  ucp_worker_release_address(ucx_h->ucp_worker, local_address);
  ucx_free_ep_info(&ep_info);

  return UCS_OK;
}
