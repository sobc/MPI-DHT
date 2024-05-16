#include "../macros.h"
#include "DHT_ucx/UCX_bcast_functions.h"
#include "ucx_lib.h"

#include <stdint.h>
#include <stdlib.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_compat.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>
#include <ucs/type/thread_mode.h>

const ucp_worker_params_t worker_params = {
    .field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
    .thread_mode = UCS_THREAD_MODE_SINGLE};

static ucs_status_t ucx_initContext(ucp_context_h *context, uint64_t features,
                                    uint64_t estimated_num_eps) {
  ucs_status_t status;

  ucp_config_t *config;

  status = ucp_config_read(NULL, NULL, &config);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  const ucp_params_t ucp_params = {.field_mask =
                                       UCP_PARAM_FIELD_FEATURES |
                                       UCP_PARAM_FIELD_ESTIMATED_NUM_EPS |
                                       UCP_PARAM_FIELD_MT_WORKERS_SHARED,
                                   .mt_workers_shared = 0,
                                   .estimated_num_eps = estimated_num_eps,
                                   .features = features};

  status = ucp_init(&ucp_params, config, context);

  ucp_config_release(config);

  return status;
}

static ucs_status_t ucx_initWorker(ucp_context_h context, ucp_worker_h *worker,
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

static ucs_status_t ucx_createEndpoints(ucp_worker_h worker,
                                        const ucx_ep_info_t *ep_info,
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
      for (uint32_t j = i - 1; j >= 0; j--) {
        ucp_ep_destroy((*ep_list)[j]);
      }
      free(*ep_list);
      return status;
    }
  }

  return UCS_OK;
}

static inline void ucx_free_ep_info(ucx_ep_info_t *ep_info) {
  for (uint32_t i = 0; i < ep_info->comm_size; i++) {
    free(ep_info->worker_addr[i]);
  }
  free(ep_info->worker_addr);
}

static ucs_status_t ucx_init_ctx_worker_ep(ucx_worker_addr_bootstrap func_bcast,
                                           const void *func_args, int *func_ret,
                                           uint64_t ctx_features,
                                           uint64_t estimated_num_eps,
                                           struct ucx_c_w_ep_handle *c_w_ep_h,
                                           uint32_t *rank, uint32_t *comm_size,
                                           int with_request) {
  ucs_status_t status = UCS_OK;

  ucp_address_t *local_address;
  uint64_t local_address_len;

  ucx_ep_info_t ep_info;

  *func_ret = UCX_BCAST_NOT_RUN;

  status =
      ucx_initContext(&c_w_ep_h->ucp_context, ctx_features, estimated_num_eps);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  status = ucx_initWorker(c_w_ep_h->ucp_context, &c_w_ep_h->ucp_worker,
                          &local_address, &local_address_len);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  *func_ret =
      (*func_bcast)(local_address, local_address_len, func_args, &ep_info);
  if (unlikely(*func_ret != UCX_BCAST_OK)) {
    return UCS_ERR_NOT_CONNECTED;
  }

  status =
      ucx_createEndpoints(c_w_ep_h->ucp_worker, &ep_info, &c_w_ep_h->ep_list);
  if (unlikely(status != UCS_OK)) {
    ucx_free_ep_info(&ep_info);
    return status;
  }

  ucp_worker_release_address(c_w_ep_h->ucp_worker, local_address);

  if (rank != NULL) {
    *rank = ep_info.self_rank;
  }

  if (comm_size != NULL) {
    *comm_size = ep_info.comm_size;
  }

  ucx_free_ep_info(&ep_info);

  return status;
}

ucx_handle_t *ucx_init(ucx_worker_addr_bootstrap func_bcast,
                       const void *func_args, int *func_ret) {
  ucs_status_t status;

  ucx_handle_t *ucx_h;

  ucx_h = malloc(sizeof(ucx_handle_t));
  if (unlikely(ucx_h == NULL)) {
    goto err_no_handle;
  }

  // status = ucx_initContext(&ucx_h->ucp_context);
  // if (unlikely(status != UCS_OK)) {
  //   goto err_subfuncs;
  // }

  // status = ucx_initWorker(ucx_h->ucp_context, &ucx_h->ucp_worker,
  //                         &local_address, &local_address_len);
  // if (unlikely(status != UCS_OK)) {
  //   goto err_subfuncs;
  // }

  // *func_ret =
  //     (*func_bcast)(local_address, local_address_len, func_args, &ep_info);
  // if (unlikely(*func_ret != UCX_BCAST_OK)) {
  //   goto err_subfuncs;
  // }

  status = ucx_init_ctx_worker_ep(func_bcast, func_args, func_ret,
                                  UCX_PTP_REQ_FEAT, 0, &ucx_h->ptp_h,
                                  &ucx_h->self_rank, &ucx_h->comm_size, 1);
  if (unlikely(UCS_OK != status)) {
    goto err_subfuncs;
  }

  status = ucx_init_ctx_worker_ep(func_bcast, func_args, func_ret,
                                  UCX_RMA_REQ_FEAT, ucx_h->comm_size,
                                  &ucx_h->rma_h.c_w_ep_h, NULL, NULL, 0);

  if (unlikely(UCS_OK != status)) {
    goto err_subfuncs;
  }

  // status = ucx_createEndpoints(ucx_h->ucp_worker, &ep_info, &ucx_h->ep_list);
  // if (unlikely(status != UCS_OK)) {
  //   goto err_subfuncs;
  // }

  return ucx_h;

err_subfuncs:
  free(ucx_h);
err_no_handle:
  return NULL;
}
