#include <LUCX/DHT.h>

#include "ucx/ucx_lib.h"

int DHT_free(DHT *table, uint32_t *chksum_retries) {
  int64_t buf;

  ucs_status_t status;

  status = ucx_barrier(table->ucx_h);
  if (status != UCS_OK) {
    return status;
  }

  if (chksum_retries != NULL) {
    buf = table->chksum_retries;
    if (UCS_OK != ucx_reduce_sum(table->ucx_h, &buf, 0)) {
      return DHT_UCX_ERROR;
    }
    *chksum_retries = buf;
  }
  status = ucx_free_mem(table->ucx_h);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  ucx_finalize(table->ucx_h);

  free(table->recv_entry);
  free(table->send_entry);

#ifdef DHT_STATISTICS
  free(table->stats.writes_local);
#endif

  free(table);

  return DHT_SUCCESS;
}