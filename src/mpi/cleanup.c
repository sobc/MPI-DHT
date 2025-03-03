#include <LUCX/DHT.h>
#include <mpi.h>
#include <stdlib.h>

#include "macros.h"

int DHT_free(DHT *table, uint32_t *chksum_retries) {
  int64_t buf;

  int status;

#ifndef LUCX_MPI_OLD
  status = MPI_Win_flush_all(table->window);
  if (status != MPI_SUCCESS) {
    return status;
  }
#endif

  status = MPI_Barrier(table->comm);
  if (status != MPI_SUCCESS) {
    return status;
  }

  if (chksum_retries != NULL) {
    buf = table->chksum_retries;
    if (MPI_SUCCESS != MPI_Reduce(&buf, chksum_retries, 1, MPI_UINT32_T,
                                  MPI_SUM, 0, table->comm)) {
      return DHT_UCX_ERROR;
    }
    *chksum_retries = buf;
  }

#ifndef LUCX_MPI_OLD
  if (unlikely(MPI_SUCCESS != MPI_Win_unlock_all(table->window))) {
    return DHT_UCX_ERROR;
  }
#endif

  status = MPI_Win_free(&(table->window));
  if (unlikely(status != MPI_SUCCESS)) {
    return status;
  }

  status = MPI_Free_mem(table->mem_alloc);
  if (unlikely(status != MPI_SUCCESS)) {
    return status;
  }

  free(table->recv_entry);
  free(table->send_entry);

#ifdef DHT_STATISTICS
  free(table->stats.writes_local);
#endif

  free(table);

  return DHT_SUCCESS;
}