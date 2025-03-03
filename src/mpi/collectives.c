#include <LUCX/DHT.h>

int DHT_print_statistics(DHT *table) {
#ifdef DHT_STATISTICS
  int *written_buckets;
  int *read_misses = NULL;
  int64_t sum_read_misses;
  int *evictions = NULL;
  int64_t sum_evictions;
  int *w_access = NULL;
  int *r_access = NULL;
  int64_t sum_w_access;
  int64_t sum_r_access;
  int rank = table->ucx_h->self_rank;
  uint32_t comm_size = table->ucx_h->comm_size;
  // MPI_Comm_rank(table->communicator, &rank);

  ucs_status_t status;

// disable possible warning of unitialized variable, which is not the case
// since we're using MPI_Gather to obtain all values only on rank 0
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

  // obtaining all values from all processes in the communicator
  if (rank == 0) {
    read_misses = (int *)malloc(comm_size * sizeof(int));
    if (unlikely(read_misses == NULL)) {
      return DHT_UCX_ERROR;
    }
  }

  status = ucx_gather(table->ucx_h, &table->stats.read_misses, sizeof(int),
                      read_misses, 0);
  if (unlikely(status != UCS_OK)) {
    return DHT_UCX_ERROR;
  }

  if (rank == 0) {
    sum_read_misses = 0;
    for (uint32_t i = 0; i < comm_size; i++) {
      sum_read_misses += read_misses[i];
    }
  }

  table->stats.read_misses = 0;

  if (rank == 0) {
    evictions = (int *)malloc(comm_size * sizeof(int));
    if (unlikely(evictions == NULL)) {
      return DHT_UCX_ERROR;
    }
  }

  status = ucx_gather(table->ucx_h, &table->stats.evictions, sizeof(int),
                      evictions, 0);
  if (unlikely(status != UCS_OK)) {
    return DHT_UCX_ERROR;
  }

  if (rank == 0) {
    sum_evictions = 0;
    for (uint32_t i = 0; i < comm_size; i++) {
      sum_evictions += evictions[i];
    }
  }

  table->stats.evictions = 0;

  if (rank == 0) {
    w_access = (int *)malloc(comm_size * sizeof(int));
    if (unlikely(w_access == NULL)) {
      return DHT_UCX_ERROR;
    }
  }

  status = ucx_gather(table->ucx_h, &table->stats.w_access, sizeof(int),
                      w_access, 0);
  if (unlikely(status != UCS_OK)) {
    return DHT_UCX_ERROR;
  }

  if (rank == 0) {
    sum_w_access = 0;
    for (uint32_t i = 0; i < comm_size; i++) {
      sum_w_access += w_access[i];
    }
  }

  table->stats.w_access = 0;

  if (rank == 0) {
    r_access = (int *)malloc(comm_size * sizeof(int));
    if (unlikely(r_access == NULL)) {
      return DHT_UCX_ERROR;
    }
  }

  status = ucx_gather(table->ucx_h, &table->stats.r_access, sizeof(int),
                      r_access, 0);
  if (unlikely(status != UCS_OK)) {
    return DHT_UCX_ERROR;
  }

  if (rank == 0) {
    sum_r_access = 0;
    for (uint32_t i = 0; i < comm_size; i++) {
      sum_r_access += r_access[i];
    }
  }

  table->stats.r_access = 0;

  if (rank == 0) {
    written_buckets = (int *)calloc(comm_size, sizeof(int64_t));
    if (unlikely(written_buckets == NULL)) {
      return DHT_UCX_ERROR;
    }
  }

  for (uint32_t i = 0; i < comm_size; i++) {
    int64_t buf = table->stats.writes_local[i];
    status = ucx_reduce_sum(table->ucx_h, &buf, 0);
    if (rank == 0) {
      written_buckets[i] = buf;
    }
  }

  if (rank == 0) { // only process with rank 0 will print out results as a
                   // table
    int sum_written_buckets = 0;

    for (uint32_t i = 0; i < comm_size; i++) {
      sum_written_buckets += written_buckets[i];
    }

    int members = 7;
    int padsize = (members * 12) + 1;
    char pad[padsize + 1];

    memset(pad, '-', padsize * sizeof(char));
    pad[padsize] = '\0';
    printf("\n");
    printf("%-35s||resets with every call of this function\n", " ");
    printf("%-11s|%-11s|%-11s||%-11s|%-11s|%-11s|%-11s\n", "rank", "occupied",
           "free", "w_access", "r_access", "read misses", "evictions");
    printf("%s\n", pad);
    for (uint32_t i = 0; i < comm_size; i++) {
      printf("%-11d|%-11d|%-11d||%-11d|%-11d|%-11d|%-11d\n", i,
             written_buckets[i], table->bucket_count - written_buckets[i],
             w_access[i], r_access[i], read_misses[i], evictions[i]);
    }
    printf("%s\n", pad);
    printf("%-11s|%-11d|%-11d||%-11ld|%-11ld|%-11ld|%-11ld\n", "sum",
           sum_written_buckets,
           (table->bucket_count * comm_size) - sum_written_buckets,
           sum_w_access, sum_r_access, sum_read_misses, sum_evictions);

    printf("%s\n", pad);
    printf("%s %d\n",
           "new entries:", sum_written_buckets - table->stats.old_writes);

    printf("\n");
    fflush(stdout);

    table->stats.old_writes = sum_written_buckets;
  }

// enable warning again
#pragma GCC diagnostic pop

  status = ucx_barrier(table->ucx_h);
  if (status != UCS_OK) {
    return DHT_UCX_ERROR;
  }

  if (rank == 0) {
    free(written_buckets);
    free(r_access);
    free(w_access);
    free(evictions);
    free(read_misses);
  }
  return DHT_SUCCESS;
#else
  return DHT_NOT_IMPLEMENTED;
#endif
}

#ifdef DHT_STATISTICS
#define free_already_allocated(dist, j)                                        \
  do {                                                                         \
    for (int i = 0; i < j; i++) {                                              \
      free(dist[i]);                                                           \
    }                                                                          \
    free(dist);                                                                \
  } while (0)

static int gather_distribution_master(uint64_t **distribution, DHT *table) {
  for (uint32_t i = 0; i < table->ucx_h->comm_size; i++) {
    distribution[i] =
        (uint64_t *)calloc(table->ucx_h->comm_size, sizeof(uint64_t));
    if (unlikely(distribution[i] == NULL)) {
      free_already_allocated(distribution, i - 1);
      return DHT_NO_MEM;
    }
    if (i == table->ucx_h->self_rank) {
      memcpy(distribution[i], table->stats.index_usage,
             table->ucx_h->comm_size * sizeof(uint64_t));
      continue;
    }

    ucs_status_t status =
        ucx_tagged_recv(table->ucx_h, i, distribution[i],
                        table->ucx_h->comm_size * sizeof(uint64_t), i);
    if (unlikely(status != UCS_OK)) {
      free_already_allocated(distribution, i);
      return DHT_UCX_ERROR;
    }
  }
  return DHT_SUCCESS;
}

static int gather_index_master(uint64_t **index_usage, DHT *table) {
  for (uint32_t i = 0; i < table->ucx_h->comm_size; i++) {
    index_usage[i] = (uint64_t *)calloc(table->index_count, sizeof(uint64_t));
    if (unlikely(index_usage[i] == NULL)) {
      free_already_allocated(index_usage, i - 1);
      return DHT_NO_MEM;
    }
    if (i == table->ucx_h->self_rank) {
      memcpy(index_usage[i], table->stats.index_usage,
             table->index_count * sizeof(uint64_t));
      continue;
    }

    ucs_status_t status =
        ucx_tagged_recv(table->ucx_h, i, index_usage[i],
                        table->index_count * sizeof(uint64_t), i);
    if (unlikely(status != UCS_OK)) {
      free_already_allocated(index_usage, i);
      return DHT_UCX_ERROR;
    }
  }
  return DHT_SUCCESS;
}

#endif

int DHT_gather_distribution(DHT *table, uint64_t ***distribution, int reset) {
#ifdef DHT_STATISTICS
  if (table->ucx_h->self_rank == 0) {
    *distribution =
        (uint64_t **)calloc(table->ucx_h->comm_size, sizeof(uint64_t *));
    if (unlikely(distribution == NULL)) {
      return DHT_NO_MEM;
    }

    ucs_status_t status = gather_distribution_master(*distribution, table);
    if (unlikely(status != UCS_OK)) {
      return DHT_UCX_ERROR;
    }
  } else {
    ucs_status_t status = ucx_tagged_send(
        table->ucx_h, 0, table->stats.index_usage,
        table->ucx_h->comm_size * sizeof(uint64_t), table->ucx_h->self_rank);
    if (unlikely(status != UCS_OK)) {
      return DHT_UCX_ERROR;
    }
  }

  if (reset) {
    memset(table->stats.index_usage, 0,
           table->ucx_h->comm_size * sizeof(uint64_t));
  }

  return DHT_SUCCESS;

#else
  return DHT_NOT_IMPLEMENTED;
#endif
}

int DHT_gather_index_usage(DHT *table, uint64_t ***index_usage, int reset) {
#ifdef DHT_STATISTICS
  if (table->ucx_h->self_rank == 0) {
    *index_usage =
        (uint64_t **)calloc(table->ucx_h->comm_size, sizeof(uint64_t *));
    if (unlikely(index_usage == NULL)) {
      return DHT_NO_MEM;
    }

    ucs_status_t status = gather_index_master(*index_usage, table);
    if (unlikely(status != UCS_OK)) {
      return DHT_UCX_ERROR;
    }
  } else {
    ucs_status_t status = ucx_tagged_send(
        table->ucx_h, 0, table->stats.index_usage,
        table->index_count * sizeof(uint64_t), table->ucx_h->self_rank);
    if (unlikely(status != UCS_OK)) {
      return DHT_UCX_ERROR;
    }
  }

  if (reset) {
    memset(table->stats.index_usage, 0, table->index_count * sizeof(uint64_t));
  }

  return DHT_SUCCESS;

#else
  return DHT_NOT_IMPLEMENTED;
#endif
}