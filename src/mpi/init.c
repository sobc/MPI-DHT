#include <LUCX/DHT.h>
#include <math.h>
#include <mpi.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "macros.h"

#define CHKSUM_SIZE sizeof(uint32_t)

/**
 * Calculates the number of bytes needed to store an index for a given number of
 * buckets.
 *
 * @param nbuckets The number of buckets.
 * @return The number of bytes needed to store the index.
 */
static inline void get_index_bytes(uint64_t nbuckets, uint8_t *index_shift,
                                   uint8_t *index_bytes) {
  uint8_t index_bits = 0;
  uint64_t temp_size = nbuckets;

  // Calculate the logarithm base 2
  while (temp_size > 0) {
    temp_size >>= 1;
    index_bits++;
  }

  *index_bytes = (uint8_t)(index_bits / 8) + 1;

  // FIXME: do not use global variable
  *index_shift = *index_bytes * 8 - index_bits;
}

DHT *DHT_create(const DHT_init_t *init_params) {
  DHT *object;

  int status;

  uint64_t bucket_size = init_params->data_size + init_params->key_size + 1;

  uint32_t padding = 0;

// calculate the size of a bucket
#if defined(LUCX_MPI_NO_LOCK)
  bucket_size += CHKSUM_SIZE;
#elif defined(LUCX_MPI_FINE_LOCK)
  bucket_size += LOCK_SIZE;
  if (!!(bucket_size & (LOCK_SIZE - 1))) {
    padding = LOCK_SIZE - (bucket_size & (LOCK_SIZE - 1));
    bucket_size += padding;
  }
#endif

  const uint64_t size_of_dht = init_params->bucket_count * bucket_size;

  // allocate memory for dht-object
  object = (DHT *)malloc(sizeof(DHT));
  if (unlikely(object == NULL)) {
    return NULL;
  }

  if (unlikely(MPI_SUCCESS !=
               MPI_Alloc_mem(size_of_dht, MPI_INFO_NULL, &object->mem_alloc))) {
    goto err_after_object;
  }

  if (unlikely(MPI_SUCCESS != MPI_Win_create(object->mem_alloc, size_of_dht, 1,
                                             MPI_INFO_NULL, init_params->comm,
                                             &object->window))) {
    goto err_after_mem_alloc;
  }

  memset(object->mem_alloc, '\0', size_of_dht);

  object->comm = init_params->comm;
  MPI_Comm_size(init_params->comm, &object->comm_size);

  // fill dht-object
  object->displacement = bucket_size;
  object->padding = padding;
  object->data_size = init_params->data_size;
  object->key_size = init_params->key_size;
  object->bucket_count = init_params->bucket_count;
  object->hash_func = init_params->hash_func;
  object->chksum_retries = 0;
  object->recv_entry = malloc(bucket_size - padding - LOCK_SIZE);
  if (unlikely(object->recv_entry == NULL)) {
    goto err_after_win_create;
  }
  object->send_entry = malloc(bucket_size - padding - LOCK_SIZE);
  if (unlikely(object->send_entry == NULL)) {
    free(object->recv_entry);
    goto err_after_win_create;
  }

  uint8_t index_bytes;
  get_index_bytes(init_params->bucket_count, &object->index_shift,
                  &index_bytes);

  object->index_count = 8 - (index_bytes - 1);
  object->rank_shift =
      sizeof(uint32_t) * 8 - (unsigned int)ceil(log2(object->comm_size));

  // if set, initialize dht_stats
#ifdef DHT_STATISTICS
  object->stats.writes_local =
      (int64_t *)calloc(object->ucx_h->comm_size, sizeof(int64_t));
  if (unlikely(object->stats.writes_local == NULL)) {
    free(object->recv_entry);
    free(object->send_entry);
    goto err_after_ucx_init;
  }

  object->stats.index_usage =
      (uint64_t *)calloc(object->index_count, sizeof(uint64_t));

  if (unlikely(object->stats.index_usage == NULL)) {
    free(object->recv_entry);
    free(object->send_entry);
    free(object->stats.writes_local);
    goto err_after_ucx_init;
  }

  object->stats.old_writes = 0;
  object->stats.read_misses = 0;
  object->stats.evictions = 0;
  object->stats.w_access = 0;
  object->stats.r_access = 0;
#endif

#ifndef LUCX_MPI_OLD
  MPI_Win_lock_all(0, object->window);
#endif

  MPI_Barrier(object->comm);

  return object;

err_after_win_create:
  MPI_Win_free(&object->window);
err_after_mem_alloc:
  MPI_Free_mem(object->mem_alloc);
err_after_object:
  free(object);
  return NULL;
}
