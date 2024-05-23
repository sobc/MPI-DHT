#include <LUCX/DHT.h>
#include <math.h>
#include <stdint.h>

#include "macros.h"
#include "ucx/ucx_lib.h"

#define LOCK_SIZE sizeof(uint32_t)

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

  ucs_status_t status;

  const uint64_t bucket_size =
      sizeof(uint32_t) + init_params->data_size + init_params->key_size + 1;

  const uint64_t size_of_dht = init_params->bucket_count * bucket_size;

  // allocate memory for dht-object
  object = (DHT *)malloc(sizeof(DHT));
  if (unlikely(object == NULL)) {
    return NULL;
  }

  int bcast_func_ret;

  object->ucx_h = ucx_init(init_params->bcast_func,
                           init_params->bcast_func_args, &bcast_func_ret);

  if (unlikely((bcast_func_ret != UCX_BCAST_OK) || (object->ucx_h == NULL))) {
    goto err_after_object;
  }

  status = ucx_init_rma(object->ucx_h, size_of_dht);
  if (unlikely(status != UCS_OK)) {
    goto err_after_ucx_init;
  }

  // fill dht-object
  object->offset = bucket_size;
  object->data_size = init_params->data_size;
  object->key_size = init_params->key_size;
  object->bucket_count = init_params->bucket_count;
  object->hash_func = init_params->hash_func;
  object->read_misses = 0;
  object->evictions = 0;
  object->chksum_retries = 0;
  object->recv_entry =
      malloc(1 + object->data_size + object->key_size + sizeof(uint32_t));
  if (unlikely(object->recv_entry == NULL)) {
    goto err_after_mem_init;
  }
  object->send_entry =
      malloc(1 + object->data_size + object->key_size + sizeof(uint32_t));
  if (unlikely(object->send_entry == NULL)) {
    free(object->recv_entry);
    goto err_after_mem_init;
  }

  uint8_t index_bytes;
  get_index_bytes(init_params->bucket_count, &object->index_shift,
                  &index_bytes);

  object->index_count = 8 - (index_bytes - 1);
  object->rank_shift =
      sizeof(uint32_t) * 8 - (unsigned int)ceil(log2(object->ucx_h->comm_size));

  // if set, initialize dht_stats
#ifdef DHT_STATISTICS
  object->stats.writes_local =
      (int64_t *)calloc(object->ucx_h->comm_size, sizeof(int64_t));
  if (unlikely(object->stats.writes_local == NULL)) {
    free(object->recv_entry);
    free(object->send_entry);
    free(object->index);
    goto err_after_ucx_init;
  }

  object->stats.index_usage =
      (uint64_t *)calloc(object->index_count, sizeof(uint64_t));

  if (unlikely(object->stats.index_usage == NULL)) {
    free(object->recv_entry);
    free(object->send_entry);
    free(object->index);
    free(object->stats.writes_local);
    goto err_after_ucx_init;
  }

  object->stats.old_writes = 0;
  object->stats.read_misses = 0;
  object->stats.evictions = 0;
  object->stats.w_access = 0;
  object->stats.r_access = 0;
#endif

#ifdef DHT_DISTRIBUTION
  object->access_distribution =
      (uint64_t *)calloc(object->ucx_h->comm_size, sizeof(uint64_t));

  if (unlikely(object->access_distribution == NULL)) {
    free(object->recv_entry);
    free(object->send_entry);
    free(object->index);
    goto err_after_ucx_init;
  }
#endif

  return object;

err_after_mem_init:
  ucx_free_mem(object->ucx_h);
err_after_ucx_init:
  ucx_finalize(object->ucx_h);
  free(object->ucx_h);
err_after_object:
  free(object);
  return NULL;
}