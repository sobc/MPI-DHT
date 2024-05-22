#include <DHT_ucx/DHT.h>

#include "macros.h"
#include "ucx/ucx_lib.h"

#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#define CROP_HASH(hash) (uint32_t) hash
#define BUCKET_OFFSET(offset, i, disp) (offset * i) + disp

/**
 * Determines the destination rank and index for a given key.
 *
 * @param hash The hash value of the key.
 * @param comm_size The size of the communicator.
 * @param table_size The size of the DHT.
 * @param dest_rank A pointer to the destination rank.
 * @param index A pointer to the index array.
 * @param index_count The number of indices.
 */
static inline void determine_dest(uint64_t hash, int comm_size,
                                  unsigned int table_size,
                                  unsigned int index_count, uint8_t index_shift,
                                  uint8_t rank_shift, unsigned int *dest_rank,
                                  uint64_t *index) {
  /** how many bytes do we need for one index? */
  const uint8_t index_size = (sizeof(hash) - index_count + 1) * 8;
  for (uint32_t i = 0; i < index_count; i++) {
    // calculate the index by shifting the hash value and masking it
    const uint8_t tmp_index =
        (hash >> (i * index_size)) & ((1 << (index_size)) - 1);
    // store the index in the index array
    index[i] = (tmp_index >> index_shift) % table_size;
  }

  // calculate the destination rank by shifting the hash value
  *dest_rank = (unsigned int)((hash >> rank_shift) % comm_size);
}

int DHT_write(DHT *table, void *send_key, void *send_data, uint32_t *proc,
              uint32_t *index) {
  unsigned int curr_index;
  unsigned int dest_rank;
  uint64_t indices[table->index_count];

  int result = DHT_SUCCESS;

  // pointer to the beginning of the receive buffer
  const char *buffer_begin = (char *)table->recv_entry;
  // hash of the key to be written
  const uint64_t hash = table->hash_func(table->key_size, send_key);

  ucs_status_t status;

  // determine destination rank and index by hash of key
  determine_dest(hash, table->ucx_h->comm_size, table->bucket_count,
                 table->index_count, table->index_shift, table->rank_shift,
                 &dest_rank, indices);

  // concatenate key and data to write entry to DHT
  // set write flag
  *((char *)table->send_entry) = (0 | BUCKET_OCCUPIED);
  // copy key
  memcpy((char *)table->send_entry + 1, send_key, table->key_size);
  // copy data
  memcpy((char *)table->send_entry + table->key_size + 1, send_data,
         table->data_size);

  // calculate the checksum of the key and data
  const void *key_val_begin = (char *)table->send_entry + 1;
  const uint32_t chksum = CROP_HASH(
      table->hash_func(table->key_size + table->data_size, key_val_begin));

  // store the checksum at the end of the entry
  memcpy((char *)table->send_entry + table->data_size + table->key_size + 1,
         &chksum, sizeof(uint32_t));

#ifdef DHT_STATISTICS
  table->stats.w_access++;
#endif

  // loop through the index array, checking for an available bucket
  for (curr_index = 0; curr_index < table->index_count; curr_index++) {
    // get the contents of the destination bucket
    status = ucx_get_blocking(table->ucx_h, dest_rank,
                              BUCKET_OFFSET(table->offset, indices[curr_index],
                                            table->data_displacement),
                              table->recv_entry,
                              table->data_size + table->key_size + 1 +
                                  sizeof(uint32_t));

#ifdef DHT_DISTRIBUTION
    table->access_distribution[dest_rank]++;
#endif

    if (status != UCS_OK) {
      return DHT_UCX_ERROR;
    }

    // check if the destination bucket is available
    if (!(*(buffer_begin) & (BUCKET_OCCUPIED | BUCKET_INVALID))) {
#ifdef DHT_STATISTICS
      table->stats.writes_local[dest_rank]++;
#endif
      break;
    }

    // check if the keys match
    if (memcmp((buffer_begin + 1), send_key, table->key_size) == 0) {
      // if the keys don't match, continue to the next index
      break;
    }
  }

  if (curr_index == (table->index_count) - 1) {
    // if this is the last index, increment the eviction counter
    table->evictions += 1;
#ifdef DHT_STATISTICS
    table->stats.evictions += 1;
#endif
    result = DHT_WRITE_SUCCESS_WITH_EVICTION;
  }

#ifdef DHT_WITH_LOCKING
  // acquire a lock on the destination bucket
  const uint64_t offset_lock = table->offset * indices[curr_index];
  if (UCS_OK != ucx_write_acquire_lock(table->ucx_h, dest_rank, offset_lock)) {
    return DHT_UCX_ERROR;
  }
#endif

  // write the entry to the destination bucket
  status = ucx_put_blocking(table->ucx_h, dest_rank,
                            BUCKET_OFFSET(table->offset, indices[curr_index],
                                          table->data_displacement),
                            table->send_entry,
                            table->data_size + table->key_size +
                                sizeof(uint32_t) + 1);

  if (status != UCS_OK) {
    return DHT_UCX_ERROR;
  }

#ifdef DHT_WITH_LOCKING
  // release the lock on the destination bucket
  if (UCS_OK != ucx_write_release_lock(table->ucx_h)) {
    return DHT_UCX_ERROR;
  }
#endif

#ifdef DHT_STATISTICS
  table->stats.index_usage[curr_index]++;
#endif

#ifdef DHT_DISTRIBUTION
  table->access_distribution[dest_rank]++;
#endif

  // set the return values
  if (proc) {
    *proc = dest_rank;
  }
  if (index) {
    *index = indices[curr_index];
  }

  return result;
}

int DHT_read(DHT *table, const void *send_key, void *destination) {
  unsigned int curr_index;
  unsigned int dest_rank;
  uint64_t indices[table->index_count];

  const char *buffer_begin = (char *)table->recv_entry;
  const uint64_t hash = table->hash_func(table->key_size, send_key);

  uint8_t retry = 1;

#ifdef DHT_STATISTICS
  table->stats.r_access++;
#endif

  // determine destination rank and index by hash of key
  determine_dest(hash, table->ucx_h->comm_size, table->bucket_count,
                 table->index_count, table->index_shift, table->rank_shift,
                 &dest_rank, indices);

#ifdef DHT_DISTRIBUTION
  table->access_distribution[dest_rank]++;
#endif

  ucs_status_t status;

  for (curr_index = 0; curr_index < table->index_count; curr_index++) {
    status = ucx_get_blocking(table->ucx_h, dest_rank,
                              BUCKET_OFFSET(table->offset, indices[curr_index],
                                            table->data_displacement),
                              table->recv_entry,
                              table->data_size + table->key_size + 1 +
                                  sizeof(uint32_t));

    if (status != UCS_OK) {
      return DHT_UCX_ERROR;
    }

#ifdef DHT_DISTRIBUTION
    table->access_distribution[dest_rank]++;
#endif

    // return if bucket is not occupied
    if (!(*(buffer_begin) & (BUCKET_OCCUPIED))) {
      table->read_misses += 1;
#ifdef DHT_STATISTICS
      table->stats.read_misses += 1;
#endif
      // unlock window and return
      return DHT_READ_MISS;
    }

    // matching keys found
    if (memcmp((buffer_begin + 1), send_key, table->key_size) == 0) {
      // check if the bucket is invalid
      if (*(buffer_begin) & (BUCKET_INVALID)) {
        return DHT_READ_INVALID;
      }

      const void *key_val_begin = buffer_begin + 1;
      const uint32_t *bucket_check =
          (uint32_t *)(buffer_begin + table->data_size + table->key_size + 1);

      // check if the checksum is correct
      if (*bucket_check ==
          CROP_HASH(table->hash_func(table->data_size + table->key_size,
                                     key_val_begin))) {
        break;
      }

      // if the checksum is incorrect, retry once
      if (retry > 0) {
        retry--;
        curr_index--;
        table->chksum_retries++;
        continue;
      }

      // if the checksum is incorrect, invalidate the bucket
      const char invalidate = *(buffer_begin) | BUCKET_INVALID;
      status =
          ucx_put_blocking(table->ucx_h, dest_rank,
                           BUCKET_OFFSET(table->offset, indices[curr_index],
                                         table->data_displacement),
                           &invalidate, sizeof(char));
      if (status != UCS_OK) {
        return DHT_UCX_ERROR;
      }
#ifdef DHT_DISTRIBUTION
      table->access_distribution[dest_rank]++;
#endif

      return DHT_READ_CORRUPT;
    }

    // check if the last index was reached
    if (curr_index == (table->index_count) - 1) {
      table->read_misses += 1;
#ifdef DHT_STATISTICS
      table->stats.read_misses += 0;
#endif
      return DHT_READ_MISS;
    }
  }

  // if matching key was found copy data into memory of passed pointer
  memcpy((char *)destination, (char *)table->recv_entry + table->key_size + 1,
         table->data_size);

  return DHT_SUCCESS;
}