#include <LUCX/DHT.h>

#include "macros.h"
#include "ucx/ucx_lib.h"

#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#define REDUCE_HASH(hash) ((uint32_t)(hash) ^ (uint32_t)((hash) >> 32))
#define BUCKET_OFFSET(offset, i, disp) (offset * i) + disp

/**
 * Determines the destination rank and indices for a given key.
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
  const uint32_t chksum = REDUCE_HASH(
      table->hash_func(table->key_size + table->data_size, key_val_begin));

  // store the checksum at the end of the entry
  memcpy((char *)table->send_entry + table->data_size + table->key_size + 1,
         &chksum, sizeof(uint32_t));

#ifdef DHT_STATISTICS
  table->stats.w_access++;
  table->stats.writes_local[dest_rank]++;
#endif

  // loop through the index array, checking for an available bucket
  for (curr_index = 0; curr_index < table->index_count; curr_index++) {
    // get the contents of the destination bucket
    status = ucx_get_blocking(
        table->ucx_h, dest_rank, table->offset * indices[curr_index],
        table->recv_entry,
        table->data_size + table->key_size + 1 + sizeof(uint32_t));

    if (status != UCS_OK) {
      return DHT_UCX_ERROR;
    }

    // check if the destination bucket is available
    if (!(*(buffer_begin) & (BUCKET_OCCUPIED | BUCKET_INVALID))) {
      break;
    }

    // check if the keys match
    if (memcmp((buffer_begin + 1), send_key, table->key_size) == 0) {
      break;
    }
  }

#ifdef DHT_STATISTICS
  table->stats.evictions += (uint8_t)(curr_index == table->index_count);
#endif

  if (curr_index == table->index_count) {
    result = DHT_WRITE_SUCCESS_WITH_EVICTION;
    curr_index--;
  }

  // write the entry to the destination bucket
  status = ucx_put_blocking(
      table->ucx_h, dest_rank, table->offset * indices[curr_index],
      table->send_entry,
      table->data_size + table->key_size + sizeof(uint32_t) + 1);

  if (status != UCS_OK) {
    return DHT_UCX_ERROR;
  }

#ifdef DHT_STATISTICS
  // if this is the last index, increment the eviction counter
  table->stats.index_usage[curr_index]++;
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

  ucs_status_t status;

  for (curr_index = 0; curr_index < table->index_count; curr_index++) {
    status = ucx_get_blocking(
        table->ucx_h, dest_rank, table->offset * indices[curr_index],
        table->recv_entry,
        table->data_size + table->key_size + 1 + sizeof(uint32_t));

    if (status != UCS_OK) {
      return DHT_UCX_ERROR;
    }

    // return if bucket is not occupied
    if (!(*(buffer_begin) & (BUCKET_OCCUPIED))) {
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
      const uint32_t *bucket_checksum =
          (uint32_t *)(buffer_begin + table->data_size + table->key_size + 1);

      // check if the checksum is correct
      if (*bucket_checksum ==
          REDUCE_HASH(table->hash_func(table->data_size + table->key_size,
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
      status = ucx_put_blocking(table->ucx_h, dest_rank,
                                table->offset * indices[curr_index],
                                &invalidate, sizeof(char));
      if (status != UCS_OK) {
        return DHT_UCX_ERROR;
      }

      return DHT_READ_CORRUPT;
    }
  }

  // check if all indices were checked
  if (curr_index == table->index_count) {
#ifdef DHT_STATISTICS
    table->stats.read_misses += 1;
#endif
    return DHT_READ_MISS;
  }

  // if matching key was found copy data into memory of passed pointer
  memcpy((char *)destination, (char *)table->recv_entry + table->key_size + 1,
         table->data_size);

  return DHT_SUCCESS;
}