#include <LUCX/DHT.h>

#include "macros.h"

#include <mpi.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>

#define REDUCE_HASH(hash) ((uint32_t)(hash) ^ (uint32_t)((hash) >> 32))
#define BUCKET_OFFSET(offset, i, disp) (offset * i) + disp

#define EXCLUSIVE_LOCK 0x0000000010000000ULL

#ifdef LUCX_MPI_FINE_LOCK
const LOCK_DTYPE UNLOCKED = 0;
const LOCK_DTYPE LOCK_EXCLUSIVE = EXCLUSIVE_LOCK;
const LOCK_DTYPE LOCK_EXCLUSIVE_DEC = -EXCLUSIVE_LOCK;
const LOCK_DTYPE LOCK_INC = 1;
const LOCK_DTYPE LOCK_DEC = -1;
#endif

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
    const uint32_t tmp_index =
        (hash >> (i * index_size)) & ((1 << (index_size)) - 1);
    // store the index in the index array
    index[i] = (tmp_index >> index_shift) % table_size;
  }

  // calculate the destination rank by shifting the hash value
  *dest_rank = (unsigned int)((hash >> rank_shift) % comm_size);
}

static inline int osc_bucket_lock(int dest_rank, int index, DHT *table,
                                  int lock_type) {
#ifdef LUCX_MPI_FINE_LOCK
  MPI_Aint disp = index * table->displacement;
  LOCK_DTYPE lock_val;
  switch (lock_type) {
  case MPI_LOCK_EXCLUSIVE: {

    do {
      CHECK_MPI(MPI_Compare_and_swap(&LOCK_EXCLUSIVE, &UNLOCKED, &lock_val,
                                     MPI_INT64_T, dest_rank, disp,
                                     table->window));
      CHECK_MPI(MPI_Win_flush(dest_rank, table->window))
    } while (lock_val != UNLOCKED);
    break;
  }
  case MPI_LOCK_SHARED: {
    while (1) {
      CHECK_MPI(MPI_Fetch_and_op(&LOCK_INC, &lock_val, MPI_INT64_T, dest_rank,
                                 disp, MPI_SUM, table->window));
      CHECK_MPI(MPI_Win_flush(dest_rank, table->window));

      if (lock_val < (int64_t)EXCLUSIVE_LOCK) {
        break;
      }

      CHECK_MPI(MPI_Fetch_and_op(&LOCK_DEC, &lock_val, MPI_INT64_T, dest_rank,
                                 disp, MPI_SUM, table->window));
      CHECK_MPI(MPI_Win_flush(dest_rank, table->window));
    }

    break;
  }
  }
#endif
  return MPI_SUCCESS;
}

static inline int osc_bucket_unlock(int dest_rank, int index, DHT *table,
                                    int lock_type) {
#ifdef LUCX_MPI_FINE_LOCK
  MPI_Aint disp = index * table->displacement;
  LOCK_DTYPE lock_val;

  switch (lock_type) {
  case MPI_LOCK_EXCLUSIVE: {
    CHECK_MPI(MPI_Fetch_and_op(&LOCK_EXCLUSIVE_DEC, &lock_val, MPI_INT64_T,
                               dest_rank, disp, MPI_SUM, table->window));
    break;
  }
  case MPI_LOCK_SHARED: {
    CHECK_MPI(MPI_Fetch_and_op(&LOCK_DEC, &lock_val, MPI_INT64_T, dest_rank,
                               disp, MPI_SUM, table->window));
    break;
  }
  }

  CHECK_MPI(MPI_Win_flush(dest_rank, table->window));

#endif
  return MPI_SUCCESS;
}

static inline int osc_win_lock(int lock_type, int dest_rank, MPI_Win window) {
#ifdef LUCX_MPI_OLD
  return MPI_Win_lock(lock_type, dest_rank, 0, window);
#else
  return MPI_SUCCESS;
#endif
}

static inline int osc_win_unlock(int dest_rank, MPI_Win window) {
#ifdef LUCX_MPI_OLD
  return MPI_Win_unlock(dest_rank, window);
#else
  return MPI_SUCCESS;
#endif
}

static inline int osc_get(DHT *table, int dest_rank, uint64_t index) {
  MPI_Aint disp = index * table->displacement + LOCK_SIZE + table->padding;
  uint32_t tcount = table->displacement - LOCK_SIZE - table->padding;
#ifndef LUCX_MPI_ACC
  CHECK_MPI(MPI_Get(table->recv_entry, tcount, MPI_BYTE, dest_rank, disp,
                    tcount, MPI_BYTE, table->window));
#else
  CHECK_MPI(MPI_Get_accumulate(
      table->recv_entry, tcount, MPI_BYTE, table->recv_entry, tcount, MPI_BYTE,
      dest_rank, disp, tcount, MPI_BYTE, MPI_NO_OP, table->window));
#endif
  return MPI_Win_flush(dest_rank, table->window);
}

static inline int osc_put(DHT *table, int dest_rank, uint64_t index) {
  MPI_Aint disp = index * table->displacement + LOCK_SIZE + table->padding;
  uint32_t tcount = table->displacement - LOCK_SIZE - table->padding;
#ifndef LUCX_MPI_ACC
  CHECK_MPI(MPI_Put(table->send_entry, tcount, MPI_BYTE, dest_rank, disp,
                    tcount, MPI_BYTE, table->window));
#else
  CHECK_MPI(MPI_Accumulate(table->send_entry, tcount, MPI_BYTE, dest_rank, disp,
                           tcount, MPI_BYTE, MPI_REPLACE, table->window));
#endif
  return MPI_Win_flush(dest_rank, table->window);
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

  int status;

  // determine destination rank and index by hash of key
  determine_dest(hash, table->comm_size, table->bucket_count,
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

#if defined(LUCX_MPI_NO_LOCK)
  // calculate the checksum of the key and data
  const void *key_val_begin = (char *)table->send_entry + 1;
  const uint32_t chksum = REDUCE_HASH(
      table->hash_func(table->key_size + table->data_size, key_val_begin));

  // store the checksum at the end of the entry
  memcpy((char *)table->send_entry + table->data_size + table->key_size + 1,
         &chksum, sizeof(uint32_t));
#endif

#ifdef DHT_STATISTICS
  table->stats.w_access++;
  table->stats.writes_local[dest_rank]++;
#endif

  CHECK_MPI(osc_win_lock(MPI_LOCK_EXCLUSIVE, dest_rank, table->window));

  // loop through the index array, checking for an available bucket
  for (curr_index = 0; curr_index < table->index_count; curr_index++) {
    // get the contents of the destination bucket

    CHECK_MPI(osc_bucket_lock(dest_rank, indices[curr_index], table,
                              MPI_LOCK_EXCLUSIVE));
    CHECK_MPI(osc_get(table, dest_rank, indices[curr_index]));

    // check if the destination bucket is available
    if (!(*(buffer_begin) & (BUCKET_OCCUPIED)) ||
        (*(buffer_begin) & (BUCKET_INVALID))) {
      break;
    }

    // check if the keys match
    if (memcmp((buffer_begin + 1), send_key, table->key_size) == 0) {
      break;
    }

    // unlock the bucket if it is not the last index and continue with the next
    // index
    if (curr_index != table->index_count - 1) {
      CHECK_MPI(osc_bucket_unlock(dest_rank, indices[curr_index], table,
                                  MPI_LOCK_EXCLUSIVE));
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
  CHECK_MPI(osc_put(table, dest_rank, indices[curr_index]));

  // unlock the destination bucket
  CHECK_MPI(osc_bucket_unlock(dest_rank, indices[curr_index], table,
                              MPI_LOCK_EXCLUSIVE));

  // unlock the window
  CHECK_MPI(osc_win_unlock(dest_rank, table->window));

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
  determine_dest(hash, table->comm_size, table->bucket_count,
                 table->index_count, table->index_shift, table->rank_shift,
                 &dest_rank, indices);

  int status;

  CHECK_MPI(osc_win_lock(MPI_LOCK_SHARED, dest_rank, table->window));

  for (curr_index = 0; curr_index < table->index_count; curr_index++) {
    CHECK_MPI(osc_bucket_lock(dest_rank, indices[curr_index], table,
                              MPI_LOCK_SHARED));
    CHECK_MPI(osc_get(table, dest_rank, indices[curr_index]));
    CHECK_MPI(osc_bucket_unlock(dest_rank, indices[curr_index], table,
                                MPI_LOCK_SHARED));

    // return if bucket is not occupied
    if (!(*(buffer_begin) & (BUCKET_OCCUPIED))) {
#ifdef DHT_STATISTICS
      table->stats.read_misses += 1;
#endif
      // unlock window and return
      CHECK_MPI(osc_win_unlock(dest_rank, table->window));
      return DHT_READ_MISS;
    }

    // matching keys found
    if (memcmp((buffer_begin + 1), send_key, table->key_size) == 0) {
      // check if the bucket is invalid
#if defined(LUCX_MPI_NO_LOCK)
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

      CHECK_MPI(osc_put(table, dest_rank, indices[curr_index]));

      return DHT_READ_CORRUPT;
#endif
      break;
    }
  }

  CHECK_MPI(osc_win_unlock(dest_rank, table->window));

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
