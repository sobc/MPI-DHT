/// Time-stamp: "Last modified 2023-11-21 14:13:00 mluebke"
/*
** Copyright (C) 2017-2021 Max Luebke (University of Potsdam)
**
** POET is free software; you can redistribute it and/or modify it under the
** terms of the GNU General Public License as published by the Free Software
** Foundation; either version 2 of the License, or (at your option) any later
** version.
**
** POET is distributed in the hope that it will be useful, but WITHOUT ANY
** WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
** A PARTICULAR PURPOSE. See the GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License along with
** this program; if not, write to the Free Software Foundation, Inc., 51
** Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/

#include <DHT_ucx/DHT.h>
#include <mpi.h>

#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_compat.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>
#include <unistd.h>

#include "DHT_ucx/UCX_bcast_functions.h"
#include "macros.h"
#include "ucx/ucx_lib.h"

#define LOCK_SIZE sizeof(uint32_t)

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
                                  unsigned int *dest_rank, uint64_t *index,
                                  unsigned int index_count) {
  /** temporary index */
  uint64_t tmp_index;
  /** how many bytes do we need for one index? */
  int index_size = sizeof(double) - (index_count - 1);
  for (uint32_t i = 0; i < index_count; i++) {
    tmp_index = 0;
    memcpy(&tmp_index, (char *)&hash + i, index_size);
    tmp_index >>= 4;
    index[i] = (uint64_t)(tmp_index % table_size);
  }

  const unsigned int rank_shift = 32 - (int)ceil(log2(comm_size));

  *dest_rank = (unsigned int)((hash >> rank_shift) % comm_size);
}

/**
 * Calculates the number of bytes needed to store an index for a given number of
 * buckets.
 *
 * @param nbuckets The number of buckets.
 * @return The number of bytes needed to store the index.
 */
static inline uint8_t get_index_bytes(uint64_t nbuckets) {
  uint8_t index_bits = 0;
  uint64_t temp_size = nbuckets;

  // Calculate the logarithm base 2
  while (temp_size > 0) {
    temp_size >>= 1;
    index_bits++;
  }

  const uint8_t index_bytes = (uint8_t)(index_bits / 8) + 1;

  return index_bytes;
}

DHT *DHT_create(const DHT_init_t *init_params) {
  DHT *object;

  ucs_status_t status;

  uint64_t bucket_size =
      2 * sizeof(uint32_t) + init_params->data_size + init_params->key_size + 1;

#ifdef DHT_WITH_LOCKING
  uint8_t padding = bucket_size % sizeof(uint32_t);
  if (!!padding) {
    padding = sizeof(uint32_t) - padding;
  }
  bucket_size += padding;
#else
  uint8_t padding = 0;
#endif

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
  object->flag_padding = padding;
  object->lock_displ = 0;
  object->data_displacement = LOCK_SIZE + padding;
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
  object->index_count = 8 - (get_index_bytes(init_params->bucket_count) - 1);
  object->index = (uint64_t *)malloc((object->index_count) * sizeof(uint64_t));
  if (unlikely(object->index == NULL)) {
    free(object->recv_entry);
    free(object->send_entry);
    goto err_after_mem_init;
  }

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

int DHT_barrier(DHT *table) { return ucx_barrier(table->ucx_h); }

/* void DHT_set_accumulate_callback(DHT *table, */
/*                                  int (*callback_func)(int, void *, int, */
/*                                                       void *)) { */
/*   table->accumulate_callback = callback_func; */
/* } */

/* int DHT_write_accumulate(DHT *table, const void *send_key, int data_size, */
/*                          void *send_data, uint32_t *proc, uint32_t *index, */
/*                          int *callback_ret) { */
/*   unsigned int dest_rank, i; */
/*   int result = DHT_SUCCESS; */

/* #ifdef DHT_STATISTICS */
/*   table->stats->w_access++; */
/* #endif */

/*   // determine destination rank and index by hash of key */
/*   determine_dest(table->hash_func(table->key_size, send_key),
 * table->comm_size, */
/*                  table->table_size, &dest_rank, table->index, */
/*                  table->index_count); */

/*   // concatenating key with data to write entry to DHT */
/*   set_flag((char *)table->send_entry); */
/*   memcpy((char *)table->send_entry + 1, (char *)send_key, table->key_size);
 */
/*   /\* memcpy((char *)table->send_entry + table->key_size + 1, (char
 * *)send_data, */
/*    *\/ */
/*   /\*        table->data_size); *\/ */

/*   // locking window of target rank with exclusive lock */
/*   if (MPI_Win_lock(MPI_LOCK_EXCLUSIVE, dest_rank, 0, table->window) != 0) */
/*     return DHT_UCX_ERROR; */
/*   for (i = 0; i < table->index_count; i++) { */
/*     if (MPI_Get(table->recv_entry, 1 + table->data_size + table->key_size, */
/*                 MPI_BYTE, dest_rank, table->index[i], */
/*                 1 + table->data_size + table->key_size, MPI_BYTE, */
/*                 table->window) != 0) */
/*       return DHT_UCX_ERROR; */
/*     if (MPI_Win_flush(dest_rank, table->window) != 0) */
/*       return DHT_UCX_ERROR; */

/*     // increment eviction counter if receiving key doesn't match sending key
 */
/*     // entry has write flag and last index is reached. */
/*     if (read_flag(*(char *)table->recv_entry)) { */
/*       if (memcmp(send_key, (char *)table->recv_entry + 1, table->key_size) !=
 */
/*           0) { */
/*         if (i == (table->index_count) - 1) { */
/*           table->evictions += 1; */
/* #ifdef DHT_STATISTICS */
/*           table->stats->evictions += 1; */
/* #endif */
/*           result = DHT_WRITE_SUCCESS_WITH_EVICTION; */
/*           break; */
/*         } */
/*       } else */
/*         break; */
/*     } else { */
/* #ifdef DHT_STATISTICS */
/*       table->stats->writes_local[dest_rank]++; */
/* #endif */
/*       break; */
/*     } */
/*   } */

/*   if (result == DHT_WRITE_SUCCESS_WITH_EVICTION) { */
/*     memset((char *)table->send_entry + 1 + table->key_size, '\0', */
/*            table->data_size); */
/*   } else { */
/*     memcpy((char *)table->send_entry + 1 + table->key_size, */
/*            (char *)table->recv_entry + 1 + table->key_size,
 * table->data_size); */
/*   } */

/*   *callback_ret = table->accumulate_callback( */
/*       data_size, (char *)send_data, table->data_size, */
/*       (char *)table->send_entry + 1 + table->key_size); */

/*   // put data to DHT (with last selected index by value i) */
/*   if (*callback_ret == 0) { */
/*     if (MPI_Put(table->send_entry, 1 + table->data_size + table->key_size, */
/*                 MPI_BYTE, dest_rank, table->index[i], */
/*                 1 + table->data_size + table->key_size, MPI_BYTE, */
/*                 table->window) != 0) */
/*       return DHT_UCX_ERROR; */
/*   } */
/*   // unlock window of target rank */
/*   if (MPI_Win_unlock(dest_rank, table->window) != 0) */
/*     return DHT_UCX_ERROR; */

/*   if (proc) { */
/*     *proc = dest_rank; */
/*   } */

/*   if (index) { */
/*     *index = table->index[i]; */
/*   } */

/*   return result; */
/* } */

int DHT_write(DHT *table, void *send_key, void *send_data, uint32_t *proc,
              uint32_t *index) {
  unsigned int curr_index;
  unsigned int dest_rank;
  int result = DHT_SUCCESS;

  // pointer to the beginning of the receive buffer
  const char *buffer_begin = (char *)table->recv_entry;
  // hash of the key to be written
  const uint64_t hash = table->hash_func(table->key_size, send_key);

  ucs_status_t status;

  // determine destination rank and index by hash of key
  determine_dest(hash, table->ucx_h->comm_size, table->bucket_count, &dest_rank,
                 table->index, table->index_count);

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
    status = ucx_get_blocking(
        table->ucx_h, dest_rank,
        BUCKET_OFFSET(table->offset, table->index[curr_index],
                      table->data_displacement),
        table->recv_entry,
        table->data_size + table->key_size + 1 + sizeof(uint32_t));

#ifdef DHT_DISTRIBUTION
    table->access_distribution[dest_rank]++;
#endif

    if (status != UCS_OK) {
      return DHT_UCX_ERROR;
    }

    // check if the destination bucket is available
    if (!(*(buffer_begin) & (BUCKET_OCCUPIED | BUCKET_INVALID))) {
// if the bucket is available, break out of the loop
#ifdef DHT_STATISTICS
      table->stats.writes_local[dest_rank]++;
#endif
      break;
    }

    // check if the keys match
    if (memcmp((buffer_begin + 1), send_key, table->key_size) != 0) {
      // if the keys don't match, continue to the next index
      if (curr_index == (table->index_count) - 1) {
        // if this is the last index, increment the eviction counter
        table->evictions += 1;
#ifdef DHT_STATISTICS
        table->stats.evictions += 1;
#endif
        result = DHT_WRITE_SUCCESS_WITH_EVICTION;
        break;
      }
      continue;
    }

    // if the keys match, overwrite the contents of the destination bucket
    break;
  }

// acquire a lock on the destination bucket
#ifdef DHT_WITH_LOCKING
  const uint64_t offset_lock = table->offset * table->index[curr_index];
  if (UCS_OK != ucx_write_acquire_lock(table->ucx_h, dest_rank, offset_lock)) {
    return DHT_UCX_ERROR;
  }
#endif

  // write the entry to the destination bucket
  status = ucx_put_blocking(
      table->ucx_h, dest_rank,
      BUCKET_OFFSET(table->offset, table->index[curr_index],
                    table->data_displacement),
      table->send_entry,
      table->data_size + table->key_size + sizeof(uint32_t) + 1);

  if (status != UCS_OK) {
    return DHT_UCX_ERROR;
  }

#ifdef DHT_STATISTICS
  table->stats.index_usage[curr_index]++;
#endif

#ifdef DHT_DISTRIBUTION
  table->access_distribution[dest_rank]++;
#endif

// release the lock on the destination bucket
#ifdef DHT_WITH_LOCKING
  if (UCS_OK != ucx_write_release_lock(table->ucx_h)) {
    return DHT_UCX_ERROR;
  }
#endif

  // set the return values
  if (proc) {
    *proc = dest_rank;
  }
  if (index) {
    *index = table->index[curr_index];
  }

  return result;
}

int DHT_read(DHT *table, const void *send_key, void *destination) {
  unsigned int curr_index;
  unsigned int dest_rank;
  const char *buffer_begin = (char *)table->recv_entry;
  const uint64_t hash = table->hash_func(table->key_size, send_key);

  uint8_t retry = 1;

#ifdef DHT_STATISTICS
  table->stats.r_access++;
#endif

  // determine destination rank and index by hash of key
  determine_dest(hash, table->ucx_h->comm_size, table->bucket_count, &dest_rank,
                 table->index, table->index_count);

#ifdef DHT_DISTRIBUTION
  table->access_distribution[dest_rank]++;
#endif

  ucs_status_t status;

  for (curr_index = 0; curr_index < table->index_count; curr_index++) {
    status = ucx_get_blocking(
        table->ucx_h, dest_rank,
        BUCKET_OFFSET(table->offset, table->index[curr_index],
                      table->data_displacement),
        table->recv_entry,
        table->data_size + table->key_size + 1 + sizeof(uint32_t));

#ifdef DHT_DISTRIBUTION
    table->access_distribution[dest_rank]++;
#endif

    if (status != UCS_OK) {
      return DHT_UCX_ERROR;
    }

    if (!(*(buffer_begin) & (BUCKET_OCCUPIED))) {
      table->read_misses += 1;
#ifdef DHT_STATISTICS
      table->stats.read_misses += 1;
#endif
      // unlock window and return
      return DHT_READ_MISS;
    }
    if (memcmp((buffer_begin + 1), send_key, table->key_size) != 0) {
      if (curr_index == (table->index_count) - 1) {
        table->read_misses += 1;
#ifdef DHT_STATISTICS
        table->stats.read_misses += 0;
#endif
      }
    } else {

      if (*(buffer_begin) & (BUCKET_INVALID)) {
        return DHT_READ_MISS;
      }

      const void *key_val_begin = buffer_begin + 1;
      const uint32_t *bucket_check =
          (uint32_t *)(buffer_begin + table->data_size + table->key_size + 1);
      if (*bucket_check !=
          CROP_HASH(table->hash_func(table->data_size + table->key_size,
                                     key_val_begin))) {
        if (!!retry) {
          retry = 0;
          curr_index--;
          table->chksum_retries++;
          continue;
        }

        const char invalidate = *(buffer_begin) | BUCKET_INVALID;
        status = ucx_put_blocking(table->ucx_h, dest_rank,
                                  BUCKET_OFFSET(table->offset,
                                                table->index[curr_index],
                                                table->data_displacement),
                                  &invalidate, sizeof(char));
#ifdef DHT_DISTRIBUTION
        table->access_distribution[dest_rank]++;
#endif

        if (status != UCS_OK) {
          return DHT_UCX_ERROR;
        }

        return DHT_READ_MISS;
      }

      break;
    }
  }

  // if matching key was found copy data into memory of passed pointer
  memcpy((char *)destination, (char *)table->recv_entry + table->key_size + 1,
         table->data_size);

  return DHT_SUCCESS;
}

/* int DHT_read_location(DHT *table, uint32_t proc, uint32_t index, */
/*                       void *destination) { */
/*   const uint32_t bucket_size = table->data_size + table->key_size + 1; */

/* #ifdef DHT_STATISTICS */
/*   table->stats->r_access++; */
/* #endif */

/*   // locking window of target rank with shared lock */
/*   if (MPI_Win_lock(MPI_LOCK_SHARED, proc, 0, table->window) != 0) */
/*     return DHT_UCX_ERROR; */
/*   // receive data */
/*   if (MPI_Get(table->recv_entry, bucket_size, MPI_BYTE, proc, index, */
/*               bucket_size, MPI_BYTE, table->window) != 0) { */
/*     return DHT_UCX_ERROR; */
/*   } */

/*   // unlock window of target rank */
/*   if (MPI_Win_unlock(proc, table->window) != 0) */
/*     return DHT_UCX_ERROR; */

/*   // if matching key was found copy data into memory of passed pointer */
/*   memcpy((char *)destination, (char *)table->recv_entry + 1 +
 * table->key_size, */
/*          table->data_size); */

/*   return DHT_SUCCESS; */
/* } */

int DHT_to_file(DHT *table, const char *filename) {
  /* // open file */
  /* MPI_File file; */
  /* if (MPI_File_open(table->communicator, filename, */
  /*                   MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, */
  /*                   &file) != 0) */
  /*   return DHT_FILE_IO_ERROR; */

  /* int rank; */
  /* MPI_Comm_rank(table->communicator, &rank); */

  /* // write header (key_size and data_size) */
  /* if (rank == 0) { */
  /*   if (MPI_File_write_shared(file, &table->key_size, 1, MPI_INT, */
  /*                             MPI_STATUS_IGNORE) != 0) */
  /*     return DHT_FILE_WRITE_ERROR; */
  /*   if (MPI_File_write_shared(file, &table->data_size, 1, MPI_INT, */
  /*                             MPI_STATUS_IGNORE) != 0) */
  /*     return DHT_FILE_WRITE_ERROR; */
  /* } */

  /* MPI_Barrier(table->communicator); */

  /* char *ptr; */
  /* int bucket_size = table->key_size + table->data_size + 1; */

  /* // iterate over local memory */
  /* for (unsigned int i = 0; i < table->table_size; i++) { */
  /*   ptr = (char *)table->mem_alloc + (i * bucket_size); */
  /*   // if bucket has been written to (checked by written_flag)... */
  /*   if (read_flag(*ptr)) { */
  /*     // write key and data to file */
  /*     if (MPI_File_write_shared(file, ptr + 1, bucket_size - 1, MPI_BYTE, */
  /*                               MPI_STATUS_IGNORE) != 0) */
  /*       return DHT_FILE_WRITE_ERROR; */
  /*   } */
  /* } */

  /* MPI_Barrier(table->communicator); */

  /* // close file */
  /* if (MPI_File_close(&file) != 0) */
  /*   return DHT_FILE_IO_ERROR; */

  /* return DHT_SUCCESS; */
}

// int DHT_from_file(DHT *table, const char *filename) {
//   MPI_File file;
//   MPI_Offset f_size;
//   int bucket_size, buffer_size, cur_pos, rank, offset;
//   char *buffer;
//   void *key;
//   void *data;

//   // open file
//   if (MPI_File_open(table->communicator, filename, MPI_MODE_RDONLY,
//                     MPI_INFO_NULL, &file) != 0)
//     return DHT_FILE_IO_ERROR;

//   // get file size
//   if (MPI_File_get_size(file, &f_size) != 0)
//     return DHT_FILE_IO_ERROR;

//   MPI_Comm_rank(table->communicator, &rank);

//   // calculate bucket size
//   bucket_size = table->key_size + table->data_size;
//   // buffer size is either bucket size or, if bucket size is smaller than the
//   // file header, the size of DHT_FILEHEADER_SIZE
//   buffer_size =
//       bucket_size > DHT_FILEHEADER_SIZE ? bucket_size : DHT_FILEHEADER_SIZE;
//   // allocate buffer
//   buffer = (char *)malloc(buffer_size);

//   // read file header
//   if (MPI_File_read(file, buffer, DHT_FILEHEADER_SIZE, MPI_BYTE,
//                     MPI_STATUS_IGNORE) != 0)
//     return DHT_FILE_READ_ERROR;

//   // compare if written header data and key size matches current sizes
//   if (*(int *)buffer != table->key_size)
//     return DHT_WRONG_FILE;
//   if (*(int *)(buffer + 4) != table->data_size)
//     return DHT_WRONG_FILE;

//   // set offset for each process
//   offset = bucket_size * table->comm_size;

//   // seek behind header of DHT file
//   if (MPI_File_seek(file, DHT_FILEHEADER_SIZE, MPI_SEEK_SET) != 0)
//     return DHT_FILE_IO_ERROR;

//   // current position is rank * bucket_size + OFFSET
//   cur_pos = DHT_FILEHEADER_SIZE + (rank * bucket_size);

//   // loop over file and write data to DHT with DHT_write
//   while (cur_pos < f_size) {
//     if (MPI_File_seek(file, cur_pos, MPI_SEEK_SET) != 0)
//       return DHT_FILE_IO_ERROR;
//     // TODO: really necessary?
//     MPI_Offset tmp;
//     MPI_File_get_position(file, &tmp);
//     if (MPI_File_read(file, buffer, bucket_size, MPI_BYTE, MPI_STATUS_IGNORE)
//     !=
//         0)
//       return DHT_FILE_READ_ERROR;
//     // extract key and data and write to DHT
//     key = buffer;
//     data = (buffer + table->key_size);
//     if (DHT_write(table, key, data, NULL, NULL) == DHT_UCX_ERROR)
//       return DHT_UCX_ERROR;

//     // increment current position
//     cur_pos += offset;
//   }

//   free(buffer);
//   if (MPI_File_close(&file) != 0)
//     return DHT_FILE_IO_ERROR;

//   return DHT_SUCCESS;
// }

int DHT_free(DHT *table, uint64_t *eviction_counter,
             uint64_t *readerror_counter, uint64_t *chksum_retries) {
  int64_t buf;

  ucs_status_t status;

  status = ucx_barrier(table->ucx_h);
  if (status != UCS_OK) {
    return status;
  }

  if (eviction_counter != NULL) {
    buf = table->evictions;
    if (UCS_OK != ucx_reduce_sum(table->ucx_h, &buf, 0)) {
      return DHT_UCX_ERROR;
    }
    *eviction_counter = buf;
  }

  if (readerror_counter != NULL) {
    buf = table->read_misses;
    if (UCS_OK != ucx_reduce_sum(table->ucx_h, &buf, 0)) {
      return DHT_UCX_ERROR;
    }
    *readerror_counter = buf;
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
  free(table->index);

#ifdef DHT_STATISTICS
  free(table->stats.writes_local);
#endif

#ifdef DHT_DISTRIBUTION
  free(table->access_distribution);
#endif

  free(table);

  return DHT_SUCCESS;
}

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

#ifdef DHT_DISTRIBUTION
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
      memcpy(distribution[i], table->access_distribution,
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
#ifdef DHT_DISTRIBUTION
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
        table->ucx_h, 0, table->access_distribution,
        table->ucx_h->comm_size * sizeof(uint64_t), table->ucx_h->self_rank);
    if (unlikely(status != UCS_OK)) {
      return DHT_UCX_ERROR;
    }
  }

  if (reset) {
    memset(table->access_distribution, 0,
           table->ucx_h->comm_size * sizeof(uint64_t));
  }

  return DHT_SUCCESS;

#else
  return DHT_NOT_IMPLEMENTED;
#endif
}

int DHT_gather_index_usage(DHT *table, uint64_t ***index_usage, int reset) {
#ifdef DHT_DISTRIBUTION
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