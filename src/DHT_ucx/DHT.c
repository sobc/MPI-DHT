/// Time-stamp: "Last modified 2023-11-10 10:15:30 mluebke"
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

#include <inttypes.h>
#include <math.h>
#include <setjmp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_compat.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>
#include <unistd.h>
#include <xxhash.h>

#include "dht_macros.h"
#include "ucx_communication.h"
#include "ucx_init_deinit.h"

static void determine_dest(uint64_t hash, int comm_size,
                           unsigned int table_size, unsigned int *dest_rank,
                           uint64_t *index, unsigned int index_count) {
  /** temporary index */
  uint64_t tmp_index;
  /** how many bytes do we need for one index? */
  int index_size = sizeof(double) - (index_count - 1);
  for (int i = 0; i < index_count; i++) {
    tmp_index = 0;
    memcpy(&tmp_index, (char *)&hash + i, index_size);
    index[i] = (uint64_t)(tmp_index % table_size);
  }
  *dest_rank = (unsigned int)(hash % comm_size);
}

static void set_flag(char *flag_byte) {
  *flag_byte = 0;
  *flag_byte |= (1 << 0);
}

static int read_flag(char flag_byte) {
  if ((flag_byte & 0x01) == 0x01) {
    return 1;
  } else
    return 0;
}

static uint32_t calc_chksum(const void *begin, int32_t count) {
  return (uint32_t)XXH3_64bits(begin, count);
}

DHT *DHT_create(MPI_Comm comm, uint64_t size, unsigned int data_size,
                unsigned int key_size,
                uint64_t (*hash_func)(int, const void *)) {
  DHT *object;
  MPI_Win window;
  void *mem_alloc;
  int comm_size, index_bytes, rank;

  ucs_status_t status;

  const uint64_t bucket_size = 2 * sizeof(uint32_t) + data_size + key_size + 1;
  uint8_t padding = bucket_size % sizeof(uint32_t);
  if (!!padding) {
    padding = sizeof(uint32_t) - padding;
  }

  uint64_t size_of_dht = size * (bucket_size + padding);

  if (MPI_Comm_size(comm, &comm_size) != 0)
    return NULL;

  if (MPI_Comm_rank(comm, &rank) != 0)
    return NULL;

  // HACK: this will be extinguished in future, as the exchange process will be
  // decoupled from the actual DHT semantics
  MPI_exchange mpi_ex;
  mpi_ex.comm = comm;
  mpi_ex.rank = rank;
  mpi_ex.size = comm_size;

  // calculate how much bytes for the index are needed to address count of
  // buckets per process
  index_bytes = (int)ceil(log2(size));
  if (index_bytes % 8 != 0)
    index_bytes = index_bytes + (8 - (index_bytes % 8));

  // allocate memory for dht-object
  object = (DHT *)malloc(sizeof(DHT));
  CHK_UNLIKELY_RETURN(object == NULL, "allocating DHT object", NULL);

  object->ucx_h = (ucx_handle_t *)malloc(sizeof(ucx_handle_t));
  CHK_UNLIKELY_RETURN(object->ucx_h == NULL, "allocating ucx handle", NULL);

  status = ucx_initContext(&object->ucx_h->ucp_context);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "creating ucx context", NULL);

  ucp_address_t *local_addr_worker;
  uint64_t local_addr_worker_len;

  status =
      ucx_initWorker(object->ucx_h->ucp_context, &object->ucx_h->ucp_worker,
                     &local_addr_worker, &local_addr_worker_len);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "creating worker", NULL);

  status = ucx_createEndpoints(object->ucx_h->ucp_worker, local_addr_worker,
                               local_addr_worker_len, &object->ucx_h->ep_list,
                               &mpi_ex);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "exchange worker addresses", NULL);

  status =
      ucx_createMemory(object->ucx_h->ucp_context, size_of_dht,
                       &object->ucx_h->mem_h, &object->ucx_h->local_mem_addr);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "creating memory", NULL);

  object->ucx_h->comm_size = comm_size;
  object->ucx_h->self_rank = rank;

  status = ucx_exchangeRKeys(object->ucx_h);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "exchange Rkeys", NULL);

  status = ucx_barrier(object->ucx_h);
  CHK_UNLIKELY_RETURN(status != UCS_OK, "barrier", NULL);

  // every memory allocation has 1 additional byte for flags etc.
  /* if (MPI_Alloc_mem(size * (1 + data_size + key_size), MPI_INFO_NULL, */
  /*                   &mem_alloc) != 0) */
  /*   return NULL; */

  /* // since MPI_Alloc_mem doesn't provide memory allocation with the memory
   * set */
  /* // to zero, we're doing this here */
  /* memset(mem_alloc, '\0', size * (1 + data_size + key_size)); */

  /* // create windows on previously allocated memory */
  /* if (MPI_Win_create(mem_alloc, size * (1 + data_size + key_size), */
  /*                    (1 + data_size + key_size), MPI_INFO_NULL, comm, */
  /*                    &window) != 0) */
  /*   return NULL; */

  // fill dht-object
  object->ucx_h->offset =
      data_size + key_size + (sizeof(uint32_t) * 2) + 1 + padding;
  object->ucx_h->flag_padding = padding;
  object->ucx_h->lock_size = sizeof(uint32_t);
  object->data_size = data_size;
  object->key_size = key_size;
  object->table_size = size;
  object->hash_func = hash_func;
  object->comm_size = comm_size;
  object->communicator = comm;
  object->read_misses = 0;
  object->evictions = 0;
  object->recv_entry = malloc(1 + data_size + key_size + sizeof(uint32_t));
  object->send_entry = malloc(1 + data_size + key_size + sizeof(uint32_t));
  object->index_count = 9 - (index_bytes / 8);
  object->index = (uint64_t *)malloc((object->index_count) * sizeof(uint64_t));

  // if set, initialize dht_stats
#ifdef DHT_STATISTICS
  DHT_stats *stats;

  stats = (DHT_stats *)malloc(sizeof(DHT_stats));
  if (stats == NULL)
    return NULL;

  object->stats = stats;
  object->stats->writes_local = (int *)calloc(comm_size, sizeof(int));
  object->stats->old_writes = 0;
  object->stats->read_misses = 0;
  object->stats->evictions = 0;
  object->stats->w_access = 0;
  object->stats->r_access = 0;
#endif

  if (UCS_OK != ucx_initPostRecv(object->ucx_h, object->comm_size)) {
    return NULL;
  }

  return object;
}

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
/*     return DHT_MPI_ERROR; */
/*   for (i = 0; i < table->index_count; i++) { */
/*     if (MPI_Get(table->recv_entry, 1 + table->data_size + table->key_size, */
/*                 MPI_BYTE, dest_rank, table->index[i], */
/*                 1 + table->data_size + table->key_size, MPI_BYTE, */
/*                 table->window) != 0) */
/*       return DHT_MPI_ERROR; */
/*     if (MPI_Win_flush(dest_rank, table->window) != 0) */
/*       return DHT_MPI_ERROR; */

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
/*       return DHT_MPI_ERROR; */
/*   } */
/*   // unlock window of target rank */
/*   if (MPI_Win_unlock(dest_rank, table->window) != 0) */
/*     return DHT_MPI_ERROR; */

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
  unsigned int dest_rank, i;
  int result = DHT_SUCCESS;
  const char *buffer_begin = (char *)table->recv_entry;

#ifdef DHT_STATISTICS
  table->stats->w_access++;
#endif

  // determine destination rank and index by hash of key
  determine_dest(table->hash_func(table->key_size, send_key), table->comm_size,
                 table->table_size, &dest_rank, table->index,
                 table->index_count);

  // concatenating key with data to write entry to DHT
  set_flag((char *)table->send_entry);
  memcpy((char *)table->send_entry + 1, (char *)send_key, table->key_size);
  memcpy((char *)table->send_entry + table->key_size + 1, (char *)send_data,
         table->data_size);

  const uint32_t chksum = calc_chksum((char *)table->send_entry + 1,
                                      table->data_size + table->key_size);

  memcpy((char *)table->send_entry + table->data_size + table->key_size + 1,
         &chksum, sizeof(uint32_t));

  ucs_status_ptr_t get_req;

  for (i = 0; i < table->index_count; i++) {
    get_req =
        ucx_get_data(table->ucx_h, dest_rank, table->index[i],
                     table->data_size + table->key_size + 1 + sizeof(uint32_t),
                     table->recv_entry);

    if (unlikely(UCS_PTR_IS_ERR(get_req))) {
      DHT_MPI_ERROR;
    }

    if (ucx_check_and_wait_completion(table->ucx_h, get_req, CHECK_NO_WAIT) !=
        UCS_OK) {
      return DHT_MPI_ERROR;
    }

    if (UCS_OK != ucx_flush_ep(table->ucx_h, dest_rank)) {
      return DHT_MPI_ERROR;
    }

    if ((read_flag(*(buffer_begin))) == 0) {
#ifdef DHT_STATISTICS
      table->stats->writes_local[dest_rank]++;
#endif
      break;
    }

    if (memcmp((buffer_begin + 1), send_key, table->key_size) != 0) {
      if (i == (table->index_count) - 1) {
        table->evictions += 1;
#ifdef DHT_STATISTICS
        table->stats->evictions += 1;
#endif
        result = DHT_WRITE_SUCCESS_WITH_EVICTION;
        break;
      }
      continue;
    }

    break;
  }

  if (UCS_OK !=
      ucx_write_acquire_lock(table->ucx_h, table->index[i], dest_rank)) {
    return DHT_MPI_ERROR;
  };

  get_req =
      ucx_put_data(table->ucx_h, dest_rank, table->index[i],
                   table->data_size + table->key_size + sizeof(uint32_t) + 1,
                   table->send_entry);

  if (UCS_OK !=
      ucx_check_and_wait_completion(table->ucx_h, get_req, CHECK_NO_WAIT)) {
    return DHT_MPI_ERROR;
  }

  if (UCS_OK != ucx_write_release_lock(table->ucx_h)) {
    return DHT_MPI_ERROR;
  }

  if (proc) {
    *proc = dest_rank;
  }

  if (index) {
    *index = table->index[i];
  }

  return result;
}

int DHT_read(DHT *table, const void *send_key, void *destination) {
  unsigned int dest_rank, i;
  const char *buffer_begin = (char *)table->recv_entry;

#ifdef DHT_STATISTICS
  table->stats->r_access++;
#endif

  // determine destination rank and index by hash of key
  determine_dest(table->hash_func(table->key_size, send_key), table->comm_size,
                 table->table_size, &dest_rank, table->index,
                 table->index_count);

  ucs_status_ptr_t get_req;

  for (i = 0; i < table->index_count; i++) {
    get_req =
        ucx_get_data(table->ucx_h, dest_rank, table->index[i],
                     table->data_size + table->key_size + 1 + sizeof(uint32_t),
                     table->recv_entry);
    int status =
        ucx_check_and_wait_completion(table->ucx_h, get_req, CHECK_NO_WAIT);
    if (status != UCS_OK) {
      return status;
    }

    if (UCS_OK != ucx_flush_ep(table->ucx_h, dest_rank)) {
      return DHT_MPI_ERROR;
    }

    if ((read_flag(*buffer_begin)) == 0) {
      table->read_misses += 1;
#ifdef DHT_STATISTICS
      table->stats->read_misses += 1;
#endif
      // unlock window and return
      return DHT_READ_MISS;
    }
    if (memcmp((buffer_begin + 1), send_key, table->key_size) != 0) {
      if (i == (table->index_count) - 1) {
        table->read_misses += 1;
#ifdef DHT_STATISTICS
        table->stats->read_misses += 1;
#endif
        // unlock window an return
        /* if (MPI_Win_unlock(dest_rank, table->window) != 0) */
        /*   return DHT_MPI_ERROR; */
        /* return DHT_READ_MISS; */
      }
    } else
      break;
  }

  const uint32_t *bucket_check =
      (uint32_t *)(buffer_begin + table->data_size + table->key_size + 1);
  if (*bucket_check !=
      calc_chksum(buffer_begin + 1, table->key_size + table->data_size)) {
    return DHT_READ_MISS;
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
/*     return DHT_MPI_ERROR; */
/*   // receive data */
/*   if (MPI_Get(table->recv_entry, bucket_size, MPI_BYTE, proc, index, */
/*               bucket_size, MPI_BYTE, table->window) != 0) { */
/*     return DHT_MPI_ERROR; */
/*   } */

/*   // unlock window of target rank */
/*   if (MPI_Win_unlock(proc, table->window) != 0) */
/*     return DHT_MPI_ERROR; */

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

int DHT_from_file(DHT *table, const char *filename) {
  MPI_File file;
  MPI_Offset f_size;
  int bucket_size, buffer_size, cur_pos, rank, offset;
  char *buffer;
  void *key;
  void *data;

  // open file
  if (MPI_File_open(table->communicator, filename, MPI_MODE_RDONLY,
                    MPI_INFO_NULL, &file) != 0)
    return DHT_FILE_IO_ERROR;

  // get file size
  if (MPI_File_get_size(file, &f_size) != 0)
    return DHT_FILE_IO_ERROR;

  MPI_Comm_rank(table->communicator, &rank);

  // calculate bucket size
  bucket_size = table->key_size + table->data_size;
  // buffer size is either bucket size or, if bucket size is smaller than the
  // file header, the size of DHT_FILEHEADER_SIZE
  buffer_size =
      bucket_size > DHT_FILEHEADER_SIZE ? bucket_size : DHT_FILEHEADER_SIZE;
  // allocate buffer
  buffer = (char *)malloc(buffer_size);

  // read file header
  if (MPI_File_read(file, buffer, DHT_FILEHEADER_SIZE, MPI_BYTE,
                    MPI_STATUS_IGNORE) != 0)
    return DHT_FILE_READ_ERROR;

  // compare if written header data and key size matches current sizes
  if (*(int *)buffer != table->key_size)
    return DHT_WRONG_FILE;
  if (*(int *)(buffer + 4) != table->data_size)
    return DHT_WRONG_FILE;

  // set offset for each process
  offset = bucket_size * table->comm_size;

  // seek behind header of DHT file
  if (MPI_File_seek(file, DHT_FILEHEADER_SIZE, MPI_SEEK_SET) != 0)
    return DHT_FILE_IO_ERROR;

  // current position is rank * bucket_size + OFFSET
  cur_pos = DHT_FILEHEADER_SIZE + (rank * bucket_size);

  // loop over file and write data to DHT with DHT_write
  while (cur_pos < f_size) {
    if (MPI_File_seek(file, cur_pos, MPI_SEEK_SET) != 0)
      return DHT_FILE_IO_ERROR;
    // TODO: really necessary?
    MPI_Offset tmp;
    MPI_File_get_position(file, &tmp);
    if (MPI_File_read(file, buffer, bucket_size, MPI_BYTE, MPI_STATUS_IGNORE) !=
        0)
      return DHT_FILE_READ_ERROR;
    // extract key and data and write to DHT
    key = buffer;
    data = (buffer + table->key_size);
    if (DHT_write(table, key, data, NULL, NULL) == DHT_MPI_ERROR)
      return DHT_MPI_ERROR;

    // increment current position
    cur_pos += offset;
  }

  free(buffer);
  if (MPI_File_close(&file) != 0)
    return DHT_FILE_IO_ERROR;

  return DHT_SUCCESS;
}

int DHT_free(DHT *table, int *eviction_counter, int *readerror_counter) {
  int buf;

  ucs_status_t status;

  status = ucx_barrier(table->ucx_h);

  if (status != UCS_OK) {
    return status;
  }

  if (eviction_counter != NULL) {
    buf = 0;
    if (MPI_Reduce(&table->evictions, &buf, 1, MPI_INT, MPI_SUM, 0,
                   table->communicator) != 0)
      return DHT_MPI_ERROR;
    *eviction_counter = buf;
  }
  if (readerror_counter != NULL) {
    buf = 0;
    if (MPI_Reduce(&table->read_misses, &buf, 1, MPI_INT, MPI_SUM, 0,
                   table->communicator) != 0)
      return DHT_MPI_ERROR;
    *readerror_counter = buf;
  }
  ucx_releaseRKeys(table->ucx_h->rkey_handles, table->ucx_h->rkey_buffer,
                   table->ucx_h->remote_addr, table->comm_size);
  status =
      ucx_releaseLocalMemory(table->ucx_h->ucp_context, table->ucx_h->mem_h);
  if (unlikely(status != UCS_OK)) {
    return status;
  }
  status = ucx_releaseEndpoints(table->ucx_h->ep_list, table->comm_size,
                                table->ucx_h->ucp_worker);
  if (unlikely(status != UCS_OK)) {
    return status;
  }

  ucx_cleanup(table->ucx_h->ucp_context, table->ucx_h->ucp_worker);

  free(table->ucx_h);

  free(table->recv_entry);
  free(table->send_entry);
  free(table->index);

#ifdef DHT_STATISTICS
  free(table->stats->writes_local);
  free(table->stats);
#endif
  free(table);

  return DHT_SUCCESS;
}

int DHT_print_statistics(DHT *table) {
#ifdef DHT_STATISTICS
  int *written_buckets;
  int *read_misses, sum_read_misses;
  int *evictions, sum_evictions;
  int sum_w_access, sum_r_access, *w_access, *r_access;
  int rank;
  MPI_Comm_rank(table->communicator, &rank);

// disable possible warning of unitialized variable, which is not the case
// since we're using MPI_Gather to obtain all values only on rank 0
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

  // obtaining all values from all processes in the communicator
  if (rank == 0)
    read_misses = (int *)malloc(table->comm_size * sizeof(int));
  if (MPI_Gather(&table->stats->read_misses, 1, MPI_INT, read_misses, 1,
                 MPI_INT, 0, table->communicator) != 0)
    return DHT_MPI_ERROR;
  if (MPI_Reduce(&table->stats->read_misses, &sum_read_misses, 1, MPI_INT,
                 MPI_SUM, 0, table->communicator) != 0)
    return DHT_MPI_ERROR;
  table->stats->read_misses = 0;

  if (rank == 0)
    evictions = (int *)malloc(table->comm_size * sizeof(int));
  if (MPI_Gather(&table->stats->evictions, 1, MPI_INT, evictions, 1, MPI_INT, 0,
                 table->communicator) != 0)
    return DHT_MPI_ERROR;
  if (MPI_Reduce(&table->stats->evictions, &sum_evictions, 1, MPI_INT, MPI_SUM,
                 0, table->communicator) != 0)
    return DHT_MPI_ERROR;
  table->stats->evictions = 0;

  if (rank == 0)
    w_access = (int *)malloc(table->comm_size * sizeof(int));
  if (MPI_Gather(&table->stats->w_access, 1, MPI_INT, w_access, 1, MPI_INT, 0,
                 table->communicator) != 0)
    return DHT_MPI_ERROR;
  if (MPI_Reduce(&table->stats->w_access, &sum_w_access, 1, MPI_INT, MPI_SUM, 0,
                 table->communicator) != 0)
    return DHT_MPI_ERROR;
  table->stats->w_access = 0;

  if (rank == 0)
    r_access = (int *)malloc(table->comm_size * sizeof(int));
  if (MPI_Gather(&table->stats->r_access, 1, MPI_INT, r_access, 1, MPI_INT, 0,
                 table->communicator) != 0)
    return DHT_MPI_ERROR;
  if (MPI_Reduce(&table->stats->r_access, &sum_r_access, 1, MPI_INT, MPI_SUM, 0,
                 table->communicator) != 0)
    return DHT_MPI_ERROR;
  table->stats->r_access = 0;

  if (rank == 0)
    written_buckets = (int *)calloc(table->comm_size, sizeof(int));
  if (MPI_Reduce(table->stats->writes_local, written_buckets, table->comm_size,
                 MPI_INT, MPI_SUM, 0, table->communicator) != 0)
    return DHT_MPI_ERROR;

  if (rank == 0) { // only process with rank 0 will print out results as a
                   // table
    int sum_written_buckets = 0;

    for (int i = 0; i < table->comm_size; i++) {
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
    for (int i = 0; i < table->comm_size; i++) {
      printf("%-11d|%-11d|%-11d||%-11d|%-11d|%-11d|%-11d\n", i,
             written_buckets[i], table->table_size - written_buckets[i],
             w_access[i], r_access[i], read_misses[i], evictions[i]);
    }
    printf("%s\n", pad);
    printf("%-11s|%-11d|%-11d||%-11d|%-11d|%-11d|%-11d\n", "sum",
           sum_written_buckets,
           (table->table_size * table->comm_size) - sum_written_buckets,
           sum_w_access, sum_r_access, sum_read_misses, sum_evictions);

    printf("%s\n", pad);
    printf("%s %d\n",
           "new entries:", sum_written_buckets - table->stats->old_writes);

    printf("\n");
    fflush(stdout);

    table->stats->old_writes = sum_written_buckets;
  }

// enable warning again
#pragma GCC diagnostic pop

  MPI_Barrier(table->communicator);
  return DHT_SUCCESS;
#endif
}
