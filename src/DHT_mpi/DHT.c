/// Time-stamp: "Last modified 2023-11-10 11:59:10 mluebke"
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

#include <DHT_mpi/DHT.h>
#include <mpi.h>

#include <inttypes.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static unsigned int index_shift = 0;

static void determine_dest(uint64_t hash, int comm_size,
                           unsigned int table_size, unsigned int *dest_rank,
                           uint64_t *index, unsigned int index_count) {
  /** temporary index */
  uint64_t tmp_index;
  /** how many bytes do we need for one index? */
  int index_size = sizeof(double) - (index_count - 1);
  for (uint32_t i = 0; i < index_count; i++) {
    tmp_index = 0;
    memcpy(&tmp_index, (char *)&hash + i, index_size);
    tmp_index >>= index_shift;
    index[i] = (uint64_t)(tmp_index % table_size);
  }

  const unsigned int rank_shift = 32 - (int)ceil(log2(comm_size));

  *dest_rank = (unsigned int)((hash >> rank_shift) % comm_size);
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

  // FIXME: do not use global variable
  index_shift = 8 * index_bytes - index_bits;

  return index_bytes;
}

DHT *DHT_create(MPI_Comm comm, uint64_t size, unsigned int data_size,
                unsigned int key_size,
                uint64_t (*hash_func)(int, const void *)) {
  DHT *object;
  MPI_Win window;
  void *mem_alloc;
  int comm_size, index_bytes;

  // calculate how much bytes for the index are needed to address count of
  // buckets per process
  index_bytes = (int)ceil(log2(size));
  if (index_bytes % 8 != 0)
    index_bytes = index_bytes + (8 - (index_bytes % 8));

  // allocate memory for dht-object
  object = (DHT *)malloc(sizeof(DHT));
  if (object == NULL)
    return NULL;

  // every memory allocation has 1 additional byte for flags etc.
  if (MPI_Alloc_mem(size * (1 + data_size + key_size), MPI_INFO_NULL,
                    &mem_alloc) != 0)
    return NULL;
  if (MPI_Comm_size(comm, &comm_size) != 0)
    return NULL;

  // since MPI_Alloc_mem doesn't provide memory allocation with the memory set
  // to zero, we're doing this here
  memset(mem_alloc, '\0', size * (1 + data_size + key_size));

  // create windows on previously allocated memory
  if (MPI_Win_create(mem_alloc, size * (1 + data_size + key_size),
                     (1 + data_size + key_size), MPI_INFO_NULL, comm,
                     &window) != 0)
    return NULL;

  // fill dht-object
  object->data_size = data_size;
  object->key_size = key_size;
  object->table_size = size;
  object->window = window;
  object->hash_func = hash_func;
  object->comm_size = comm_size;
  object->communicator = comm;
  object->read_misses = 0;
  object->evictions = 0;
  object->recv_entry = malloc(1 + data_size + key_size);
  object->send_entry = malloc(1 + data_size + key_size);
  object->index_count = 8 - (get_index_bytes(size) - 1);
  object->index = (uint64_t *)malloc((object->index_count) * sizeof(uint64_t));
  object->mem_alloc = mem_alloc;

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

  return object;
}

void DHT_set_accumulate_callback(DHT *table,
                                 int (*callback_func)(int, void *, int,
                                                      void *)) {
  table->accumulate_callback = callback_func;
}

int DHT_write_accumulate(DHT *table, const void *send_key, int data_size,
                         void *send_data, uint32_t *proc, uint32_t *index,
                         int *callback_ret) {
  unsigned int dest_rank, i;
  int result = DHT_SUCCESS;

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
  /* memcpy((char *)table->send_entry + table->key_size + 1, (char *)send_data,
   */
  /*        table->data_size); */

  // locking window of target rank with exclusive lock
  if (MPI_Win_lock(MPI_LOCK_EXCLUSIVE, dest_rank, 0, table->window) != 0)
    return DHT_MPI_ERROR;
  for (i = 0; i < table->index_count; i++) {
    if (MPI_Get(table->recv_entry, 1 + table->data_size + table->key_size,
                MPI_BYTE, dest_rank, table->index[i],
                1 + table->data_size + table->key_size, MPI_BYTE,
                table->window) != 0)
      return DHT_MPI_ERROR;
    if (MPI_Win_flush(dest_rank, table->window) != 0)
      return DHT_MPI_ERROR;

    // increment eviction counter if receiving key doesn't match sending key
    // entry has write flag and last index is reached.
    if (read_flag(*(char *)table->recv_entry)) {
      if (memcmp(send_key, (char *)table->recv_entry + 1, table->key_size) !=
          0) {
        if (i == (table->index_count) - 1) {
          table->evictions += 1;
#ifdef DHT_STATISTICS
          table->stats->evictions += 1;
#endif
          result = DHT_WRITE_SUCCESS_WITH_EVICTION;
          break;
        }
      } else
        break;
    } else {
#ifdef DHT_STATISTICS
      table->stats->writes_local[dest_rank]++;
#endif
      break;
    }
  }

  if (result == DHT_WRITE_SUCCESS_WITH_EVICTION) {
    memset((char *)table->send_entry + 1 + table->key_size, '\0',
           table->data_size);
  } else {
    memcpy((char *)table->send_entry + 1 + table->key_size,
           (char *)table->recv_entry + 1 + table->key_size, table->data_size);
  }

  *callback_ret = table->accumulate_callback(
      data_size, (char *)send_data, table->data_size,
      (char *)table->send_entry + 1 + table->key_size);

  // put data to DHT (with last selected index by value i)
  if (*callback_ret == 0) {
    if (MPI_Put(table->send_entry, 1 + table->data_size + table->key_size,
                MPI_BYTE, dest_rank, table->index[i],
                1 + table->data_size + table->key_size, MPI_BYTE,
                table->window) != 0)
      return DHT_MPI_ERROR;
  }
  // unlock window of target rank
  if (MPI_Win_unlock(dest_rank, table->window) != 0)
    return DHT_MPI_ERROR;

  if (proc) {
    *proc = dest_rank;
  }

  if (index) {
    *index = table->index[i];
  }

  return result;
}

int DHT_write(DHT *table, void *send_key, void *send_data, uint32_t *proc,
              uint32_t *index) {
  unsigned int dest_rank, i;
  int result = DHT_SUCCESS;

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

  // locking window of target rank with exclusive lock
  if (MPI_Win_lock(MPI_LOCK_EXCLUSIVE, dest_rank, 0, table->window) != 0)
    return DHT_MPI_ERROR;
  for (i = 0; i < table->index_count; i++) {
    if (MPI_Get(table->recv_entry, 1 + table->data_size + table->key_size,
                MPI_BYTE, dest_rank, table->index[i],
                1 + table->data_size + table->key_size, MPI_BYTE,
                table->window) != 0)
      return DHT_MPI_ERROR;
    if (MPI_Win_flush(dest_rank, table->window) != 0)
      return DHT_MPI_ERROR;

    // increment eviction counter if receiving key doesn't match sending key
    // entry has write flag and last index is reached.
    if (read_flag(*(char *)table->recv_entry)) {
      if (memcmp(send_key, (char *)table->recv_entry + 1, table->key_size) !=
          0) {
        if (i == (table->index_count) - 1) {
          table->evictions += 1;
#ifdef DHT_STATISTICS
          table->stats->evictions += 1;
#endif
          result = DHT_WRITE_SUCCESS_WITH_EVICTION;
          break;
        }
      } else
        break;
    } else {
#ifdef DHT_STATISTICS
      table->stats->writes_local[dest_rank]++;
#endif
      break;
    }
  }

  // put data to DHT (with last selected index by value i)
  if (MPI_Put(table->send_entry, 1 + table->data_size + table->key_size,
              MPI_BYTE, dest_rank, table->index[i],
              1 + table->data_size + table->key_size, MPI_BYTE,
              table->window) != 0)
    return DHT_MPI_ERROR;
  // unlock window of target rank
  if (MPI_Win_unlock(dest_rank, table->window) != 0)
    return DHT_MPI_ERROR;

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

#ifdef DHT_STATISTICS
  table->stats->r_access++;
#endif

  // determine destination rank and index by hash of key
  determine_dest(table->hash_func(table->key_size, send_key), table->comm_size,
                 table->table_size, &dest_rank, table->index,
                 table->index_count);

  // locking window of target rank with shared lock
  if (MPI_Win_lock(MPI_LOCK_SHARED, dest_rank, 0, table->window) != 0)
    return DHT_MPI_ERROR;
  // receive data
  for (i = 0; i < table->index_count; i++) {
    if (MPI_Get(table->recv_entry, 1 + table->data_size + table->key_size,
                MPI_BYTE, dest_rank, table->index[i],
                1 + table->data_size + table->key_size, MPI_BYTE,
                table->window) != 0)
      return DHT_MPI_ERROR;
    if (MPI_Win_flush(dest_rank, table->window) != 0)
      return DHT_MPI_ERROR;

    // increment read error counter if write flag isn't set ...
    if ((read_flag(*(char *)table->recv_entry)) == 0) {
      table->read_misses += 1;
#ifdef DHT_STATISTICS
      table->stats->read_misses += 1;
#endif
      // unlock window and return
      if (MPI_Win_unlock(dest_rank, table->window) != 0)
        return DHT_MPI_ERROR;
      return DHT_READ_MISS;
    }

    // ... or key doesn't match passed by key and last index reached.
    if (memcmp(((char *)table->recv_entry) + 1, send_key, table->key_size) !=
        0) {
      if (i == (table->index_count) - 1) {
        table->read_misses += 1;
#ifdef DHT_STATISTICS
        table->stats->read_misses += 1;
#endif
        // unlock window an return
        if (MPI_Win_unlock(dest_rank, table->window) != 0)
          return DHT_MPI_ERROR;
        return DHT_READ_MISS;
      }
    } else
      break;
  }

  // unlock window of target rank
  if (MPI_Win_unlock(dest_rank, table->window) != 0)
    return DHT_MPI_ERROR;

  // if matching key was found copy data into memory of passed pointer
  memcpy((char *)destination, (char *)table->recv_entry + table->key_size + 1,
         table->data_size);

  return DHT_SUCCESS;
}

int DHT_read_location(DHT *table, uint32_t proc, uint32_t index,
                      void *destination) {
  const uint32_t bucket_size = table->data_size + table->key_size + 1;

#ifdef DHT_STATISTICS
  table->stats->r_access++;
#endif

  // locking window of target rank with shared lock
  if (MPI_Win_lock(MPI_LOCK_SHARED, proc, 0, table->window) != 0)
    return DHT_MPI_ERROR;
  // receive data
  if (MPI_Get(table->recv_entry, bucket_size, MPI_BYTE, proc, index,
              bucket_size, MPI_BYTE, table->window) != 0) {
    return DHT_MPI_ERROR;
  }

  // unlock window of target rank
  if (MPI_Win_unlock(proc, table->window) != 0)
    return DHT_MPI_ERROR;

  // if matching key was found copy data into memory of passed pointer
  memcpy((char *)destination, (char *)table->recv_entry + 1 + table->key_size,
         table->data_size);

  return DHT_SUCCESS;
}

int DHT_to_file(DHT *table, const char *filename) {
  // open file
  MPI_File file;
  if (MPI_File_open(table->communicator, filename,
                    MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL,
                    &file) != 0)
    return DHT_FILE_IO_ERROR;

  int rank;
  MPI_Comm_rank(table->communicator, &rank);

  // write header (key_size and data_size)
  if (rank == 0) {
    if (MPI_File_write_shared(file, &table->key_size, 1, MPI_INT,
                              MPI_STATUS_IGNORE) != 0)
      return DHT_FILE_WRITE_ERROR;
    if (MPI_File_write_shared(file, &table->data_size, 1, MPI_INT,
                              MPI_STATUS_IGNORE) != 0)
      return DHT_FILE_WRITE_ERROR;
  }

  MPI_Barrier(table->communicator);

  char *ptr;
  int bucket_size = table->key_size + table->data_size + 1;

  // iterate over local memory
  for (unsigned int i = 0; i < table->table_size; i++) {
    ptr = (char *)table->mem_alloc + (i * bucket_size);
    // if bucket has been written to (checked by written_flag)...
    if (read_flag(*ptr)) {
      // write key and data to file
      if (MPI_File_write_shared(file, ptr + 1, bucket_size - 1, MPI_BYTE,
                                MPI_STATUS_IGNORE) != 0)
        return DHT_FILE_WRITE_ERROR;
    }
  }

  MPI_Barrier(table->communicator);

  // close file
  if (MPI_File_close(&file) != 0)
    return DHT_FILE_IO_ERROR;

  return DHT_SUCCESS;
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
  if (MPI_Win_free(&(table->window)) != 0)
    return DHT_MPI_ERROR;
  if (MPI_Free_mem(table->mem_alloc) != 0)
    return DHT_MPI_ERROR;
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
#endif
  return DHT_SUCCESS;
}
