#include <LUCX/DHT.h>

/* void DHT_set_accumulate_callback(DHT *table, */
/*                                  int (*callback_func)(int, void *, int, */
/*                                                       void *)) { */
/*   table->accumulate_callback = callback_func; */
/* } */

int DHT_write_accumulate(DHT *table, const void *send_key, int data_size,
                         void *send_data, uint32_t *proc, uint32_t *index,
                         int *callback_ret) {
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
  /*   if (MPI_Win_lock(MPI_LOCK_EXCLUSIVE, dest_rank, 0, table->window) != 0)
   */
  /*     return DHT_UCX_ERROR; */
  /*   for (i = 0; i < table->index_count; i++) { */
  /*     if (MPI_Get(table->recv_entry, 1 + table->data_size + table->key_size,
   */
  /*                 MPI_BYTE, dest_rank, table->index[i], */
  /*                 1 + table->data_size + table->key_size, MPI_BYTE, */
  /*                 table->window) != 0) */
  /*       return DHT_UCX_ERROR; */
  /*     if (MPI_Win_flush(dest_rank, table->window) != 0) */
  /*       return DHT_UCX_ERROR; */

  /*     // increment eviction counter if receiving key doesn't match sending
   * key
   */
  /*     // entry has write flag and last index is reached. */
  /*     if (read_flag(*(char *)table->recv_entry)) { */
  /*       if (memcmp(send_key, (char *)table->recv_entry + 1, table->key_size)
   * !=
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
  /*     if (MPI_Put(table->send_entry, 1 + table->data_size + table->key_size,
   */
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

  return DHT_NOT_IMPLEMENTED;
}

int DHT_read_location(DHT *table, uint32_t proc, uint32_t index,
                      void *destination) {
  //   const uint32_t bucket_size = table->data_size + table->key_size + 1;

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

  return DHT_NOT_IMPLEMENTED;
}