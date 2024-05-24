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

/**
 * @file DHT.h
 * @author Max Lübke (mluebke@uni-potsdam.de)
 * @brief API to interact with the DHT
 * @version 0.1
 * @date 16 Nov 2017
 *
 * This file implements the creation of a DHT by using the MPI
 * one-sided-communication. There is also the possibility to write or read data
 * from or to the DHT. In addition, the current state of the DHT can be written
 * to a file and read in again later.
 */

#ifndef DHT_H
#define DHT_H

#include <mpi.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>
#include <ucp/api/ucp_def.h>
#include <ucs/type/status.h>

#include "UCX_DataTypes.h"
#include "UCX_bcast_functions.h"

/** Returned if some error in MPI routine occurs. */
#define DHT_UCX_ERROR -1
/** Returned by a call of DHT_read if no bucket with given key was found. */
#define DHT_READ_MISS -2
/** Returned by DHT_read if a corrupt bucket was identified (checksum mismatch).
 */
#define DHT_READ_CORRUPT -3
/** Returned by DHT_read if an already invalid marked bucket was read. */
#define DHT_READ_INVALID -4
/** Returned by DHT_write if a bucket was evicted. */
#define DHT_WRITE_SUCCESS_WITH_EVICTION -5
/** Returned when no errors occured. */
#define DHT_SUCCESS 0

/** Returned by DHT_from_file if the given file does not match expected file. */
#define DHT_WRONG_FILE -11
/** Returned by DHT file operations if MPI file operation fails. */
#define DHT_FILE_IO_ERROR -12
/** Returned by DHT file operations if error occured in MPI_Read operation. */
#define DHT_FILE_READ_ERROR -13
/** Returned by DHT file operations if error occured in MPI_Write operation. */
#define DHT_FILE_WRITE_ERROR -14

/** Returned if no more memory is available. */
#define DHT_NO_MEM -15

/** Returned if function is not implemented. */
#define DHT_NOT_IMPLEMENTED -32

/** Size of the file header in byte. */
#define DHT_FILEHEADER_SIZE 8

#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

typedef struct MPI_exchange_t {
  MPI_Comm comm;
  int size;
  int rank;
} MPI_exchange;

/**
 * Struct which serves as a handler or so called \a DHT-object. Will
 * be created by DHT_create and must be passed as a parameter to every following
 * function. Stores all relevant data.
 * Do not touch outside DHT functions!
 */
typedef struct {
  ucx_handle_t *ucx_h;
  uint64_t offset;
  /** Size of the data of a bucket entry in byte. */
  int data_size;
  /** Size of the key of a bucket entry in byte. */
  int key_size;
  /** Count of buckets for each process. */
  unsigned int bucket_count;
  /** Pointer to a hashfunction. */
  uint64_t (*hash_func)(int, const void *);
  /** Pre-allocated memory where a bucket can be received. */
  void *recv_entry;
  /** Pre-allocated memory where a bucket to send can be stored. */
  void *send_entry;
  /** Count of retries if checksum fails. */
  uint32_t chksum_retries;
  /** Count of possible indeces. */
  unsigned int index_count;
  /** Count of bits to shift index. */
  unsigned char index_shift;
  /** Count of bits to shift rank. */
  unsigned char rank_shift;

  int (*accumulate_callback)(int, void *, int, void *);
#ifdef DHT_STATISTICS
  /** Detailed statistics of the usage of the DHT. */
  struct DHT_stats {
    /** Count of writes to specific process this process did. */
    int64_t *writes_local;
    /** Writes after last call of DHT_print_statistics. */
    int old_writes;
    /** How many read misses occur? */
    int read_misses;
    /** How many buckets where evicted? */
    int evictions;
    /** How many calls of DHT_write() did this process? */
    int w_access;
    /** How many calls of DHT_read() did this process? */
    int r_access;

    uint64_t *index_usage;
  } stats;
#endif
} DHT;

typedef struct DHT_init {
  /** Size of the key of a bucket entry in byte. */
  int key_size;
  /** Size of the data of a bucket entry in byte. */
  int data_size;
  /** Count of buckets for each process. */
  unsigned int bucket_count;
  /** Pointer to a hashfunction. */
  uint64_t (*hash_func)(int, const void *);
  /** Pointer to broadcast function */
  ucx_worker_addr_bootstrap bcast_func;
  /** pointer to broadcast function arguments */
  const void *bcast_func_args;
} DHT_init_t;

/**
 * @brief Sets the accumulate callback function for the DHT table.
 *
 * @param table A pointer to the DHT table.
 * @param callback_func A function pointer to the callback function.
 */
extern void DHT_set_accumulate_callback(DHT *table,
                                        int (*callback_func)(int, void *, int,
                                                             void *));

/**
 * @brief Writes data to the DHT table and accumulates the values.
 *
 * This function writes the specified data to the DHT table and accumulates the
 * values with the existing data at the specified key. The function returns the
 * result of the accumulation operation.
 *
 * @param table The DHT table.
 * @param key The key to write the data to.
 * @param send_size The size of the data to send.
 * @param data The data to write.
 * @param proc The process ID of the destination process.
 * @param index The index of the destination bucket.
 * @param callback_ret Pointer to store the result of the callback function.
 *
 * @return The result of the accumulation operation.
 */
extern int DHT_write_accumulate(DHT *table, const void *key, int send_size,
                                void *data, uint32_t *proc, uint32_t *index,
                                int *callback_ret);

/**
 * @brief Creates a distributed hash table (DHT) object.
 *
 * @param init_params A pointer to a DHT_init_t structure that contains
 * initialization parameters for the DHT.
 *
 * @return A pointer to the DHT object, or NULL if an error occurred.
 */
DHT *DHT_create(const DHT_init_t *init_params);

/**
 * @brief Writes an entry to the distributed hash table.
 *
 * @param table Pointer to the \a DHT-object.
 * @param key Pointer to the key.
 * @param data Pointer to the data.
 * @param proc If not NULL, returns the process number written to.
 * @param index If not NULL, returns the index of the bucket where the data was
 * written to.
 * @return DHT_SUCCESS on success, an error code otherwise.
 */
int DHT_write(DHT *table, void *key, void *data, uint32_t *proc,
              uint32_t *index);

/**
 * @brief Read data from DHT.
 *
 * At the beginning, the target process and all possible indices are determined.
 * After that a SHARED lock on the address window for read access is done
 * and the first entry is retrieved. Now the received key is compared
 * with the key passed to the function. If they coincide the correct data
 * was found. If not it continues with the next index. If the last
 * possible bucket is reached and the keys still do not match the read
 * error counter is incremented. After the window has been released
 * again, the function returns with a corresponding return value (read
 * error or error-free read). The data to be read out is also written to
 * the memory area of the passed pointer.
 *
 * @param table Pointer to the \a DHT-object.
 * @param key Pointer to the key.
 * @param destination Pointer to memory area where retreived data should be
 * stored.
 * @return int Returns either DHT_SUCCESS on success or correspondending error
 * value on read miss or error.
 */
int DHT_read(DHT *table, const void *key, void *destination);

/* extern int DHT_read_location(DHT *table, uint32_t proc, uint32_t index, */
/*                              void *destination); */
/**
 * @brief Write current state of DHT to file.
 *
 * All contents are written as a memory dump, so that no conversion takes place.
 * First, an attempt is made to open or create a file. If this is successful the
 * file header consisting of data and key size is written. Then each process
 * reads its memory area of the DHT and each bucket that was marked as written
 * is added to the file using MPI file operations.
 *
 * @param table Pointer to the \a DHT-object.
 * @param filename Name of the file to write to.
 * @return int Returns DHT_SUCCESS on succes, DHT_FILE_IO_ERROR if file can't be
 * opened/closed or DHT_WRITE_ERROR if file is not writable.
 */
int DHT_to_file(DHT *table, const char *filename);

/**
 * @brief Read state of DHT from file.
 *
 * One needs a previously written DHT file (by DHT_from_file).
 * First of all, an attempt is made to open the specified file. If this is
 * succeeded the file header is read and compared with the current values of the
 * DHT. If the data and key sizes do not differ, one can continue. Each process
 * reads one line of the file and writes it to the DHT with DHT_write. This
 * happens until no more lines are left. The writing is done by the
 * implementation of DHT_write.
 *
 * @param table Pointer to the \a DHT-object.
 * @param filename Name of the file to read from.
 * @return int Returns DHT_SUCCESS on succes, DHT_FILE_IO_ERROR if file can't be
 * opened/closed, DHT_READ_MISS if file is not readable or DHT_WRONG_FILE if
 * file doesn't match expectation. This is possible if the data size or key size
 * is different.
 */
int DHT_from_file(DHT *table, const char *filename);

/**
 * @brief Frees the memory allocated for the DHT data structure and releases
 * the UCX resources.
 *
 * @param table A pointer to the DHT data structure.
 * @param chksum_retries A pointer to a variable that will receive the total
 * number of times the checksum verification failed during a write operation.
 * Can be NULL if the number of checksum retries is not needed.
 *
 * @return UCS_OK on success, or an error code on failure.
 */
int DHT_free(DHT *table, uint32_t *chksum_retries);

/**
 * @brief Prints a table with statistics about current use of DHT.
 *
 * These statistics are from each participated process and also summed up over
 * all processes. Detailed statistics are:
 *  -# occupied buckets (in respect to the memory of this process)
 *  -# free buckets (in respect to the memory of this process)
 *  -# calls of DHT_write (w_access)
 *  -# calls of DHT_read (r_access)
 *  -# read misses (see DHT_READ_MISS)
 *  -# collisions (see DHT_WRITE_SUCCESS_WITH_EVICTION)
 * 3-6 will reset with every call of this function finally the amount of new
 * written entries is printed out (since the last call of this funtion).
 *
 * This is done by collective MPI operations with the root process with rank 0,
 * which will also print a table with all informations to stdout.
 *
 * Also, as this function was implemented for a special case (POET project) one
 * need to define DHT_STATISTICS to the compiler macros to use this
 * function (eg. <emph>gcc -DDHT_STATISTICS ... </emph>).
 * @param table Pointer to the \a DHT-object.
 * @return int Returns DHT_SUCCESS on success or DHT_UCX_ERROR on internal MPI
 * error.
 */
int DHT_print_statistics(DHT *table);

/**
 * Synchronizes all processes in the DHT communicator.
 *
 * @param table The DHT object.
 * @return UCS_OK if the barrier was successful, or an error code if it failed.
 */
int DHT_barrier(DHT *table);

int DHT_gather_distribution(DHT *table, uint64_t ***distribution, int reset);

static inline void DHT_free_distribution(DHT *table, uint64_t **distribution) {
  for (uint32_t i = 0; i < table->ucx_h->comm_size; i++) {
    free(distribution[i]);
  }
  free(distribution);
}

int DHT_gather_index_usage(DHT *table, uint64_t ***index_usage, int reset);

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

#endif /* DHT_H */
