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
#include <stdint.h>

/** Returned if some error in MPI routine occurs. */
#define DHT_MPI_ERROR -1
/** Returned by a call of DHT_read if no bucket with given key was found. */
#define DHT_READ_MISS -2
/** Returned by DHT_write if a bucket was evicted. */
#define DHT_WRITE_SUCCESS_WITH_EVICTION -3
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

/** Size of the file header in byte. */
#define DHT_FILEHEADER_SIZE 8

/**
 * Internal struct to store statistics about read and write accesses and also
 * read misses and evictions.
 * <b>All values will be resetted to zero after a call of
 * DHT_print_statistics().</b>
 * Internal use only!
 *
 * @todo There's maybe a better solution than DHT_print_statistics and this
 * struct
 */
typedef struct {
  /** Count of writes to specific process this process did. */
  int *writes_local;
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
} DHT_stats;

/**
 * Struct which serves as a handler or so called \a DHT-object. Will
 * be created by DHT_create and must be passed as a parameter to every following
 * function. Stores all relevant data.
 * Do not touch outside DHT functions!
 */
typedef struct {
  /** Created MPI Window, which serves as the DHT memory area of the process. */
  MPI_Win window;
  /** Size of the data of a bucket entry in byte. */
  int data_size;
  /** Size of the key of a bucket entry in byte. */
  int key_size;
  /** Count of buckets for each process. */
  unsigned int table_size;
  /** MPI communicator of all participating processes. */
  MPI_Comm communicator;
  /** Size of the MPI communicator respectively all participating processes. */
  int comm_size;
  /** Pointer to a hashfunction. */
  uint64_t (*hash_func)(int, const void *);
  /** Pre-allocated memory where a bucket can be received. */
  void *recv_entry;
  /** Pre-allocated memory where a bucket to send can be stored. */
  void *send_entry;
  /** Allocated memory on which the MPI window was created. */
  void *mem_alloc;
  /** Count of read misses over all time. */
  int read_misses;
  /** Count of evictions over all time. */
  int evictions;
  /** Array of indeces where a bucket can be stored. */
  uint64_t *index;
  /** Count of possible indeces. */
  unsigned int index_count;

  int (*accumulate_callback)(int, void *, int, void *);
#ifdef DHT_STATISTICS
  /** Detailed statistics of the usage of the DHT. */
  DHT_stats *stats;
#endif
} DHT;

extern void DHT_set_accumulate_callback(DHT *table,
                                        int (*callback_func)(int, void *, int,
                                                              void *));

extern int DHT_write_accumulate(DHT *table, const void *key, int send_size,
                                void *data, uint32_t *proc, uint32_t *index, int *callback_ret);

/**
 * @brief Create a DHT.
 *
 * When calling this function, the required memory is allocated and a
 * MPI_Window is created. This allows the execution of MPI_Get and
 * MPI_Put operations for one-sided communication. Then the number of
 * indexes is calculated and finally all relevant data is entered into the
 * \a DHT-object which is returned.
 *
 * @param comm MPI communicator which addresses all participating process of the
 * DHT.
 * @param size_per_process Number of buckets per process.
 * @param data_size Size of data in byte.
 * @param key_size Size of the key in byte.
 * @param hash_func Pointer to a hash function. This function must take the size
 * of the key and a pointer to the key as input parameters and return a 64 bit
 * hash.
 * @return DHT* The returned value is the \a DHT-object which serves as a handle
 * for all DHT operations. If an error occured NULL is returned.
 */
extern DHT *DHT_create(MPI_Comm comm, uint64_t size_per_process,
                       unsigned int data_size, unsigned int key_size,
                       uint64_t (*hash_func)(int, const void *));

/**
 * @brief Write data into DHT.
 *
 * When DHT_write is called, the address window is locked with a
 * LOCK_EXCLUSIVE for write access. Now the first bucket is received
 * using MPI_Get and it is checked if the bucket is empty or if the received key
 * matches the passed key. If this is the case, the data of the bucket is
 * overwritten with the new value. If not, the function continues with the next
 * index until no more indexes are available. When the last index is reached and
 * there are no more indexes available, the last examined bucket is replaced.
 * After successful writing, the memory window is released and the function
 * returns.
 *
 * @param table Pointer to the \a DHT-object.
 * @param key Pointer to the key.
 * @param data Pointer to the data.
 * @param proc If not NULL, returns the process number written to.
 * @param index If not NULL, returns the index of the bucket where the data was
 * written to.
 * @return int Returns either DHT_SUCCESS on success or correspondending error
 * value on eviction or error.
 */
extern int DHT_write(DHT *table, void *key, void *data, uint32_t *proc,
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
extern int DHT_read(DHT *table, const void *key, void *destination);

extern int DHT_read_location(DHT *table, uint32_t proc, uint32_t index,
                             void *destination);
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
extern int DHT_to_file(DHT *table, const char *filename);

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
extern int DHT_from_file(DHT *table, const char *filename);

/**
 * @brief Free ressources of DHT.
 *
 * Finally, to free all resources after using the DHT, the function
 * DHT_free must be used. This will free the MPI\_Window, as well as the
 * associated memory. Also all internal variables are released. Optionally, the
 * count of evictions and read misses can also be obtained.
 *
 * @param table Pointer to the \a DHT-object.
 * @param eviction_counter \a optional: Pointer to integer where the count of
 * evictions should be stored.
 * @param readerror_counter \a optional: Pointer to integer where the count of
 * read errors should be stored.
 * @return int Returns either DHT_SUCCESS on success or DHT_MPI_ERROR on
 * internal MPI error.
 */
extern int DHT_free(DHT *table, int *eviction_counter, int *readerror_counter);

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
 * @return int Returns DHT_SUCCESS on success or DHT_MPI_ERROR on internal MPI
 * error.
 */
extern int DHT_print_statistics(DHT *table);

/**
 * @brief Determine destination rank and index.
 *
 * This is done by looping over all possbile indices. First of all, set a
 * temporary index to zero and copy count of bytes for each index into the
 * memory area of the temporary index. After that the current index is
 * calculated by the temporary index modulo the table size. The destination rank
 * of the process is simply determined by hash modulo the communicator size.
 *
 * @param hash Calculated 64 bit hash.
 * @param comm_size Communicator size.
 * @param table_size Count of buckets per process.
 * @param dest_rank Reference to the destination rank variable.
 * @param index Pointer to the array index.
 * @param index_count Count of possible indeces.
 */
static void determine_dest(uint64_t hash, int comm_size,
                           unsigned int table_size, unsigned int *dest_rank,
                           uint64_t *index, unsigned int index_count);

/**
 * @brief Set the occupied flag.
 *
 * This will set the first bit of a bucket to 1.
 *
 * @param flag_byte First byte of a bucket.
 */
static void set_flag(char *flag_byte);

/**
 * @brief Get the occupied flag.
 *
 * This function determines whether the occupied flag of a bucket was set or
 * not.
 *
 * @param flag_byte First byte of a bucket.
 * @return int Returns 1 for true or 0 for false.
 */
static int read_flag(char flag_byte);

#endif /* DHT_H */
