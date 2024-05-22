#include <DHT_ucx/DHT.h>

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

  return DHT_NOT_IMPLEMENTED;
}

int DHT_from_file(DHT *table, const char *filename) {
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
  //   // buffer size is either bucket size or, if bucket size is smaller than
  //   the
  //   // file header, the size of DHT_FILEHEADER_SIZE
  //   buffer_size =
  //       bucket_size > DHT_FILEHEADER_SIZE ? bucket_size :
  //       DHT_FILEHEADER_SIZE;
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
  //     if (MPI_File_read(file, buffer, bucket_size, MPI_BYTE,
  //     MPI_STATUS_IGNORE)
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

  return DHT_NOT_IMPLEMENTED;
}