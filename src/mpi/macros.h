#ifndef DHT_MACROS_H_
#define DHT_MACROS_H_

#define DHT_BIT(i) 1UL << i

#define BUCKET_OCCUPIED DHT_BIT(0)
#define BUCKET_INVALID DHT_BIT(1)

#include <mpi.h>

#define unlikely(_x) __builtin_expect(!!(_x), 0)

#define CHK_UNLIKELY_ACTION(_cond, _msg, _action)                              \
  do {                                                                         \
    if (unlikely(_cond)) {                                                     \
      fprintf(stderr, "DHT: Failed to %s\n", _msg);                            \
      _action;                                                                 \
    }                                                                          \
  } while (0)

#define CHK_UNLIKELY_RETURN(_cond, _msg, _ret)                                 \
  CHK_UNLIKELY_ACTION(_cond, _msg, return _ret)

#define CHECK_MPI(eval)                                                        \
  do {                                                                         \
    int status;                                                                \
    if (unlikely(MPI_SUCCESS != (status = eval))) {                            \
      return status;                                                           \
    }                                                                          \
  } while (0);

#ifdef LUCX_MPI_FINE_LOCK
#define LOCK_DTYPE int64_t
#define LOCK_SIZE sizeof(LOCK_DTYPE)
#else
#define LOCK_SIZE 0
#endif

#endif // DHT_MACROS_H_
