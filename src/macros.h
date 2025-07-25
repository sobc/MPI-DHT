#ifndef DHT_MACROS_H_
#define DHT_MACROS_H_

#define DHT_BIT(i) 1UL << i

#define BUCKET_OCCUPIED DHT_BIT(0)
#define BUCKET_INVALID DHT_BIT(1)

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

#endif // DHT_MACROS_H_
