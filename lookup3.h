
#include <stdint.h>     /* defines uint32_t etc */
#include <sys/param.h>  /* attempt to define endianness */
#ifdef linux
# include <endian.h>    /* attempt to define endianness */
#endif

uint32_t hashword(const uint32_t *k, size_t length, uint32_t initval);

void hashword2 (const uint32_t *k, size_t length, uint32_t *pc, uint32_t *pb);

uint32_t hashlittle( const void *key, size_t length, uint32_t initval);

void hashlittle2(const void *key, size_t length, uint32_t *pc, uint32_t *pb);

uint32_t hashbig( const void *key, size_t length, uint32_t initval);
