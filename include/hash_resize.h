#ifndef HASH_RESIZE_H
#define HASH_RESIZE_H

#ifdef DEBUG
#define dbg_printf(fmt, args...)     printf("[debug rculflist] " fmt, ## args)
#else
#define dbg_printf(fmt, args...)				\
do {								\
	/* do nothing but check printf format */		\
	if (0)							\
		printf("[debug rculflist] " fmt, ## args);	\
} while (0)
#endif

#endif /* HASH_RESIZE_H */
