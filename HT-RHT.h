/*
 * Header file of hash_resize_Linux.
 *
 * NOTE: The following code and comments are written by Junchang according to
 * his knowledge on rhashtable. Should you have any questions on them, you are
 * strongly suggested to check rhashtable.[hc] in the Linux kernel.
 */

#ifndef _LINUX_RHASHTABLE_H
#define _LINUX_RHASHTABLE_H

#include "include/hash_resize.h"

/* Objects in rhashtable should include struct rhash_head which linkes collided
 * objects into a chain.
 */
struct rhash_head {
	struct rhash_head *next;
};

#define BIT(nr)	(1UL << (nr))

#define NULLS_MARKER(value) (1UL | (((long)(value)) << 1))

#define RHT_NULLS_MARKER(ptr)	\
	((void *)NULLS_MARKER(((unsigned long)(ptr)) >> 1))

#define INIT_RHT_NULLS_HEAD(ptr) ((ptr) = NULL)

static inline bool rht_is_a_nulls(const struct rhash_head *ptr)
{
	return ((unsigned long)ptr & 1UL); 
}

#define rht_dereference(p, ht)		rcu_dereference(p)
#define rht_dereference_rcu(p, ht)	rcu_dereference(p)
#define rht_entry(tpos, pos, member) \
	({ tpos = container_of(pos, typeof(*tpos), member); 1; })

/* The least significant bit is used to indicate if this is the last node in
 * the list. Hence, logic is as follows:
 * (1) If _bkt_ contains a valid address, return the valid address by
 *     discarding BIT(0).
 * (2) Otherwise, return _bkt_ with BIT(0) being set.
 */
static inline struct rhash_head * rht_ptr(
	struct rhash_head *const *bkt)
{
	return (struct rhash_head *)
		((unsigned long)*bkt & ~BIT(0) ? :
		(unsigned long)RHT_NULLS_MARKER(bkt));
}

static inline void rht_assign_locked(struct rhash_head **bkt,
				     struct rhash_head *obj)
{
	struct rhash_head **p = (struct rhash_head **)bkt;
	
	if (rht_is_a_nulls(obj))
		obj = NULL;
	rcu_assign_pointer(*p, (void *)((unsigned long)obj | BIT(0)));
}

static inline void rht_assign_unlocked(struct rhash_head **bkt,
				       struct rhash_head *obj)
{
	struct rhash_head **p = (struct rhash_head **)bkt;
	
	if (rht_is_a_nulls(obj))
		obj = NULL;
	rcu_assign_pointer(*p, obj);
}

#define rht_for_each_from(pos, head) 		\
	for (pos = head;			\
	     !rht_is_a_nulls(pos);		\
	     pos = rcu_dereference(pos->next))

#define rht_for_each(pos, tbl)					\
	rht_for_each_from(pos, 						\
		rcu_dereference(rht_ptr(&tbl->htb_head)))

#define rht_for_each_entry_from(tpos, pos, head, member)		\
	for (pos = head;						\
	     (!rht_is_a_nulls(pos)) && pos && rht_entry(tpos, pos, member);	\
	     pos = rcu_dereference((pos)->next))

#define rht_for_each_entry(tpos, pos, tbl, member)				\
	rht_for_each_entry_from(tpos, pos,				\
			        rcu_dereference(rht_ptr(&tbl->htb_head)),	\
			        member)

#endif /* _LINUX_RHASHTABLE_H */
