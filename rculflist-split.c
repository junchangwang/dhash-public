/*
 * rculflist-split.c: RCU-based Lock-Free, ordered singly-linked list
 *
 * Copyright (c) 2021 Dunwei Liu.
 * Copyright (c) 2021 Junchang Wang
 * Copyright (c) 2019 Paul E. McKenney
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 *
 * Based on the following research paper:
 * - Maged M. Michael. High performance dynamic lock-free hash tables
 *   and list-based sets. In Proceedings of the fourteenth annual ACM
 *   symposium on Parallel algorithms and architectures, ACM Press,
 *   (2002), 73-82.
 *
 * Some specificities of this Lock-free linked list implementation:
 * - The original algorithm prevents the ABA problem by adding a tag field
 *   in each hash-table node, whereas this implementation addresses this
 *   issue by using the RCU mechanism.
 */
#define _LGPL_SOURCE

#include <errno.h>
#include <urcu/uatomic.h>
#include <urcu/pointer.h>
#include "rculflist-split.h"

#define ACCESS_ONCE(x) (*(volatile typeof(x) *)&(x))
#define READ_ONCE(x) \
            ({ typeof(x) ___x = ACCESS_ONCE(x); ___x; })
#define WRITE_ONCE(x, val) ({ ACCESS_ONCE(x) = (val); })

unsigned long get_flag(struct ht_node *htnp)
{
	return (unsigned long)((uintptr_t)htnp & FLAGS_MASK);
}

struct ht_node * get_ptr(struct ht_node *htnp)
{
	return (struct ht_node *)((uintptr_t)htnp & ~FLAGS_MASK);
}

struct ht_node * ptr_flag(struct ht_node *htnp, unsigned long flag)
{
	return (struct ht_node *)
                    (((uintptr_t)htnp & ~FLAGS_MASK) | flag);
}

void set_next_ptr(struct ht_node *htnp, struct ht_node *next)
{
	struct ht_node *old, *new;

	do {
		old = READ_ONCE(htnp->next);
		new = ptr_flag(next, get_flag(old));
	} while (uatomic_cmpxchg(&htnp->next, old, new) != old);
}

int is_removed(struct ht_node *htnp)
{
	return ((uintptr_t)htnp & FLAGS_MASK) != 0;
}

int logically_removed(struct ht_node *htnp)
{
	return ((uintptr_t)htnp & LOGICALLY_REMOVED) != 0;
}

void set_snapshot(struct lflist_snapshot *ssp,struct ht_node **prev,
		     struct ht_node *cur, struct ht_node *next)
{
	ssp->prev = prev;
	ssp->cur = get_ptr(cur);
	ssp->next = get_ptr(next);
}

int lflist_find_rcu(struct lflist_rcu *list, unsigned long key, unsigned int hash_code,
                            struct lflist_snapshot *ssp)
{
	/* Local variables to record the snapshot */
	struct ht_node *cur_t, *next_t;
	struct ht_node **prev, *cur, *next;
	unsigned long ckey, cmark;
	unsigned int chash;

retry:
	prev = &list->head;
	cur_t = rcu_dereference(*prev);
	cur = get_ptr(cur_t);

	for (;;) {
		if (cur == NULL) {
			/* Have reached the end of the list. */
			set_snapshot(ssp, prev, NULL, NULL);
			return -ENOENT;
		}
		next_t = rcu_dereference(cur->next);
		next = get_ptr(next_t);
		cmark = get_flag(next_t);
		chash = cur->hash_code;
		ckey = cur->key;

		/* If a new node has been added before cur, go to retry. */
		if (READ_ONCE(*prev) != cur)
			goto retry;

		if ( !cmark ) {
			if (chash >= hash_code) {
				set_snapshot(ssp, prev, cur, next);
				return ((ckey == key) && (chash == hash_code)) ?
					0 : -ENOENT;
			}
			prev = &cur->next;
		} else {
			/* The node cur has been logically deleted (the 
			 * LOGICALLY_REMOVED/IS_BEEN_DISTRIBUTED is set),
			 * try to physically delete it. */
			if (uatomic_cmpxchg(prev, cur, next) == cur){
				/* Some framework (e.g., hashtorture) manages
				 * (deletes) nodes by themselves. In these cases,
				 * list->delete_node is initialized to NULL.  */
				if(list->delete_node)
					list->delete_node(cur);
			} else {
				/* One of other threads has physically delete
				 * the node. Retry. */
				goto retry;
			}
		}
		cur = next;
	}
}

void * lflist_insert_rcu(struct lflist_rcu *list, struct ht_node *node)
{
	unsigned long key = node->key;
	struct lflist_snapshot ss;
	unsigned int hash_code = node->hash_code;

	for (;;) {
		/* Note: In Kumpera's code, if find failed, it returns the existing node
		 * with the same key. */
		if (!lflist_find_rcu(list, key, hash_code, &ss))
			if (ss.cur->hash_code == node->hash_code &&
					ss.cur->key == node->key)
				return ss.cur;
		set_next_ptr(node, ss.cur);
		if (uatomic_cmpxchg(ss.prev, ptr_flag(ss.cur, 0UL),
				ptr_flag(node, 0UL)) == ptr_flag(ss.cur, 0UL)) {
			return NULL;
		}
	}
}

int lflist_delete_rcu(struct lflist_rcu *list, unsigned long key, unsigned int hash_code,
		      struct lflist_snapshot *ssp, unsigned long flag)
{
	/* Local variables to record the snapshot */
	struct ht_node *cur, *next;
	struct lflist_snapshot ss_t;

	for (;;) {
		if (lflist_find_rcu(list, key, hash_code, ssp))
			return -EINVAL;
		cur = ssp->cur;
		next = ssp->next;

		if (cur->hash_code != hash_code || cur->key != key)
			return -EINVAL;

		/* The node to be deleted is pointed to by ssp->cur.
		 * We first logically deleted it by setting its LOGICALLY_REMOVED.
		 */
		if (uatomic_cmpxchg(&cur->next, ptr_flag(next, 0UL),
				ptr_flag(next, flag)) != ptr_flag(next, 0UL))
			continue;
		/* If node pointed to by ssp->cur has been logically deleted,
		 * try to physically delete it.
		 */
		if (uatomic_cmpxchg(ssp->prev, ptr_flag(cur, 0UL),
				ptr_flag(next, 0UL)) == ptr_flag(cur, 0UL)) {
			/* Some applications (e.g., hashtorture) manages
			 * (destroy) nodes by themselves. For these cases,
			 * list->delete_node is initialized to NULL.  */
			if (list->delete_node)
				list->delete_node(cur);
		} else {
			lflist_find_rcu(list, key, hash_code, &ss_t);
		}
		return 0;
	}
}
