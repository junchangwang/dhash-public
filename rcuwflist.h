#ifndef _URCU_RCUWFLIST_H
#define _URCU_RCUWFLIST_H

/*
 * rcuwflist.h: RCU-based ordered singly-linked list with
 * 		wait-free lookup operations.
 *
 * Copyright (c) 2021 Junchang Wang
 * Copyright (c) 2021 Dunwei Liu.
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
 */
#define _GNU_SOURCE

#include <urcu/call-rcu.h>
#include "include/hash_resize.h"
#include "api.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * wflist reserves the least-significant 2 bits to record control information.
 */
#define RESERVED_BITS_LEN (2)

/* Used to indicate that the node has been logically removed from the list.
 * Subsequent list traversal operations will (1) physically remove the node
 * from the list and (2) reclaim the node memory.
 */
#define LOGICALLY_REMOVED 	(1UL << 0)

/* Used to indicate that the node has been logically removed from the list.
 * Subsequent list traversal operations will physically remove the node
 * from the list (NO reclamation).
 * This flag is set when the rebuild operation distributes one node from
 * the old linked list to the new one. The node needs to be removed from the 
 * old linked list. However, the node memory should not be reclaimed because
 * the node will be inserted into the new linked list.
 */
#define IS_BEING_DISTRIBUTED	(1UL << 1)

#define FLAGS_MASK 	((1UL << RESERVED_BITS_LEN) - 1)

#define MIN_KEY (0UL)
#define MAX_KEY (~0UL)

/*
 * Define the structure of list node and related functions.
 */
struct ht_node {
	struct rcu_head rh;
	unsigned long key;
	struct ht_node *next; /* (legal pointer | FLAGS) */
	spinlock_t lock;
} __attribute__((aligned(4)));

/*
 * get_flag - retrieve the flag information (the least-significant
 * 		RESERVED_BITS_LEN bits) out of a ht_node pointer.
 */
extern
unsigned long get_flag(struct ht_node *htnp);

/*
 * get_ptr - cast a ht_node pointer to a legal pointer, by discarding
 * 		its flag information.
 */
extern
struct ht_node * get_ptr(struct ht_node *htnp);

/*
 * ptr_flag - combine a pointer and its flag information
 *
 */
extern
struct ht_node * ptr_flag(struct ht_node *htnp, unsigned long flag);

/*
 * set_flag - write flag information to the node pointed to by htnp.
 */
//struct ht_node * set_flag(struct ht_node *htnp, unsigned long flag);
#define set_flag(htnp, flag) \
	(void) __atomic_or_fetch(&(htnp)->next, (flag), __ATOMIC_SEQ_CST)

/*
 * clean_flag - clean flag information from the node pointed to by htnp.
 */
//struct ht_node * clean_flag(struct ht_node *htnp, unsigned long flag);
#define clean_flag(htnp, flag) \
	(void) __atomic_and_fetch(&(htnp)->next, ~(flag), __ATOMIC_SEQ_CST);

/*
 * set_next_ptr - set the printer information of the next field of the node.
 *                The flag information of the next field is kept untouched.
 */
extern
void set_next_ptr(struct ht_node *htnp, struct ht_node *next);

/*
 * is_removed - check if any of the least-significant RESERVED_BITS_LEN
 * 		bits is set.
 */
extern
int is_removed(struct ht_node *ptr);

/*
 * logically_removed - check if the LOGICALLY_REMOVED bit is set.
 */
extern
int logically_removed(struct ht_node *ptr);

static inline
void ht_node_init_rcu(struct ht_node *node)
{
	node->key = 0UL;
	node->next = NULL;
	spin_lock_init(&node->lock);
}

static inline
void ht_node_set_key(struct ht_node *node, unsigned long key)
{
	node->key = key;
}

struct wflist_snapshot {
	struct ht_node **prev;
	struct ht_node *cur;
	struct ht_node *next;
};

extern
void set_snapshot(struct wflist_snapshot *ssp,struct ht_node **prev,
		     struct ht_node *cur, struct ht_node *next);

/*
 * Define the struct of lock-free list and its helper functions.
 */
struct wflist_rcu {
	struct ht_node *head;
	void (*delete_node)(struct ht_node *);
};

static inline
int wflist_init_rcu(struct wflist_rcu *list,
			  void (*node_free)(struct ht_node *))
{
	struct ht_node *min, *max;

	list->delete_node = node_free;
	min = calloc(1, sizeof(*min));
	ht_node_init_rcu(min);
	ht_node_set_key(min, MIN_KEY);
	list->head = min;
	max = calloc(1, sizeof(*max));
	ht_node_init_rcu(max);
	ht_node_set_key(max, MAX_KEY);
	min->next = max;

	return 0;
}

/*************************************************************************
 * NOTE: Functions wflist_*_rcu() must be invoked within RCU read-side
 * 	 critical sections.
 ************************************************************************/
/*
 * Function wflist_find_rcu() returns SUCCESS(0) if the node with a
 * matching key was found in the list, or returns -Exxx otherwise. No matter
 * if find_rcu() returns success or failure, by its completion, it guarantees
 * that ssp points to the snapshot of a segment of the list. Within the snapshot,
 * field cur points to the node that contains the lowest key value greater than
 * or equal to the input key, and prev is the predecessor pointer of cur.
 */
extern
int wflist_find_rcu(struct wflist_rcu *list, unsigned long key,
			struct wflist_snapshot *ssp);

/*
 * Function wflist_insert_rcu first invokes wflist_find_rcu(),
 * and returns -EINVAL if a node with a matching key is found in the list.
 * Otherwise, this function inserts the new node before the node ss.cur 
 * and returns SUCCESS(0).
 */
extern
int wflist_insert_rcu(struct wflist_rcu *list, struct ht_node *node);

/*
 * Function wflist_delete_rcu returns -EINVAL if the key is not found.
 * Otherwise, the key value of the node pointed to by ss.cur must be equal to
 * the input key. The function deletes the node by (1) marking the node pointed
 * to by ss.cur as deleted, and (2) swinging prev->next to point to next.
 */
extern
int wflist_delete_rcu(struct wflist_rcu *list, unsigned long key,
			struct wflist_snapshot *ssp, unsigned long flag);

#ifdef __cplusplus
}
#endif

#endif /*_URCU_RCUWFLIST_H */
