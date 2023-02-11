/*
 * rculflist.c: RCU-based Lock-Free, ordered singly-linked list
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
#include "dcss/dcss.c"
#include "rculflist-dcss.h"

#define ACCESS_ONCE(x) (*(volatile typeof(x) *)&(x))
#define READ_ONCE(x) \
            ({ typeof(x) ___x = ACCESS_ONCE(x); ___x; })
#define WRITE_ONCE(x, val) ({ ACCESS_ONCE(x) = (val); })

extern dcsspProvider_t *prov;

// htnp: pointer
unsigned long get_flag(struct ht_node *htnp)
{
	return (unsigned long)((uintptr_t)htnp & FLAGS_MASK);
}

// htnp: pointer
struct ht_node * get_ptr(struct ht_node *htnp)
{
	return (struct ht_node *)((uintptr_t)htnp & ~FLAGS_MASK);
}

// htnp: pointer
struct ht_node * ptr_flag(struct ht_node *htnp, unsigned long flag)
{
	return (struct ht_node *)
                    (((uintptr_t)htnp & ~FLAGS_MASK) | flag);
}

// The following two helper functions can only be used
// when you can ensure their is no concurrent updates to htnp.
//
// htnp: pointer
struct ht_node * ptr_2_desc(struct ht_node *htnp)
{
	return (struct ht_node *) ((uintptr_t)htnp << LEFTSHIFT);
}

// htnp: pointer
struct ht_node * desc_2_ptr(struct ht_node *htnp)
{
	return (struct ht_node *) ((uintptr_t)htnp >> LEFTSHIFT);
}

// pointers
void set_next_ptr(int tid, struct ht_node *htnp, struct ht_node *next)
{
// htnp and next is the pointer of the node;
// htnp->next is the descriptor tagptr
	struct ht_node *old_p, *new_p;

	do {
		old_p = read_val(tid, &htnp->next);
		new_p = ptr_flag(next, get_flag(old_p));
	  } while(uatomic_cmpxchg(&htnp->next, ptr_2_desc(old_p), ptr_2_desc(new_p)) != ptr_2_desc(old_p));
}

int is_removed(struct ht_node *htnp)
{
	return ((uintptr_t)htnp & FLAGS_MASK) != 0;
}

int logically_removed(struct ht_node *htnp)
{
	return ((uintptr_t)htnp & LOGICALLY_REMOVED) != 0;
}

void set_snapshot(struct lflist_snapshot *ssp, struct ht_node **prev, 
		     struct ht_node *cur, struct ht_node *next)
{
	// ssp->prev is the descriptor tagptr
	// ssp->cur is the pointer of the cur node
	// ssp->next is the pointer of the next node
	ssp->prev = prev;
	ssp->cur = get_ptr(cur);  
	ssp->next = get_ptr(next);
}


// addr is the address of the descriptor tagptr
struct ht_node * read_val(int tid, void *addr)
{
	return (struct ht_node *) readVal(prov, tid, (casword_t volatile*) addr);
}

int lflist_find_rcu(int tid, struct lflist_rcu *list, unsigned long key,
                            struct lflist_snapshot *ssp)
{
	/* Local variables to record the snapshot */
	struct ht_node *cur_t_p, *next_t_p;
	struct ht_node  **prev, *cur_p, *next_p;
	unsigned long ckey, cmark;

retry:
	prev = &list->head;
	cur_t_p = read_val(tid, prev);    // *prev is the descriptor tagptr 
	cur_p = get_ptr(cur_t_p);         // get the pointer of the cur node

	for (;;) {
		if (cur_p == NULL) {
			/* Have reached the end of the list. */
			set_snapshot(ssp, prev, NULL, NULL);  // cur and next are the pointer of the node.
			return -ENOENT;
		}
		next_t_p = read_val(tid, &cur_p->next); // cur_p->next is the descriptor tagptr 
		next_p = get_ptr(next_t_p);          // get the pointer of the next node
		cmark = get_flag(next_t_p);        // get the flag of the cur node.
		ckey = cur_p->key;

		/* If a new node has been added before cur, go to retry. */
		if (read_val(tid, prev) != cur_p) 
			goto retry;

		if ( !cmark ) {
			if (ckey >= key) {
				set_snapshot(ssp, prev, cur_p, next_p);
				return (ckey == key) ? 0 : -ENOENT;
			}
			prev = &cur_p->next;
		} else {
			/* The node cur has been logically deleted (the 
			 * LOGICALLY_REMOVED/IS_BEEN_DISTRIBUTED is set),
			 * try to physically delete it. */
			if (uatomic_cmpxchg(prev, ptr_2_desc(cur_p), ptr_2_desc(next_p)) == ptr_2_desc(cur_p)){ 
				/* Some framework (e.g., hashtorture) manages
				 * (deletes) nodes by themselves. In these cases,
				 * list->delete_node is initialized to NULL.  */
				if(list->delete_node)
					list->delete_node(cur_p); // delete the cur node
			} else {
				/* One of other threads has physically delete
				 * the node. Retry. */
				goto retry;
			}
		}
		cur_p = next_p;
	}
}

int lflist_insert_rcu(int tid, struct lflist_rcu *list,
                              struct ht_node *node)
{
	unsigned long key = node->key;
	struct lflist_snapshot ss, ss_t;
	struct ht_node *old_p, *new_p;

	for (;;) {
		if (!lflist_find_rcu(tid, list, key, &ss))
			return -EINVAL;
		set_next_ptr(tid, node, ss.cur);
		old_p = ptr_flag(ss.cur, 0UL);
		new_p = ptr_flag(node, 0UL);

		if (uatomic_cmpxchg(ss.prev, ptr_2_desc(old_p), ptr_2_desc(new_p)) == ptr_2_desc(old_p)){
			if (logically_removed(desc_2_ptr(new_p->next))) {
				printf("Notion: Inserting a logically_removed node into the hash table in %s\n",
						__FUNCTION__);
				lflist_find_rcu(tid, list, key, &ss_t);
			}
			return 0;
		}
	}
}

int lflist_insert_dcss(int tid, void **htp_new, void *old1, struct lflist_rcu *list, struct ht_node *node)
{
    	unsigned long key = node->key;
	struct lflist_snapshot ss;
	dcsspresult_t ret;
	
	for (;;) {
		if (!lflist_find_rcu(tid, list, key, &ss))
			return -EINVAL;
		set_next_ptr(tid, node, ss.cur);
		struct ht_node * old2 = ptr_flag(ss.cur, 0UL);
		struct ht_node * new2 = ptr_flag(node, 0UL);

		ret = dcsspVal(prov, tid, (casword_t *) htp_new, (casword_t) old1, (casword_t *) ss.prev, (casword_t) old2, (casword_t) new2);
		/*
		 * The ret has three results.
		 * (1) ret.status = DCSSP_SUCCESS. Insert the node successfully.
		 * (2) ret.status = DCSSP_FAILED_ADDR1. The node has been inserted failure, due to *htp_new != old1.
		 * (3) ret.status = DCSSP_FAILED_ADDR2. The node has been inserted failure, due to *ss.prev != old2.
		 */
		if (ret.status == DCSSP_SUCCESS) {
			return 0;
		} else if (ret.status == DCSSP_FAILED_ADDR1) {
			printf("Note: Fail to insert a node because a rebuild operation has been in progress. (%s)\n",
					__FUNCTION__);
			return -DCSSP_FAILED_ADDR1;
		} 

	}
}

int lflist_delete_rcu(int tid, struct lflist_rcu *list, unsigned long key,
		      struct lflist_snapshot *ssp, unsigned long flag)
{
	/* Local variables to record the snapshot */
	struct ht_node *cur, *next, *next_old_p, *next_new_p, *cur_old_p;
	struct lflist_snapshot ss_t;

	for (;;) {
		if (lflist_find_rcu(tid, list, key, ssp))
			return -EINVAL;
		cur = ssp->cur;
		next = ssp->next;

		/* The node to be deleted is pointed to by ssp->cur.
		 * We first logically deleted it by setting its LOGICALLY_REMOVED.
		 */
		next_old_p = ptr_flag(next, 0UL);       // set ptr_flag
		next_new_p = ptr_flag(next, flag);      // set ptr_flag

		if (uatomic_cmpxchg(&cur->next, ptr_2_desc(next_old_p), ptr_2_desc(next_new_p)) != ptr_2_desc(next_old_p))
			continue;
		
		/* If node pointed to by ssp->cur has been logically deleted,
		 * try to physically delete it.
		 */
		cur_old_p = ptr_flag(cur, 0UL);        // set ptr_flag

		if (uatomic_cmpxchg(ssp->prev, ptr_2_desc(cur_old_p),
				ptr_2_desc(next_old_p)) == ptr_2_desc(cur_old_p)) {			
			/* Some applications (e.g., hashtorture) manages
			 * (destroy) nodes by themselves. For these cases,
			 * list->delete_node is initialized to NULL.  */
			if (list->delete_node){
				list->delete_node(cur);
			}
		} else {
			lflist_find_rcu(tid, list, key, &ss_t);
		}
		return 0;
	}
}
