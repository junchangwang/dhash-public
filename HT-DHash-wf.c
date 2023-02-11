/*
 * hash_rebuild.c: Hash table that can dynamically change it's
 * 		   hash function.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, you can access it online at
 * http://www.gnu.org/licenses/gpl-2.0.html.
 *
 * Copyright (c) 2013-2019 Paul E. McKenney, IBM Corporation.
 * Copyright (c) 2019 Akira Yokosawa.
 * Copyright (c) 2021 Junchang Wang.
 * Copyright (c) 2021 Dunwei Liu.
 *
 */

#define _GNU_SOURCE
#define RCU_SIGNAL
#include <urcu.h>
#include "rcuwflist.h"
#include "include/hash_resize.h"
#include "api.h"

/* To turn on related switches in test framework hashtorture.h,
 * which supports the evaluation of different hash table implementations.*/
#define DHASH

/* Use the API set that does not invole struct ht_lock_state. */
#define API_NO_LOCK_STATE

extern int rebuild;
extern int repeatedly_resize;
extern atomic_t enlarge_requests;
extern atomic_t shrink_requests;
extern int max_list_length;
extern int min_avg_load_factor;

/* rebuild_cur points to the node that is in hazard period. */
static struct ht_node *rebuild_cur;

/* Hash-table bucket element. */
struct ht_bucket {
	struct wflist_rcu wflist;
	atomic_t nnodes;
	/* Some statistical counters. */
};

/* Hash-table instance, duplicated at rebuild time. */
struct ht {
	long ht_nbuckets;
	struct ht *ht_new;
	int ht_idx;
	int (*ht_cmp)(struct ht_node *htnp, void *key, uint32_t seed);
	uint32_t hash_seed;
	unsigned long (*ht_gethash)(void *key, uint32_t seed);
	void *(*ht_getkey)(struct ht_node *htnp);
	struct ht_bucket ht_bkt[0];
};

/* Top-level hash-table data structure, including buckets. */
struct hashtab {
	struct ht *ht_cur;
	spinlock_t ht_lock;
};

/* Allocate a hash-table instance. */
struct ht *ht_alloc(unsigned long nbuckets,
	 int (*cmp)(struct ht_node *htnp, void *key, uint32_t seed),
	 unsigned long (*gethash)(void *key, uint32_t seed),
	 void *(*getkey)(struct ht_node *htnp),
	 uint32_t seed)
{
	struct ht *htp;
	int i;

	htp = malloc(sizeof(*htp) + nbuckets * sizeof(struct ht_bucket));
	if (htp == NULL)
		return NULL;
	htp->ht_nbuckets = nbuckets;
	htp->ht_new = NULL;
	htp->ht_idx = 0;
	htp->ht_cmp = cmp;
	htp->ht_gethash = gethash;
	htp->ht_getkey = getkey;
	htp->hash_seed = seed;

	for (i = 0; i < nbuckets; i++) {
		wflist_init_rcu(&(htp->ht_bkt[i].wflist), NULL);
		atomic_set(&htp->ht_bkt[i].nnodes, 0);
	}
	return htp;
}

/* Allocate a full hash table, master plus instance. */
struct hashtab *hashtab_alloc(unsigned long nbuckets,
	      int (*cmp)(struct ht_node *htnp, void *key, uint32_t seed),
	      unsigned long (*gethash)(void *key, uint32_t seed),
	      void *(*getkey)(struct ht_node *htnp),
	      uint32_t seed)
{
	struct hashtab *htp_master;

	htp_master = malloc(sizeof(*htp_master));
	if (htp_master == NULL)
		return NULL;
	htp_master->ht_cur =
		ht_alloc(nbuckets, cmp, gethash, getkey, seed);
	if (htp_master->ht_cur == NULL) {
		free(htp_master);
		return NULL;
	}
	spin_lock_init(&htp_master->ht_lock);
	return htp_master;
}

/* Free a full hash table, master plus instance. */
void hashtab_free(struct hashtab *htp_master)
{
	free(htp_master->ht_cur);
	free(htp_master);
}

/* Read-side lock/unlock functions. */
static void hashtab_lock_lookup(struct hashtab *htp_master)
{
	rcu_read_lock();
}

static void hashtab_unlock_lookup(struct hashtab *htp_master)
{
	rcu_read_unlock();
}

/* Write-side lock/unlock functions. */
static void hashtab_lock_mod(struct hashtab *htp_master)
{
	/*
	 * RCU-based lock-free linked list is used as the building block
	 * of a hash bucket, so no explicit lock operations are required.
	 * rcu_read_lock() is required because an updater needs to perform
	 * list traversal in hashtab_add() and hashtab_del().
	 */
	rcu_read_lock();
}

static void hashtab_unlock_mod(struct hashtab *htp_master)
{
	rcu_read_unlock();
}

/*
 * Finished using a looked up hashtable element.
 */
void hashtab_lookup_done(struct ht_node *htnp)
{
}

/* Get hash bucket corresponding to key. */
static struct ht_bucket * ht_get_bucket(struct ht *htp, void *key,
              				long *b, unsigned long *h)
{
	unsigned long hash = htp->ht_gethash(key, htp->hash_seed);

	*b = hash % htp->ht_nbuckets;
	if (h)
		*h = hash;
	return &htp->ht_bkt[*b];
}

/*
 * Look up a key.  Caller must have acquired either a read-side or update-side
 * lock via either hashtab_lock_lookup() or hashtab_lock_mod().  Note that
 * the return is a pointer to the ht_node: Use offset_of() or equivalent
 * to get a pointer to the full data structure.
 */
struct ht_node *
hashtab_lookup(struct hashtab *htp_master, void *key)
{
	long b;
	struct ht *htp;
	struct ht *htp_new;
	struct ht_bucket *htbp;
	struct ht_bucket *htbp_new;
	struct wflist_snapshot ss;
	struct ht_node *cur_t;

	/* (1) Search the old hash table */
	htp = rcu_dereference(htp_master->ht_cur);
	htbp = ht_get_bucket(htp, key, &b, NULL);
	if (!wflist_find_rcu(&htbp->wflist, (unsigned long)key, &ss)) {
		dbg_printf("Found value %lu in %d (old table)\n",
				ss.cur->key, htp->ht_idx);
		return ss.cur;
	}

	htp_new = rcu_dereference(htp->ht_new);
	if (htp_new == NULL) {
		dbg_printf("Can't found value %lu in %d (old table)\n",
				(unsigned long)key, htp->ht_idx);
		return NULL;
	}

	smp_rmb();
	
	/* (2) Check the node referenced to by rebuild_cur. */
	cur_t = rcu_dereference(rebuild_cur); 
	/*
	 * - hashtab_rebuild hasn't reached the end of the list,
	 * - key values are equal, and
	 * - the node with the matching key hasn't been logically removed.
	 */
	if (cur_t && (cur_t->key == (unsigned long)key)
			&& !logically_removed(cur_t)){
		return get_ptr(cur_t);
	}

	smp_rmb();

	/* (3) Search the new hash table */
	htbp_new = ht_get_bucket(htp_new, key, &b, NULL);
	if (!wflist_find_rcu(&htbp_new->wflist, (unsigned long)key, &ss)) {
		dbg_printf("Found value %lu in %d (new table)\n",
				ss.cur->key, htp_new->ht_idx);
		return ss.cur;
	}
	else {
		dbg_printf("Can't found value %lu in %d (new table)\n",
				(unsigned long)key, htp_new->ht_idx);
		return NULL;
	}
}

/*
 * Insert an element into the hash table.  Caller must have acquired an
 * update-side lock via hashtab_lock_mod().
 */
int hashtab_add(struct hashtab *htp_master, void *key,
				 struct ht_node *htnp)
{
	long b;
	struct ht *htp;
	struct ht_bucket *htbp;
	struct ht *htp_new;
	struct ht_bucket *htbp_new;
	struct wflist_rcu *list;

	ht_node_init_rcu(htnp);
	ht_node_set_key(htnp, (unsigned long)key);
	htp = rcu_dereference(htp_master->ht_cur);
	htp_new = rcu_dereference(htp->ht_new);

	if (htp_new == NULL) {
		/* (1) Insert the node into the only(old) hash table. */
		htbp = ht_get_bucket(htp, key, &b, NULL);
		if (!wflist_insert_rcu(&htbp->wflist, htnp)) {
			dbg_printf("Insert %lu into %d (old table)\n",
					(unsigned long)key, htp->ht_idx);
			if (1) {
				if (atomic_inc_return(&htbp->nnodes) > max_list_length) {
					if (!atomic_read(&enlarge_requests)) {
						/*printf("Registering enlarge_request. Addr: %p, nnodes: %d, old enlarge_req: %d\n",*/
								/*htbp, atomic_read(&htbp->nnodes), atomic_read(&enlarge_requests));*/
						atomic_inc(&enlarge_requests);
					}
				}
			}
			return 0;
		} else {
			list = &htbp->wflist;
		}
	} else {
		/* Or (2) insert the node into the new hash table. */
		htbp_new = ht_get_bucket(htp_new, key, &b, NULL);
		if (!wflist_insert_rcu(&htbp_new->wflist, htnp)) {
			dbg_printf("Insert %lu into %d (new table)\n",
					(unsigned long)key, htp_new->ht_idx);
			if (1) {
				if (atomic_inc_return(&htbp_new->nnodes) > max_list_length) {
					if (!atomic_read(&enlarge_requests)) {
						/*printf("Registering enlarge_request. Addr: %p, nnodes: %d, old enlarge_req: %d\n",*/
								/*htbp_new, atomic_read(&htbp_new->nnodes), atomic_read(&enlarge_requests));*/
						atomic_inc(&enlarge_requests);
					}
				}
			}
			return 0;
		} else {
			list = &htbp_new->wflist;
		}
	}
	BUG_ON(!list);
	if (list->delete_node)
		list->delete_node(htnp);
	return -1;
}

int hashtab_del(struct hashtab *htp_master, struct ht_node *htnp)
{
	long b;
	struct ht *htp;
	struct ht_bucket *htbp;
	struct ht *htp_new;
	struct ht_bucket *htbp_new;
	struct ht_node *cur_t;
	struct wflist_snapshot ss;

	/* (1) Delete the node from the old hash table */
	htp = rcu_dereference(htp_master->ht_cur);
	htbp = ht_get_bucket(htp, (void *)htnp->key, &b, NULL);
	if (!wflist_delete_rcu(&htbp->wflist, htnp->key, &ss, LOGICALLY_REMOVED)) {
		dbg_printf("Delete %lu from %d\n", htnp->key, htp->ht_idx);
		if (1) {
			if (atomic_dec_return(&htbp->nnodes) < 0)
				printf("Error in atomic_dec_return(&htbp->nnodes)\n");
		}
		return 0;
	}

	htp_new = rcu_dereference(htp->ht_new);
	if (htp_new == NULL)
		return -ENOENT;

	smp_rmb();

	/* Delete the node pointed to by rebuild_cur. */
	cur_t = rcu_dereference(rebuild_cur);
	if (cur_t && (cur_t->key == htnp->key)) {
		set_flag(rebuild_cur, LOGICALLY_REMOVED);
		return 0;
	}

	smp_rmb();

	/* (3) Delete the node in the new hash table */
	htbp_new = ht_get_bucket(htp_new, (void *)htnp->key, &b, NULL);
	if (!wflist_delete_rcu(&htbp_new->wflist, htnp->key, &ss, LOGICALLY_REMOVED)) {
		dbg_printf("Delete %lu from %d\n", htnp->key, htp_new->ht_idx);
		if (1) {
			if (atomic_dec_return(&htbp_new->nnodes) < 0)
				printf("Error in atomic_dec_return(&htbp->nnodes)\n");
		}
		return 0;
	}
	return -ENOENT;
}

int hashtab_rebuild(struct hashtab *htp_master,
                   unsigned long nbuckets,
                   int (*cmp)(struct ht_node *htnp, void *key, uint32_t seed),
                   unsigned long (*gethash)(void *key, uint32_t seed),
                   void *(*getkey)(struct ht_node *htnp))
{
	int i;
	struct ht *htp;
	struct ht *htp_new;
	struct ht_node *htnp;
	struct ht_node *curr;
	struct ht_node *curr_ptr;
	struct ht_bucket *htbp;
	struct ht_bucket *htbp_new;
	struct wflist_rcu *list;
	struct wflist_snapshot ss;
	long long starttime;
	long b;

	if (!spin_trylock(&htp_master->ht_lock))
		return -EBUSY;
	htp = rcu_dereference(htp_master->ht_cur);
	htp_new = rebuild ?
		ht_alloc(nbuckets,
			cmp ? cmp : htp->ht_cmp,
			gethash ? gethash : htp->ht_gethash,
			getkey ? getkey : htp->ht_getkey,
			(htp->hash_seed+1)):
		ht_alloc(nbuckets,
			cmp ? cmp : htp->ht_cmp,
			gethash ? gethash : htp->ht_gethash,
			getkey ? getkey : htp->ht_getkey,
			htp->hash_seed);
	if (htp_new == NULL) {
		spin_unlock(&htp_master->ht_lock);
		return -ENOMEM;
	}
	/* ht_idx is used for debugging. */
	htp_new->ht_idx = htp->ht_idx + 1;
	rcu_assign_pointer(htp->ht_new, htp_new);

	synchronize_rcu();

	starttime = get_microseconds();
	for (i = 0; i < htp->ht_nbuckets; i++) {
		htbp = &htp->ht_bkt[i];
		htnp = rcu_dereference(htbp->wflist.head);
		BUG_ON(htnp->key != MIN_KEY);
		curr = rcu_dereference(htnp->next);
		curr_ptr = get_ptr(curr);

		while (curr_ptr->key != MAX_KEY) {
			rcu_assign_pointer(rebuild_cur, curr);

			smp_wmb();

			/* Remove node from the old hash table */
			if (wflist_delete_rcu(&htbp->wflist, curr_ptr->key, 
					&ss, IS_BEING_DISTRIBUTED) != 0) {
				dbg_printf("Rebuild thread failed in deleting %lu\n",
						curr_ptr->key);
				curr = rcu_dereference(htnp->next);
				curr_ptr = get_ptr(curr);
				continue;
			}
			/*if (!repeatedly_resize) {*/
			if (1) {
				if (atomic_dec_return(&htbp->nnodes) < 0)
					printf("Error in atomic_dec_return(&htbp->nnodes)\n");
			}

			BUG_ON(curr_ptr != ss.cur);

			/* Refresh the node pointed to by rebuild_cur. */
			clean_flag(rebuild_cur, IS_BEING_DISTRIBUTED);

			/* Insert the node into the new hash table. */
			htbp_new = ht_get_bucket(htp_new, (void *)curr_ptr->key, &b, NULL);
			if (wflist_insert_rcu(&htbp_new->wflist, curr_ptr)) {
				dbg_printf("Rebuild thread failed in inserting %lu\n",
						curr_ptr->key);
				list = &htbp_new->wflist;
				if (list->delete_node)
					list->delete_node(curr_ptr);
			}
			/*if (!repeatedly_resize) {*/
			if (1) {
				if (atomic_inc_return(&htbp_new->nnodes) > max_list_length) {
					if (!atomic_read(&enlarge_requests))
						atomic_inc(&enlarge_requests);
				}
			}

			smp_wmb();
			
			rcu_assign_pointer(rebuild_cur, NULL);
			dbg_printf("Moving value %lu (next: %p) from %d into %d\n",
					curr_ptr->key, curr_ptr->next, htp->ht_idx, htp_new->ht_idx);
			curr = rcu_dereference(htnp->next);
			curr_ptr = get_ptr(curr);
		}
	}
	starttime = get_microseconds() - starttime;
	printf("\nRebuild done. ID: %d -> %d. Size: %lu -> %lu. Time %lld (microseconds)\n",
			htp->ht_idx, htp_new->ht_idx, htp->ht_nbuckets, nbuckets, starttime);

	synchronize_rcu();

	rcu_assign_pointer(htp_master->ht_cur, htp_new);

	synchronize_rcu();

	spin_unlock(&htp_master->ht_lock);
	for (i = 0; i < htp->ht_nbuckets; i++) {
		htbp = &htp->ht_bkt[i];
		htnp = rcu_dereference(htbp->wflist.head);
		BUG_ON(htnp->key != MIN_KEY);
		curr = rcu_dereference(htnp->next);
		curr_ptr = get_ptr(curr);
		BUG_ON(curr_ptr->key != MAX_KEY);

		free(curr_ptr);
		free(htnp);
	}
	free(htp);
	return 0;
}

void init() {}
void deinit() {}

#define INIT() init()
#define DEINIT() deinit()
#define hashtab_lock_lookup(htp, i) hashtab_lock_lookup((htp))
#define hashtab_unlock_lookup(htp, i) hashtab_unlock_lookup((htp))
#define hashtab_lock_mod(htp, i, h) hashtab_lock_mod((htp))
#define hashtab_unlock_mod(htp, i, h) hashtab_unlock_mod(htp)
#define hashtab_lookup(tid, htp, h, k) hashtab_lookup((htp), (k))
#define hashtab_add(htp, k, htnp, s, tid) hashtab_add((htp), (void *)(k), (htnp))
#define hashtab_del(tid, htnp,s) hashtab_del((htnp), (s))
#define hash_resize_test(htp, n) hashtab_rebuild((htp), (n), NULL, NULL, NULL)
struct ht_lock_state {};

#define ht_elem ht_node

#include "hashtorture-extended.h"

