/*
 * hash_resize_Linux.c: A user-space simplified version of the rhashtable
 *                       algorithm in the Linux kernel.
 *
 * "Simplified" means that some sophisticated features are missing:
 *
 *   - Nested Tables to handle GFP_ATOMIC memory allocation failures.
 *     
 *   - Listed Tables to support duplicated nodes.
 *   
 *   - Function hashtable_walk() and its friends.
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
 * Copyright (c) 2021 Dunwei Liu.
 * Copyright (c) 2021 Junchang Wang. (User space implementation)
 * Copyright (c) 2015 Herbert Xu <herbert@gondor.apana.org.au>
 * Copyright (c) 2014-2015 Thomas Graf <tgraf@suug.ch>
 * Copyright (c) 2008-2014 Patrick McHardy <kaber@trash.net>
 *
 */

/* Note:
 *   - The end of the chain is marked with a special bit-nulls marks which has
 *   the least significant bit set but otherwise stores the address of the
 *   hash bucket (check function rht_ptr() for details). This allows lookup
 *   operations to be sure that they have found the end of the right lists.
 *   Lookup operations must perform this check because rhashtable could
 *   erroneously redirect concurrent lookup operations to a wrong list while a
 *   resize operation is in progress (check function hashtab_rebuild_chain()
 *   for details).
 *
 *   - Once a new hash table is assigned to old_tbl->future_tbl, insertions go
 *   into the new table right away. Deletions and lookups will be attempted in
 *   both tables until the rebuild/resize operation completes.
 */

#define _GNU_SOURCE
#define RCU_SIGNAL
#include <urcu.h>
#include "api.h"
#include "HT-RHT.h"

/* Use the API set that does not involve struct ht_lock_state. */
#define API_NO_LOCK_STATE

extern int rebuild;
extern int repeatedly_resize;
extern atomic_t enlarge_requests;
extern atomic_t shrink_requests;
extern int max_list_length;
extern int min_avg_load_factor;

/* Hash table element. */
struct ht_elem {
	struct rcu_head rh;
	struct rhash_head hte_next;
};

/* Hash-table bucket element. */
struct ht_bucket {
	struct rhash_head *htb_head;
	spinlock_t htb_lock;
	atomic_t nnodes;
};

/* Hash-table instance, duplicated at rebuild time. */
struct ht {
	long ht_nbuckets;
	struct ht *ht_new;
	int ht_idx;
	int (*ht_cmp)(struct ht_elem *htnp, void *key, uint32_t seed);
	uint32_t hash_seed;
	unsigned long (*ht_gethash)(void *key, uint32_t seed);
	void *(*ht_getkey)(struct ht_elem *htnp);
	struct ht_bucket ht_bkt[];
};

/* Top-level hash-table data structure, including buckets. */
struct hashtab {
	struct ht *ht_cur;
	spinlock_t ht_lock;
};

/* Allocate a hash-table instance. */
struct ht *ht_alloc(unsigned long nbuckets,
	 int (*cmp)(struct ht_elem *htnp, void *key, uint32_t seed),
	 unsigned long (*gethash)(void *key, uint32_t seed),
	 void *(*getkey)(struct ht_elem *htnp), 
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
		INIT_RHT_NULLS_HEAD(htp->ht_bkt[i].htb_head);
		spin_lock_init(&htp->ht_bkt[i].htb_lock);
		atomic_set(&htp->ht_bkt[i].nnodes, 0);
	}
	return htp;
}

/* Allocate a full hash table, master plus instance. */
struct hashtab *hashtab_alloc(unsigned long nbuckets,
	      int (*cmp)(struct ht_elem *htnp, void *key, uint32_t seed),
	      unsigned long (*gethash)(void *key, uint32_t seed),
	      void *(*getkey)(struct ht_elem *htnp),
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
	rcu_read_lock();
}

static void hashtab_unlock_mod(struct hashtab *htp_master)
{
	rcu_read_unlock();
}

/*
 * Finished using a looked up hashtable element.
 */
void hashtab_lookup_done(struct ht_elem *htnp)
{
}

/*
 * Look up a key.  Caller must have acquired either a read-side or update-side
 * lock via either hashtab_lock_lookup() or hashtab_lock_mod().
 */
struct ht_elem *
hashtab_lookup(struct hashtab *htp_master, void *key)
{
	long b;
	struct ht *htp;
	struct ht_bucket *htbp;
	struct ht_elem *htep;
	struct rhash_head *pos;

	htp = rcu_dereference(htp_master->ht_cur);
restart:
	htbp = ht_get_bucket(htp, key, &b, NULL);
	do {
		rht_for_each_entry(htep, pos, htbp, hte_next) {
			if (htp->ht_cmp(htep, key, htp->hash_seed))
				return htep;
		}
		if (pos != RHT_NULLS_MARKER(&htbp->htb_head))
			dbg_printf("Corner case (errorness-end-of-list) catched\n");
	} while (pos != RHT_NULLS_MARKER(&htbp->htb_head));
		
	htp = rcu_dereference(htp->ht_new);
	if (htp)
		goto restart;

	return NULL;
}

static inline void ht_elem_init(struct ht_elem *htep)
{
	memset(htep, 0, sizeof(*htep));
}

/* Insert node htep into the newest hash table. */
static inline int hashtab_add_slow(struct hashtab *htp_master,
				void *key, struct ht_elem *htep)
{
	long b;
	struct ht *htp;
	struct ht *new_htp;
	struct ht_bucket *htbp;
	struct rhash_head *head;
	int ret = 0;

	new_htp = rcu_dereference(htp_master->ht_cur);
retry:
	htp = new_htp;
	htbp = ht_get_bucket(htp, key, &b, NULL);
	new_htp = rcu_dereference(htp->ht_new);
	if (new_htp)
		goto retry;

	/* htp is the newest ht. Insert htep into it. */
	/* The rhashtable algorithm in the Linux kernel
	 * performs two jobs here. (1) Traversing the list to
	 * calculate the elasticity of the bucket, and (2)
	 * checking if the node has been in the bucket.
	 * Our algorithm omits them because the hashtorture
	 * has performed a lookup operation before invoking
	 * hashtab_add().
	 */
	spin_lock(&htbp->htb_lock);

	head = rcu_dereference(rht_ptr(&htbp->htb_head));
	rcu_assign_pointer(htep->hte_next.next, head);
	rcu_assign_pointer(htbp->htb_head, &htep->hte_next);

	//if (!repeatedly_resize) {
	if (1) {
		if (atomic_inc_return(&htbp->nnodes) > max_list_length) {
			if (!atomic_read(&enlarge_requests)) {
				/*printf("Registering enlarge_request. Addr: %p, nnodes: %d, old enlarge_req: %d\n",*/
				/*htbp, atomic_read(&htbp->nnodes), atomic_read(&enlarge_requests));*/
				atomic_inc(&enlarge_requests);
			}
		}
	}

	spin_unlock(&htbp->htb_lock);

	return ret;
}

/*
 * Insert an element into the hash table.  
 * Before invoking hashtab_add(), rcu_read_lock() has been invoked.
 * We won't leave this read-side critical section until hashtab_add()
 * completes to reuse the hashtorture test framework. The rhashtable
 * implementation in the Linux kernel use a finer strategy to manage the
 * read-side critical section periods, which could be implemented in this
 * version later.
 */
int hashtab_add(struct hashtab *htp_master, void *key,
				 struct ht_elem *htep)
{
	long b;
	struct ht *htp;
	struct ht_bucket *htbp;
	struct ht *htp_new;
	struct rhash_head *head;

	ht_elem_init(htep);
	htp = rcu_dereference(htp_master->ht_cur);
	htbp = ht_get_bucket(htp, key, &b, NULL);
	htp_new = rcu_dereference(htp->ht_new);

	spin_lock(&htbp->htb_lock);

	if (htp_new != NULL) {
		spin_unlock(&htbp->htb_lock);
		return hashtab_add_slow(htp_master, key, htep);
	}

	/* The rhashtable algorithm in the Linux kernel first searches the
	 * node with the matching key in the bucket. The rhashtorture test
	 * framework, however, has performed a search/lookup operation before
	 * invoking hashtab_add() and passed the pointer to the node as the
	 * third argument. So we omit this search operation and delete the
	 * node directly.
	 */
	/* Inserting at the head of the list. */
	head = rcu_dereference(rht_ptr(&htbp->htb_head));
	rcu_assign_pointer(htep->hte_next.next, head);
	rcu_assign_pointer(htbp->htb_head, &htep->hte_next);

	//if (!repeatedly_resize) {
	if (1) {
		if (atomic_inc_return(&htbp->nnodes) > max_list_length) {
			if (!atomic_read(&enlarge_requests)) {
				/*printf("Registering enlarge_request. Addr: %p, nnodes: %d, old enlarge_req: %d\n",*/
				/*htbp, atomic_read(&htbp->nnodes), atomic_read(&enlarge_requests));*/
				atomic_inc(&enlarge_requests);
			}
		}
	}

	spin_unlock(&htbp->htb_lock);

	return 0;
}

int __hashtab_add2(struct ht *htp, void *key, struct ht_elem *htep)
{
	struct ht_bucket *htbp;
	struct rhash_head *head;
	long b;

	htbp = ht_get_bucket(htp, key, &b, NULL);

	spin_lock(&htbp->htb_lock);

	head = rcu_dereference(rht_ptr(&htbp->htb_head));
	rcu_assign_pointer(htep->hte_next.next, head);
	rcu_assign_pointer(htbp->htb_head, &htep->hte_next);

	if (!repeatedly_resize) {
		if (atomic_inc_return(&htbp->nnodes) > max_list_length)
			atomic_inc(&enlarge_requests);
	}

	spin_unlock(&htbp->htb_lock);

	return 0;
}

/* Function hashtab_add() in rhashtable can traverse the hash table list and
 * insert the node into the newest hash table. This simplified version,
 * however, contains at most two hash tables, such that we can have a
 * corresponding, simplified version of hashtab_add().
 */
int hashtab_add2(struct hashtab *htp_master, void *key,
				struct ht_elem *htep)
{
	struct ht *htp, *htp_new;
	int err;

	err = -1;
	ht_elem_init(htep);
	htp = rcu_dereference(htp_master->ht_cur);
	htp_new = rcu_dereference(htp->ht_new);

	if (htp_new) {
		err = __hashtab_add2(htp_new, key, htep);
	} else {
		err = __hashtab_add2(htp, key, htep);
	}
	return err;
}

/* Delete node htep from the hash table htp. */
int __hashtab_del(struct ht *htp, struct ht_elem *htep)
{
	long b;
	struct ht_bucket *htbp;
	struct rhash_head *pos;
	struct rhash_head *obj;
	struct rhash_head **pprev;
	int err = -ENOENT;

	htbp = ht_get_bucket(htp, htp->ht_getkey(htep), &b, NULL);
	if (!htbp)
		return err;

	spin_lock(&htbp->htb_lock);

	pprev = NULL;
	obj = &htep->hte_next;
	rht_for_each(pos, htbp) {
		if (pos != obj) {
			pprev = &pos->next;
			continue;
		}
		obj = rcu_dereference(obj->next);
		if (pprev) {
			rcu_assign_pointer(*pprev, obj);
		} else {
			if (rht_is_a_nulls(obj))
				obj = NULL;
			rcu_assign_pointer(htbp->htb_head, obj);
		}
		rcu_assign_pointer(pos->next, NULL);
		err = 0;

		/*if (!repeatedly_resize) {*/
		if (1) {
			if (atomic_dec_return(&htbp->nnodes) < 0)
				printf("Error in atomic_dec_return(&htbp->nnodes)\n");
		}

		break;
	}

	spin_unlock(&htbp->htb_lock);
	return err;
}

int hashtab_del(struct hashtab *htp_master, struct ht_elem *htep)
{
	struct ht *htp;
	struct ht *htp_new;
	int err;

	err = - ENOENT;
	htp = rcu_dereference(htp_master->ht_cur);
	err = __hashtab_del(htp, htep);
	if (!err)
		return err;
	htp_new = rcu_dereference(htp->ht_new);
	if (htp_new)
		err = __hashtab_del(htp_new, htep);

	return err;
}

static int hashtab_rebuild_chain(struct hashtab *htp_master, struct ht *htp,
				 struct ht *htp_new, unsigned int old_bkt_idx)
{
	struct ht_bucket *htbp;
	struct ht_bucket *htbp_new;
	struct rhash_head *head, *next, *current;
	struct rhash_head **pprev = NULL;
	struct ht_elem *htep;
	long b;
	int err;

	htbp = &htp->ht_bkt[old_bkt_idx];
	spin_lock(&htbp->htb_lock);

	while (1) {
		pprev = NULL;
		err = -ENOENT;
		/* Traverse the list and get the last node. */
		rht_for_each(current, htbp) {
			err = 0;
			next = rcu_dereference(current->next);
			if (rht_is_a_nulls(next))
				break;
			pprev = &current->next;
		}
		/* The list has become empty. */
		if (err)
			break;

		htep = container_of(current, struct ht_elem, hte_next);
		htbp_new = ht_get_bucket(htp_new, htp_new->ht_getkey(htep), &b, NULL);

		/* Insert the node into the new hash table. */
		spin_lock(&htbp_new->htb_lock);

		head = rcu_dereference(rht_ptr(&htbp_new->htb_head));
		rcu_assign_pointer(current->next, head);
		rcu_assign_pointer(htbp_new->htb_head, current);

		spin_unlock(&htbp_new->htb_lock);

		/* ***
		 * ***
		 * NOTE: During this time period, the node _current_ is
		 *       included in both hash tables, such that concurrent
		 *       lookup operations which are visiting this node could
		 *       be erroneously redirected to the bucket in the new
		 *       hash table even though they are supposed to visit the
		 *       bucket in the old hash table. To solve this issue,
		 *       rhashtable (1) allows lookup operations to tolerate
		 *       visiting nodes that does not belong to the right
		 *       bucket, and (2) forces lookup operations to check if
		 *       they are still in the right bucket and to start over,
		 *       if necessary, when they reach the end of the lists
		 *       (check function lookup() for details).
		 * ***
		 * ***
		 */

		/* Delete the node from the old hash table. */
		if (pprev) {
			rcu_assign_pointer(*pprev, next);
		}
		else {
			if (rht_is_a_nulls(next))
				next = NULL;
			rcu_assign_pointer(htbp->htb_head, next);
		}

		/*if (!repeatedly_resize) {*/
		if (1) {
			if (atomic_inc_return(&htbp_new->nnodes) > max_list_length) {
				if (!atomic_read(&enlarge_requests))
					atomic_inc(&enlarge_requests);
			}
		}
	}

	spin_unlock(&htbp->htb_lock);

	return 0;
}

int hashtab_rebuild(struct hashtab *htp_master,
                   unsigned long nbuckets,
                   int (*cmp)(struct ht_elem *htep, void *key, uint32_t seed),
                   unsigned long (*gethash)(void *key, uint32_t seed),
                   void *(*getkey)(struct ht_elem *htep))
{
	int i;
	struct ht *htp;
	struct ht *htp_new;
	long long starttime;
	int err;

	err = 0;
	if (!spin_trylock(&htp_master->ht_lock)) {
		printf("Error in grabbing lock before rebuilding.\n");
	 	return -EBUSY;
	}
	htp = rcu_dereference(htp_master->ht_cur);

	/* rhashtable in the Linux kernel could have a list of hashtab(s). So
	 * it must first traverse the list and find the last hashtab before
	 * rebuilding the hash table. This simplified version, however, allows
	 * at most two hashtabs, so htp->ht_new is the only possible target
	 * hash table to distribute nodes.
	 */

	htp_new = rcu_dereference(htp->ht_new);
	BUG_ON(htp_new);
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

	/* Wait until concurrent insert/lookup/delete operations which are not
	 * aware of the new hash table complete.
	 */
	synchronize_rcu();

	starttime = get_microseconds();
	for (i = 0; i < htp->ht_nbuckets; i++) {
		err = hashtab_rebuild_chain(htp_master, htp, htp_new, i);
		if (err) {
			free(htp_new);
			dbg_printf("Error in rebuilding bucket: %d\n", i);
			goto out;
		}
	}
	starttime = get_microseconds() - starttime;
        printf("\nRebuild done. ID: %d -> %d. Size: %lu -> %lu. Time %lld (microseconds)\n",
                        htp->ht_idx, htp_new->ht_idx, htp->ht_nbuckets, nbuckets, starttime);

	/* Wait until concurrent insert/lookup/delete operations who may work
	 * on the old hash table complete.
	 */
	synchronize_rcu();
	rcu_assign_pointer(htp_master->ht_cur, htp_new);

	/* Wait until concurrent insert/lookup/delete operations who may hold
	 * references to the old hash table complete.
	 */
	synchronize_rcu();
	free(htp);
out:
	spin_unlock(&htp_master->ht_lock);
	return err;
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

#include "hashtorture-extended.h"

