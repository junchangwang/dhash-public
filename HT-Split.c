/*
 * HT-Split.c: Hash table that can dynamically change it's
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
 * Copyright (c) 2021 Junchang Wang.
 * Copyright (c) 2021 Dunwei Liu.
 *
 */

#define _GNU_SOURCE
#define RCU_SIGNAL
#include <urcu.h>
#include "rculflist-split.h"
#include "include/hash_resize.h"
#include "api.h"

/* To turn on related switches in test framework hashtorture.h,
 * which supports the evaluation of different hash table implementations.*/
#define DHASH

/* Use the API set that does not invole struct ht_lock_state. */
#define API_NO_LOCK_STATE

extern atomic_t enlarge_requests;
extern atomic_t shrink_requests;
extern int max_list_length;
extern int max_nbuckets;

#define TRUE 1
#define FALSE 0

#define UNINITIALIZED (NULL)
typedef unsigned int hash_t;
typedef void* mykey_t;
typedef void* value_t;
typedef struct ht_node* mark_ptr_t;

#define PTR_TO_UINT(p) ((unsigned int)(uintptr_t)p)
#define UINT_TO_PTR(p) ((void *)(uintptr_t)p)

/* Hash-table bucket element. */
struct ht_bucket {
	struct lflist_rcu lflist;
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

#include "HT-Split-helper.h"

static void
initialize_bucket(struct ht_bucket *bkt, unsigned bucket)
{
	unsigned parent;
	struct ht_node *node, *res;
	
	parent = get_parent(bucket);
	if (READ_ONCE(bkt[parent].lflist.head) == NULL)
		initialize_bucket(bkt, parent);

	node = calloc(1, sizeof(*node));
	node->key = (unsigned long)bucket;
	node->hash_code = hash_dummy_key(bucket);

	res = lflist_insert_rcu(&bkt[parent].lflist, node);
	if (res) {
		free(node);
		node = get_ptr(res);
	}
	
	WRITE_ONCE(bkt[bucket].lflist.head, ptr_flag(node, 0));
}


/* Allocate a hash-table instance. */
struct ht *ht_alloc(unsigned long nbuckets,
	 int (*cmp)(struct ht_node *htnp, void *key, uint32_t seed),
	 unsigned long (*gethash)(void *key, uint32_t seed),
	 void *(*getkey)(struct ht_node *htnp),
	 uint32_t seed)
{
	int i;
	struct ht *htp;

	htp = malloc(sizeof(*htp) + max_nbuckets * sizeof(struct ht_bucket));
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
		lflist_init_rcu(&(htp->ht_bkt[i].lflist), NULL);
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
	struct ht_node *node, *res;

	htp_master = malloc(sizeof(*htp_master));
	if (htp_master == NULL)
		return NULL;
	htp_master->ht_cur =
		ht_alloc(nbuckets, cmp, gethash, getkey, seed);
	if (htp_master->ht_cur == NULL) {
		free(htp_master);
		return NULL;
	}
	/* Dummy node of bkt[0] */
	node = calloc(1, sizeof(*node));
	node->key = (unsigned long)0;
	node->hash_code = hash_dummy_key(0);
	res = lflist_insert_rcu(&htp_master->ht_cur->ht_bkt[0].lflist, node);
	if (res) {
		free(node);
		printf("ERROR: cannot insert the dummy node of bkt[0] in hashtab_alloc.\n");
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
	unsigned long h;
	unsigned hash;
	struct ht *htp;
	struct ht_bucket *htbp;
	struct lflist_snapshot ss;

	htp = rcu_dereference(htp_master->ht_cur);
	htbp = ht_get_bucket(htp, key, &b, &h);

	if (htp->ht_bkt[b].lflist.head == NULL)
		initialize_bucket(htp->ht_bkt, b);

	hash = (unsigned) h;
	if (!lflist_find_rcu(&htbp->lflist, (unsigned long)key, hash_regular_key(hash), &ss)) {
		dbg_printf("Found value %lu in %d (old table)\n",
				ss.cur->key, htp->ht_idx);
		return ss.cur;
	}
	return NULL;
}

/*
 * Insert an element into the hash table.  Caller must have acquired an
 * update-side lock via hashtab_lock_mod().
 */
int hashtab_add(struct hashtab *htp_master, void *key,
				 struct ht_node *htnp)
{
	long b;
	unsigned long h;
	unsigned hash;
	struct ht *htp;
	struct ht_bucket *htbp;
	struct lflist_rcu *list;

	htp = rcu_dereference(htp_master->ht_cur);
	htbp = ht_get_bucket(htp, key, &b, &h);

	hash = (unsigned) h;
	ht_node_init_rcu(htnp);
	htnp->hash_code = hash_regular_key(hash);
	ht_node_set_key(htnp, (unsigned long)key);

	if (htp->ht_bkt[b].lflist.head == NULL)
		initialize_bucket(htp->ht_bkt, b);

	if (!lflist_insert_rcu(&htbp->lflist, htnp)) {
		dbg_printf("Insert %lu into %d \n", (unsigned long)key, htp->ht_idx);
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
		printf("Error: Failed in inserting new node into hashtable.\n");
		list = &htbp->lflist;
		if (list->delete_node)
			list->delete_node(htnp);
	}

	return -1;
}

int hashtab_del(struct hashtab *htp_master, struct ht_node *htnp)
{
	long b;
	unsigned long h;
	unsigned hash;
	struct ht *htp;
	struct ht_bucket *htbp;
	struct lflist_snapshot ss;

	htp = rcu_dereference(htp_master->ht_cur);
	htbp = ht_get_bucket(htp, (void *)htnp->key, &b, &h);

	hash = (unsigned) h;
	
	if (htp->ht_bkt[b].lflist.head == NULL)
		initialize_bucket(htp->ht_bkt, b);

	if (!lflist_delete_rcu(&htbp->lflist, htnp->key, hash_regular_key(hash), &ss, LOGICALLY_REMOVED)) {
		dbg_printf("Delete %lu from %d\n", htnp->key, htp->ht_idx);
		/*if (!repeatedly_resize) {*/
		if (1) {
			if (atomic_dec_return(&htbp->nnodes) < -8)
				printf("Error in atomic_dec_return(&htbp->nnodes) nnodes:%p: %d\n",
						htbp, atomic_read(&htbp->nnodes));
		}
		return 0;
	}

	return -ENOENT;
}

int list_length(struct ht_node *head)
{
	int cnt = 0;
	struct ht_node *htnp;

	if (!head)
		return 0;
	htnp = get_ptr(rcu_dereference(head->next));

	while (htnp && is_regular_node(htnp->hash_code)) {
		cnt ++;
		htnp = get_ptr(rcu_dereference(htnp->next));
	}
	return cnt;
}

#if 0
int hashtab_rebuild(struct hashtab *htp_master,
                   unsigned long nbuckets,
                   int (*cmp)(struct ht_node *htnp, void *key, uint32_t seed),
                   unsigned long (*gethash)(void *key, uint32_t seed),
                   void *(*getkey)(struct ht_node *htnp))
{
	struct ht *htp;
	struct ht *htp_new;
	long long starttime;

	if (!spin_trylock(&htp_master->ht_lock))
		return -EBUSY;
	htp = rcu_dereference(htp_master->ht_cur);
	htp_new = ht_alloc(nbuckets,
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
	/*rcu_assign_pointer(htp->ht_new, htp_new);*/

	/* not necessary in resizable hash tables. */
	synchronize_rcu();

	starttime = get_microseconds();

	memcpy((void*)&htp_new->ht_bkt[0], (void *)&htp->ht_bkt[0], 
			htp->ht_nbuckets * sizeof(struct ht_bucket));
	for (int i = 0; i < htp_new->ht_nbuckets; i++) {
		int count = list_length(htp_new->ht_bkt[i].lflist.head);
		atomic_set(&htp_new->ht_bkt[i].nnodes, count);
	}

	starttime = get_microseconds() - starttime;
	dbg_printf("\nResize done. ID: %d -> %d. Size: %lu -> %lu. Time %lld (microseconds)\n",
			htp->ht_idx, htp_new->ht_idx, htp->ht_nbuckets, nbuckets, starttime);

	synchronize_rcu();

	rcu_assign_pointer(htp_master->ht_cur, htp_new);

	synchronize_rcu();

	spin_unlock(&htp_master->ht_lock);
	free(htp);
	return 0;
}
#endif

int hashtab_rebuild(struct hashtab *htp_master,
                   unsigned long nbuckets,
                   int (*cmp)(struct ht_node *htnp, void *key, uint32_t seed),
                   unsigned long (*gethash)(void *key, uint32_t seed),
                   void *(*getkey)(struct ht_node *htnp))
{
	struct ht *htp;
	long long starttime;
	unsigned long nbuckets_old;

	if (!spin_trylock(&htp_master->ht_lock))
		return -EBUSY;
	htp = rcu_dereference(htp_master->ht_cur);

	starttime = get_microseconds();

	nbuckets_old = htp->ht_nbuckets;
	WRITE_ONCE(htp->ht_nbuckets, nbuckets);

	synchronize_rcu();

	int max_count = 0;
	int max_count_i = ~1;
	for (int i = 0; i < htp->ht_nbuckets; i++) {
		int count = list_length(htp->ht_bkt[i].lflist.head);
		if (count > max_count) {
			max_count = count;
			max_count_i = i;
		}
		atomic_set(&htp->ht_bkt[i].nnodes, count);
	}

	htp->ht_idx ++;

	starttime = get_microseconds() - starttime;
	printf("\nResize done. ID: %d -> %d. Size: %lu -> %lu. Time %lld (microseconds). Max list length: %d on bkt[%d]\n",
			htp->ht_idx-1, htp->ht_idx, nbuckets_old, nbuckets, starttime, max_count, max_count_i);

	synchronize_rcu();

	spin_unlock(&htp_master->ht_lock);

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

