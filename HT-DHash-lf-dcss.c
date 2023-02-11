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
 * Copyright (c) 2021 Junchang Wang.
 * Copyright (c) 2021 Dunwei Liu.
 *
 */

#define _GNU_SOURCE
#define RCU_SIGNAL
#include <assert.h>
#include <urcu.h>
#include <urcu/uatomic.h>
#include <urcu/pointer.h>
#include "rculflist-dcss.h"
#include "include/hash_resize.h"
#include "api.h"
#include "dcss/dcss.h"

/* To turn on related switches in test framework hashtorture.h,
 * which supports the evaluation of different hash table implementations.*/
//#define DHASH

// To enable the special free procedure in hashtorture.c.
#define DHASHCAS2

/* Use the API set that does not invole struct ht_lock_state. */
#define API_NO_LOCK_STATE

extern int rebuild;
extern int repeatedly_resize;
extern int nworkers;
extern atomic_t enlarge_requests;
extern atomic_t shrink_requests;
extern int max_list_length;
extern int min_avg_load_factor;
extern int rebuild_multi_thread;
extern int rebuild_threads;
#define MAX_REBUILD_THREADS (32)
extern thread_id_t rebuild_tids[];


/* rebuild_cur points to the node that is in hazard period. */

/* FIXME: rebuild_cur[] is heavily accessed by rebuilding threads. To avoid
 * cache misses, each item should be cache_line aligned. However, to that end,
 * an extra of indirection is required, which inevitably complicates our
 * presentation and more importantly proof of correctness.
 * Furthermore, "sudo perf record -e cache-references,cache-misses ./HT-DHash-lf"
 * shows that rebuild_cur[] is NOT the top hot points of cache misses. 
 * Therefore we omit the cache-line alignments temporarily.
 * BTW, the hot points are in find_list when traversing the list, which is inevitable
 * for list-based linked lists.
 */
static struct ht_node *rebuild_cur[MAX_REBUILD_THREADS];

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

dcsspProvider_t *prov;
void init(){
	prov = malloc(sizeof(*prov));
	dcsspProvider_init (prov, nworkers + rebuild_threads + 1);
}

void deinit(){
	dcsspProvider_finish(prov);
	free(prov);
}
/*
 * Look up a key.  Caller must have acquired either a read-side or update-side
 * lock via either hashtab_lock_lookup() or hashtab_lock_mod().  Note that
 * the return is a pointer to the ht_node: Use offset_of() or equivalent
 * to get a pointer to the full data structure.
 */
struct ht_node *
hashtab_lookup(int tid, struct hashtab *htp_master, void *key)
{
	long b;
	int i;
	struct ht *htp;
	struct ht *htp_new;
	struct ht_bucket *htbp;
	struct ht_bucket *htbp_new;
	struct lflist_snapshot ss;
	struct ht_node *cur_t;

	/* (1) Search the old hash table */
	htp = rcu_dereference(htp_master->ht_cur);
	htbp = ht_get_bucket(htp, key, &b, NULL);
	if (!lflist_find_rcu(tid, &htbp->lflist, (unsigned long)key, &ss)) {
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
	for (i = 0; i < rebuild_threads; i++) {
		cur_t = rcu_dereference(rebuild_cur[i]); 
		/*
		 * - hashtab_rebuild hasn't reached the end of the list,
		 * - key values are equal, and
		 * - the node with the matching key hasn't been logically removed.
		 */
		if (cur_t && (cur_t->key == (unsigned long)key)) {
			struct ht_node *next_p = read_val(tid, &cur_t->next);
			if (!logically_removed(next_p))
				return get_ptr(cur_t);
		}
	}

	smp_rmb();

	/* (3) Search the new hash table */
	htbp_new = ht_get_bucket(htp_new, key, &b, NULL);
	if (!lflist_find_rcu(tid, &htbp_new->lflist, (unsigned long)key, &ss)) {
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
			struct ht_node *htnp, int tid)
{
	int ret;
	long b;
	struct ht *htp;
	struct ht_bucket *htbp;
	struct ht *htp_new;
	struct ht_bucket *htbp_new;
	struct lflist_rcu *list;

	ht_node_init_rcu(htnp);
	ht_node_set_key(htnp, (unsigned long)key);
	htp = rcu_dereference(htp_master->ht_cur);
retry:
	htp_new = rcu_dereference(htp->ht_new);

	if (htp_new == NULL) {
		/* (1) Insert the node into the only(old) hash table. */
		htbp = ht_get_bucket(htp, key, &b, NULL);

		ret = lflist_insert_dcss(tid, (void **) &htp->ht_new, (void *) NULL, &htbp->lflist, htnp);

		if (ret == 0) {
			dbg_printf("Insert %lu into %d (old table)\n",
					(unsigned long)key, htp->ht_idx);
			if (1) {
				if (atomic_inc_return(&htbp->nnodes) > max_list_length) {
					if (!atomic_read(&enlarge_requests)) {
						atomic_inc(&enlarge_requests);
					}
				}
			}
			return 0;
		} else if (ret == -DCSSP_FAILED_ADDR1){
			goto retry;
		} else {
			assert( ret == -EINVAL );
			list = &htbp->lflist;
		}
	} else {
		if (hashtab_lookup(tid, htp_master, (void *)htnp->key)) {
			return -EEXIST;
		}
		/* Or (2) insert the node into the new hash table. */
		htbp_new = ht_get_bucket(htp_new, key, &b, NULL);
		if (!lflist_insert_rcu(tid, &htbp_new->lflist, htnp)) {
			if (1) {
				if (atomic_inc_return(&htbp_new->nnodes) > max_list_length) {
					if (!atomic_read(&enlarge_requests)) {
						atomic_inc(&enlarge_requests);
					}
				}
			}
			return 0;
		} else {
			list = &htbp_new->lflist;
		}
	}
	BUG_ON(!list);
	if (list->delete_node)
		list->delete_node(htnp);
	return -1;
}

int hashtab_del(int tid, struct hashtab *htp_master, struct ht_node *htnp)
{
	int i;
	long b;
	struct ht *htp;
	struct ht_bucket *htbp;
	struct ht *htp_new;
	struct ht_bucket *htbp_new;
	struct ht_node *cur_t;
	struct lflist_snapshot ss;

	/* (1) Delete the node from the old hash table */
	htp = rcu_dereference(htp_master->ht_cur);
	htbp = ht_get_bucket(htp, (void *)htnp->key, &b, NULL);
	if (!lflist_delete_rcu(tid, &htbp->lflist, htnp->key, &ss, LOGICALLY_REMOVED)) {
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
	for (i = 0; i < rebuild_threads; i++) {
		cur_t = rcu_dereference(rebuild_cur[i]);
		while (cur_t && (cur_t->key == htnp->key)) {
            struct ht_node *next_old_p = read_val(tid, &cur_t->next);
			if (logically_removed(next_old_p))
        		break;
            if (cmpxchg(&cur_t->next, ptr_2_desc(next_old_p), ptr_2_desc((struct ht_node *)((uintptr_t)next_old_p | LOGICALLY_REMOVED))) 
							== ptr_2_desc(next_old_p)) {
            	return 0;		// Success
			}
            cur_t = rcu_dereference(rebuild_cur[i]);
        }
	}

	smp_rmb();

	/* (3) Delete the node in the new hash table */
	htbp_new = ht_get_bucket(htp_new, (void *)htnp->key, &b, NULL);
	if (!lflist_delete_rcu(tid, &htbp_new->lflist, htnp->key, &ss, LOGICALLY_REMOVED)) {
		dbg_printf("Delete %lu from %d\n", htnp->key, htp_new->ht_idx);
		if (1) {
			if (atomic_dec_return(&htbp_new->nnodes) < 0)
				printf("Error in atomic_dec_return(&htbp->nnodes)\n");
		}
		return 0;
	}
	return -ENOENT;
}

struct rebuild_args {
	struct ht *htp;
	struct ht *htp_new;
	int thread_id;
};

extern int cpustride;

void *rebuild_func(void *thread_arg)
{
	int tid;
	struct ht *htp;
	struct ht *htp_new;
	struct ht_bucket *htbp;
	struct ht_bucket *htbp_new;
	struct ht_node *htnp_d, *htnp_p;
	struct lflist_rcu *list;
	struct lflist_snapshot ss;
	int i;
	long b;
	int maxcpus = sysconf(_SC_NPROCESSORS_CONF);
	BUG_ON(maxcpus <= 0);

	struct rebuild_args *arg = (struct rebuild_args *)thread_arg;
	htp = arg->htp;
	htp_new = arg->htp_new;
	tid = arg->thread_id;

	printf("Rebuilding thread %d\n", tid);
	/* FIXME: This is a temporary fix for servers with dozens of cores:
	 * we map worker threads to CPU cores with odd numbers,
	 * and rebuilding threads to cores with even numbers.
	 * A smarter, flexible solution is required.	*/
	run_on((tid * cpustride) % maxcpus);

	rcu_register_thread();

	for (i = (tid - nworkers); i < htp->ht_nbuckets; i += rebuild_threads ) {
		htbp = &htp->ht_bkt[i];
		htnp_d = rcu_dereference(htbp->lflist.head);
		htnp_p = read_val(tid, &htnp_d);
		while (htnp_p) {
			rcu_assign_pointer(rebuild_cur[tid], htnp_p);

			smp_wmb();

			/* Remove node from the old hash table */
			if (lflist_delete_rcu(tid, &htbp->lflist, htnp_p->key, 
						&ss, IS_BEING_DISTRIBUTED) != 0) {
				dbg_printf("Rebuild thread failed in deleting %lu\n",
						htnp_p->key);
				htnp_d = rcu_dereference(htbp->lflist.head);
				htnp_p = read_val(tid, &htnp_d);
				continue;
			}
			if (1) {
				if (atomic_dec_return(&htbp->nnodes) < 0)
					printf("Error in atomic_dec_return(&htbp->nnodes)\n");
			}

			BUG_ON(htnp_p != ss.cur);

			/* Clean the flag IS_BEING_DISTRIBUTED. */
			while (1) {
				struct ht_node *next_old_p = read_val(tid, &htnp_p->next);
				if (cmpxchg(&htnp_p->next, ptr_2_desc(next_old_p), ptr_2_desc((struct ht_node *)((uintptr_t)next_old_p & ~IS_BEING_DISTRIBUTED))) 
								== ptr_2_desc(next_old_p))
					break;
				next_old_p = read_val(tid, &htnp_p->next);
			}

			/* Guarantee: IS_BEING_DISTRIBUTED is not set. LOGICALLY_REMOVED is possible. */

			/* Insert the node into the new hash table. */
			htbp_new = ht_get_bucket(htp_new, (void *)htnp_p->key, &b, NULL);
			if (lflist_insert_rcu(tid, &htbp_new->lflist, htnp_p)) {
				dbg_printf("Rebuild thread failed in inserting %lu\n",
						htnp_p->key);
				list = &htbp_new->lflist;
				if (list->delete_node)
					list->delete_node(htnp_p);
			}
			if (1) {
				if (atomic_inc_return(&htbp_new->nnodes) > max_list_length) {
					if (!atomic_read(&enlarge_requests))
						atomic_inc(&enlarge_requests);
				}
			}

			smp_wmb();

			rcu_assign_pointer(rebuild_cur[tid], NULL);
			dbg_printf("Moving value %lu (next: %p) from %d into %d\n",
					htnp_p->key, htnp_p->next, htp->ht_idx, htp_new->ht_idx);
			htnp_d = rcu_dereference(htbp->lflist.head);
			htnp_p = read_val(tid, &htnp_d);
		}
	}

	rcu_unregister_thread();

	return NULL;
}

/* Multi-threaded version. */
int hashtab_rebuild(struct hashtab *htp_master,
                   unsigned long nbuckets,
                   int (*cmp)(struct ht_node *htnp, void *key, uint32_t seed),
                   unsigned long (*gethash)(void *key, uint32_t seed),
                   void *(*getkey)(struct ht_node *htnp))
{
	int i;
	struct ht *htp;
	struct ht *htp_new;
	long long starttime;
	void *vp;
	struct rebuild_args args[MAX_REBUILD_THREADS];

	if (!spin_trylock(&htp_master->ht_lock))
		return -EBUSY;

	if (rebuild_multi_thread)
		memset(rebuild_tids, 0, sizeof(thread_id_t) * 32);

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

	for (i = 0; i < rebuild_threads; i++) {
		args[i].htp = htp;
		args[i].htp_new = htp_new;
		args[i].thread_id = i + nworkers; // the thread_id of rebuild threads is 0 1 ... rebuild_threads-1
		if (pthread_create(&rebuild_tids[i], NULL, rebuild_func, &args[i]) != 0) {
			perror("Create_thread:pthread_create");
			exit(EXIT_FAILURE);
		}
	}

	starttime = get_microseconds();

	for (i = 0; i < rebuild_threads; i++) {
		if (pthread_join(rebuild_tids[i], &vp) != 0) {
			perror("wait_thread:pthread_join");
			exit(EXIT_FAILURE);
		}
	}

	starttime = get_microseconds() - starttime;
	printf("\nRebuild done. ID: %d -> %d. Size: %lu -> %lu. Time %lld (microseconds)\n",
			htp->ht_idx, htp_new->ht_idx, htp->ht_nbuckets, nbuckets, starttime);

	synchronize_rcu();

	rcu_assign_pointer(htp_master->ht_cur, htp_new);

	synchronize_rcu();

	spin_unlock(&htp_master->ht_lock);
	free(htp);
	return 0;
}

#define hashtab_lock_lookup(htp, i) hashtab_lock_lookup((htp))
#define hashtab_unlock_lookup(htp, i) hashtab_unlock_lookup((htp))
#define hashtab_lock_mod(htp, i, h) hashtab_lock_mod((htp))
#define hashtab_unlock_mod(htp, i, h) hashtab_unlock_mod(htp)
#define hashtab_lookup(tid, htp, h, k) hashtab_lookup((tid), (htp), (k))
#define hashtab_add(htp, k, htnp, s, tid) hashtab_add((htp), (void *)(k), (htnp), (tid))
//#define hashtab_add(tid, htp, k, htnp) hashtab_add((tid), (htp), (void *)(k), (htnp))
#define hashtab_del(tid, htnp,s) hashtab_del((tid), (htnp), (s))
#define hash_resize_test(htp, n) hashtab_rebuild((htp), (n), NULL, NULL, NULL)
#define INIT() init()
#define DEINIT() deinit()
struct ht_lock_state {};

#define ht_elem ht_node

#include "hashtorture-extended.h"
