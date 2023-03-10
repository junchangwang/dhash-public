/*
 * hash_resize_s.c: Resizable hash table protected by a per-bucket lock for
 *	updates and RCU for lookups (less bucket updates).
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
 * Copyright (c) 2021 Paul E. McKenney, Facebook.
 */

#define _GNU_SOURCE
#define _LGPL_SOURCE
#define RCU_SIGNAL
#include <urcu.h>
#include "api.h"

extern int rebuild;
extern int repeatedly_resize;
extern atomic_t enlarge_requests;
extern atomic_t shrink_requests;
extern int max_list_length;
extern int min_avg_load_factor;

/* Hash-table element to be included in structures in a hash table. */
struct ht_elem {
	struct rcu_head rh;
	struct cds_list_head hte_next[2];		//\lnlbl{ht_elem:next}
#ifndef FCV_SNIPPET
	unsigned long hte_hash;
#endif /* #ifndef FCV_SNIPPET */
};

/* Hash-table bucket element. */
struct ht_bucket {
	struct cds_list_head htb_head;
	spinlock_t htb_lock;
	atomic_t nnodes;
};

struct ht_lock_state {					//\lnlbl{hls:b}
	struct ht_bucket *hbp[2];
#ifndef FCV_SNIPPET
	unsigned long hls_hash[2];
#endif /* #ifndef FCV_SNIPPET */
	int hls_idx[2];
};							//\lnlbl{hls:e}

/* Hash-table instance, duplicated at resize time. */
struct ht {						//\lnlbl{ht:b}
	long ht_nbuckets;				//\lnlbl{ht:nbuckets}
	long ht_resize_cur;				//\lnlbl{ht:resize_cur}
	struct ht *ht_new;				//\lnlbl{ht:new}
	int ht_idx;					//\lnlbl{ht:idx}
	int (*ht_cmp)(struct ht_elem *htep,		//\lnlbl{ht:cmp}
	              void *key, uint32_t seed);
	uint32_t hash_seed;
	unsigned long (*ht_gethash)(void *key, uint32_t seed);
	void *(*ht_getkey)(struct ht_elem *htep);	//\lnlbl{ht:getkey}
	struct ht_bucket ht_bkt[0];			//\lnlbl{ht:bkt}
};							//\lnlbl{ht:e}

/* Top-level hash-table data structure, including buckets. */
struct hashtab {					//\lnlbl{hashtab:b}
	struct ht *ht_cur;
	spinlock_t ht_lock;
};							//\lnlbl{hashtab:e}

/* Allocate a hash-table instance. */
struct ht *
ht_alloc(unsigned long nbuckets,
	 int (*cmp)(struct ht_elem *htep, void *key, uint32_t seed),
	 unsigned long (*gethash)(void *key, uint32_t seed),
	 void *(*getkey)(struct ht_elem *htep),
	 uint32_t seed)
{
	struct ht *htp;
	int i;

	htp = malloc(sizeof(*htp) + nbuckets * sizeof(struct ht_bucket));
	if (htp == NULL)
		return NULL;
	htp->ht_nbuckets = nbuckets;
	htp->ht_resize_cur = -1;
	htp->ht_new = NULL;
	htp->ht_idx = 0;
	htp->ht_cmp = cmp;
	htp->ht_gethash = gethash;
	htp->ht_getkey = getkey;
	htp->hash_seed = seed;
	for (i = 0; i < nbuckets; i++) {
		CDS_INIT_LIST_HEAD(&htp->ht_bkt[i].htb_head);
		spin_lock_init(&htp->ht_bkt[i].htb_lock);
		atomic_set(&htp->ht_bkt[i].nnodes, 0);
	}
	return htp;
}

/* Allocate a full hash table, master plus instance. */
struct hashtab *
hashtab_alloc(unsigned long nbuckets,
	      int (*cmp)(struct ht_elem *htep, void *key, uint32_t seed),
	      unsigned long (*gethash)(void *key, uint32_t seed),
	      void *(*getkey)(struct ht_elem *htep),
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

/* Get hash bucket corresponding to key, ignoring the possibility of resize. */
static struct ht_bucket *				//\lnlbl{single:b}
ht_get_bucket(struct ht *htp, void *key,
              long *b, unsigned long *h)
{
	unsigned long hash = htp->ht_gethash(key, htp->hash_seed);

	*b = hash % htp->ht_nbuckets;			//\lnlbl{single:gethash}
	if (h)
		*h = hash;				//\lnlbl{single:h}
	return &htp->ht_bkt[*b];			//\lnlbl{single:return}
}							//\lnlbl{single:e}

/* Search the bucket for the specfied key in the specified ht structure. */
static struct ht_elem *					//\lnlbl{hsb:b}
ht_search_bucket(struct ht *htp, void *key)
{
	long b;
	struct ht_elem *htep;
	struct ht_bucket *htbp;

	htbp = ht_get_bucket(htp, key, &b, NULL);	//\lnlbl{hsb:get_curbkt}
	cds_list_for_each_entry_rcu(htep,		//\lnlbl{hsb:loop:b}
	                            &htbp->htb_head,
	                            hte_next[htp->ht_idx]) {
		if (htp->ht_cmp(htep, key, htp->hash_seed)) 
			return htep;			//\lnlbl{hsb:ret_match}
	}						//\lnlbl{hsb:loop:e}
	return NULL;					//\lnlbl{hsb:ret_NULL}
}							//\lnlbl{hsb:e}

/* Read-side lock/unlock functions. */
static void hashtab_lock_lookup(struct hashtab *htp_master, void *key)
{
	rcu_read_lock();
}

static void hashtab_unlock_lookup(struct hashtab *htp_master, void *key)
{
	rcu_read_unlock();
}

//\begin{snippet}[labelbase=ln:datastruct:hash_resize_s:lock_mod,commandchars=\\\@\$]
/* Update-side lock/unlock functions. */
static void						//\lnlbl{l:b}
hashtab_lock_mod(struct hashtab *htp_master, void *key,
                 struct ht_lock_state *lsp)
{
	long b;
	unsigned long h;
	struct ht *htp;
	struct ht_bucket *htbp;

	rcu_read_lock();				//\lnlbl{l:rcu_lock}
	htp = rcu_dereference(htp_master->ht_cur);	//\lnlbl{l:refhashtbl}
	htbp = ht_get_bucket(htp, key, &b, &h);		//\lnlbl{l:refbucket}
	spin_lock(&htbp->htb_lock);			//\lnlbl{l:acq_bucket}
	lsp->hbp[0] = htbp;				//\lnlbl{l:lsp0b}
	lsp->hls_idx[0] = htp->ht_idx;
#ifndef FCV_SNIPPET
	lsp->hls_hash[0] = h;				//\lnlbl{l:lsp0e}
#endif /* #ifndef FCV_SNIPPET */
	if (b > READ_ONCE(htp->ht_resize_cur)) {	//\lnlbl{l:ifresized}
		lsp->hbp[1] = NULL;			//\lnlbl{l:lsp1_1}
		return;					//\lnlbl{l:fastret1}
	}
	htp = rcu_dereference(htp->ht_new);		//\lnlbl{l:new_hashtbl}
	htbp = ht_get_bucket(htp, key, &b, &h);		//\lnlbl{l:get_newbkt}
	spin_lock(&htbp->htb_lock);			//\lnlbl{l:acq_newbkt}
	lsp->hbp[1] = lsp->hbp[0];			//\lnlbl{l:lsp1b}
	lsp->hls_idx[1] = lsp->hls_idx[0];
#ifndef FCV_SNIPPET
	lsp->hls_hash[1] = lsp->hls_hash[0];
#endif /* #ifndef FCV_SNIPPET */
	lsp->hbp[0] = htbp;
	lsp->hls_idx[0] = htp->ht_idx;
#ifndef FCV_SNIPPET
	lsp->hls_hash[0] = h;				//\lnlbl{l:lsp1e}
#endif /* #ifndef FCV_SNIPPET */
}							//\lnlbl{l:e}
//\end{snippet}

static void						//\lnlbl{ul:b}
hashtab_unlock_mod(struct ht_lock_state *lsp)
{
	spin_unlock(&lsp->hbp[0]->htb_lock);		//\lnlbl{ul:relbkt0}
	if (lsp->hbp[1])				//\lnlbl{ul:ifbkt1}
		spin_unlock(&lsp->hbp[1]->htb_lock);	//\lnlbl{ul:relbkt1}
	rcu_read_unlock();				//\lnlbl{ul:rcu_unlock}
}							//\lnlbl{ul:e}

/*
 * Finished using a looked up hashtable element.
 */
void hashtab_lookup_done(struct ht_elem *htep)
{
}

//\begin{snippet}[labelbase=ln:datastruct:hash_resize_s:access,commandchars=\\\@\$]
/*
 * Look up a key.  Caller must have acquired either a read-side or update-side
 * lock via either hashtab_lock_lookup() or hashtab_lock_mod().  Note that
 * the return is a pointer to the ht_elem: Use offset_of() or equivalent
 * to get a pointer to the full data structure.
 */
struct ht_elem *					//\lnlbl{lkp:b}
hashtab_lookup(struct hashtab *htp_master, void *key)
{
	struct ht *htp;
	struct ht_elem *htep;

	htp = rcu_dereference(htp_master->ht_cur);	//\lnlbl{lkp:get_curtbl}
	htep = ht_search_bucket(htp, key);		//\lnlbl{lkp:get_curbkt}
	if (htep)					//\lnlbl{lkp:entchk}
		return htep;				//\lnlbl{lkp:ret_match}
	htp = rcu_dereference(htp->ht_new);		//\lnlbl{lkp:get_nxttbl}
	if (!htp)					//\lnlbl{lkp:htpchk}
		return NULL;				//\lnlbl{lkp:noresize}
	return ht_search_bucket(htp, key);		//\lnlbl{lkp:ret_nxtbkt}
}							//\lnlbl{lkp:e}

/*
 * Add an element to the hash table.  Caller must have acquired the
 * update-side lock via hashtab_lock_mod().
 */
void hashtab_add(struct ht_elem *htep,			//\lnlbl{add:b}
                 struct ht_lock_state *lsp)
{
	struct ht_bucket *htbp = lsp->hbp[0];		//\lnlbl{add:htbp}
	int i = lsp->hls_idx[0];			//\lnlbl{add:i}

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

#ifndef FCV_SNIPPET
	htep->hte_hash = lsp->hls_hash[0];		//\lnlbl{add:hash}
#endif /* #ifndef FCV_SNIPPET */
	htep->hte_next[!i].prev = NULL;			//\lnlbl{add:initp}
	cds_list_add_rcu(&htep->hte_next[i], &htbp->htb_head); //\lnlbl{add:add}
}							//\lnlbl{add:e}

/*
 * Remove the specified element from the hash table.  Caller must have
 * acquired the update-side lock via hashtab_lock_mod().
 */
void hashtab_del(struct ht_elem *htep,			//\lnlbl{del:b}
                 struct ht_lock_state *lsp)
{
	int i = lsp->hls_idx[0];			//\lnlbl{del:i}

	if (htep->hte_next[i].prev) {			//\lnlbl{del:if}
		cds_list_del_rcu(&htep->hte_next[i]);	//\lnlbl{del:del}
		htep->hte_next[i].prev = NULL;		//\lnlbl{del:init}

		/*if (!repeatedly_resize) {*/
		if (1) {
			if (atomic_dec_return(&lsp->hbp[0]->nnodes) < 0)
				printf("Error in atomic_dec_return(&htbp->nnodes)\n");
		}
	}
	if (lsp->hbp[1] && htep->hte_next[!i].prev) {	//\lnlbl{del:ifnew}
		cds_list_del_rcu(&htep->hte_next[!i]);	//\lnlbl{del:delnew}
		htep->hte_next[!i].prev = NULL;		//\lnlbl{del:initnew}

		/*if (!repeatedly_resize) {*/
		if (1) {
			if (atomic_dec_return(&lsp->hbp[1]->nnodes) < 0)
				printf("Error in atomic_dec_return(&htbp->nnodes)\n");
		}
	}
}							//\lnlbl{del:e}
//\end{snippet}

/* Resize a hash table. */
int hashtab_resize(struct hashtab *htp_master,
                   unsigned long nbuckets,
                   int (*cmp)(struct ht_elem *htep, void *key, uint32_t seed),
                   unsigned long (*gethash)(void *key, uint32_t seed),
                   void *(*getkey)(struct ht_elem *htep))
{
	struct ht *htp;
	struct ht *htp_new;
	int i;
	int idx;
	struct ht_elem *htep;
	struct ht_bucket *htbp;
	struct ht_bucket *htbp_new;
	long long starttime;
	long b;

	if (!spin_trylock(&htp_master->ht_lock))	//\lnlbl{trylock}
		return -EBUSY;				//\lnlbl{ret_busy}
	htp = htp_master->ht_cur;			//\lnlbl{get_curtbl}
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
	if (htp_new == NULL) {				//\lnlbl{chk_nomem}
		spin_unlock(&htp_master->ht_lock);	//\lnlbl{rel_nomem}
		return -ENOMEM;				//\lnlbl{ret_nomem}
	}
	idx = htp->ht_idx;				//\lnlbl{get_curidx}
	htp_new->ht_idx = !idx;				//\lnlbl{put_curidx}
	rcu_assign_pointer(htp->ht_new, htp_new);	//\lnlbl{set_newtbl}
	synchronize_rcu();				//\lnlbl{sync_rcu}

	starttime = get_microseconds();
	for (i = 0; i < htp->ht_nbuckets; i++) {	//\lnlbl{loop:b}
		htbp = &htp->ht_bkt[i];			//\lnlbl{get_oldcur}
		spin_lock(&htbp->htb_lock);		//\lnlbl{acq_oldcur}
		cds_list_for_each_entry(htep, &htbp->htb_head, hte_next[idx]) { //\lnlbl{loop_list:b}
			htbp_new = ht_get_bucket(htp_new, htp_new->ht_getkey(htep), &b, NULL);
			spin_lock(&htbp_new->htb_lock);
			cds_list_add_rcu(&htep->hte_next[!idx], &htbp_new->htb_head);
			spin_unlock(&htbp_new->htb_lock);

			/*if (!repeatedly_resize) {*/
			if (1) {
				if (atomic_inc_return(&htbp_new->nnodes) > max_list_length) {
					if (!atomic_read(&enlarge_requests))
						atomic_inc(&enlarge_requests);
				}
			}
		}					//\lnlbl{loop_list:e}
		WRITE_ONCE(htp->ht_resize_cur, i);	//\lnlbl{update_resize}
		spin_unlock(&htbp->htb_lock);		//\lnlbl{rel_oldcur}
	}						//\lnlbl{loop:e}
	starttime = get_microseconds() - starttime;
        printf("\nRebuild done. ID: %d -> %d. Size: %lu -> %lu. Time %lld (microseconds)\n",
                        htp->ht_idx, htp_new->ht_idx, htp->ht_nbuckets, nbuckets, starttime);

	rcu_assign_pointer(htp_master->ht_cur, htp_new); //\lnlbl{rcu_assign}
	synchronize_rcu();				//\lnlbl{sync_rcu_2}
	spin_unlock(&htp_master->ht_lock);		//\lnlbl{rel_master}
	free(htp);					//\lnlbl{free}
	return 0;					//\lnlbl{ret_success}
}

void init() {}
void deinit() {}

#define INIT() init()
#define DEINIT() deinit()
#define hash_register_thread() rcu_register_thread()
#define hash_unregister_thread() rcu_unregister_thread()
#define hashtab_lock_lookup(htp, i) hashtab_lock_lookup((htp), (void *)(i))
#define hashtab_unlock_lookup(htp, i) hashtab_unlock_lookup((htp), (void *)(i))
#define hashtab_lock_mod(htp, i, h) hashtab_lock_mod((htp), (void *)(i), h)
#define hashtab_lock_mod_zoo(htp, k, i, h) hashtab_lock_mod((htp), k, h)
#define hashtab_unlock_mod(htp, i, h) hashtab_unlock_mod(h)
#define hashtab_lookup(tid, htp, h, k) hashtab_lookup((htp), (k))
#define hashtab_add(htp, h, htep, s, tid) hashtab_add((htep), (s))
#define hashtab_del(tid, htep,s) hashtab_del((htep), (s))
#define hash_resize_test(htp, n) hashtab_resize((htp), (n), NULL, NULL, NULL)

#include "hashtorture-extended.h"
