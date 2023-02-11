/*
 * hashtorture.h: simple user-level performance/stress test of hash tables.
 *
 * Usage:
 *
 *	./hash_xxx --perftest / --percenttest
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
 * Copyright (c) 2013 Paul E. McKenney, IBM Corporation.
 */

#define _GNU_SOURCE
#include <unistd.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sched.h>
#include <string.h>
#include <libgen.h>
#include "include/primes.h"
#include "lookup3.h"

#ifndef rcu_barrier
#  error You need a modern version of liburcu which has "rcu_barrier()".
#  define rcu_barrier() do ; while (0)
#endif /* #ifndef rcu_barrier */

/*
 * Test variables.
 */

/* Global parameters for test. */
int nbuckets = 1024;
int max_nbuckets = (1024*64);
int nreaders = 1;
int nupdaters = 1;
int nworkers = 1;
int updatewait = -1;
long elperupdater = 2048;
long elperworker = 2048;
long el_preinsert = 1024;
int cpustride = 1;
int resizediv = 0;
int resizemult = 0;
long resizewait = 1;
long nresizes = 0;
long duration = 1000; /* in milliseconds. */
int jhash = 0;
int rebuild = 0;
int enable_collision = 0;
char collision_file[128];
FILE *fp;
int repeatedly_resize = 1;
int max_list_length = 64;
int min_avg_load_factor = 4;
int measure_latency = 0;
int latency_array_size = 1024;
atomic_t enlarge_requests;
atomic_t shrink_requests;
char *program_name;
int rebuild_multi_thread = 0;
int rebuild_threads = 1;
#define MAX_REBUILD_THREADS (32)
thread_id_t rebuild_tids[MAX_REBUILD_THREADS];

atomic_t nthreads_running;   

#define GOFLAG_INIT 0
#define GOFLAG_RUN  1
#define GOFLAG_STOP 2
int goflag __attribute__((__aligned__(CACHE_LINE_SIZE))) = GOFLAG_INIT;

struct hashtab *test_htp = NULL;

struct testhe {
	struct ht_elem the_e;
	unsigned long data;
	int in_table __attribute__((__aligned__(CACHE_LINE_SIZE)));
}__attribute__((__aligned__(CACHE_LINE_SIZE)));

void defer_del_rcu(struct rcu_head *htep)
{
	struct testhe *p = container_of((struct ht_elem *)htep,
			                struct testhe, the_e);
	p->in_table = 0;
}

void (*defer_del)(struct ht_elem *htep) = NULL;

void defer_del_pcttest(struct ht_elem *htep)
{
#if defined(DHASHCAS2)
	struct ht_elem *next;
	next = (struct ht_elem *)((uintptr_t)rcu_dereference(htep->next) >> 1);
	if ((get_flag(next) & IS_BEING_DISTRIBUTED ) != 0)
		return;
	else
		call_rcu(&htep->rh, defer_del_rcu);
#elif defined(DHASH)
	struct ht_elem *next;
	next = rcu_dereference(htep->next);
	if ((get_flag(next) & IS_BEING_DISTRIBUTED ) != 0)
		return;
	else
		call_rcu(&htep->rh, defer_del_rcu);
#else
	call_rcu(&htep->rh, defer_del_rcu);
#endif
}

void defer_del_perftest(struct ht_elem *htep)
{
	call_rcu(&htep->rh, defer_del_rcu);
}

unsigned long jgh(void *data, uint32_t seed)
{
	char *buf = (char *)&data;
	int len = sizeof(unsigned long)/sizeof(char);
	uint32_t result;

	result = hashlittle(buf, len, seed);
	return (unsigned long)result;
}

int jcmp(struct ht_elem *htep, void *key, uint32_t seed)
{
	struct testhe *thep;

	thep = container_of(htep, struct testhe, the_e);
	return ((unsigned long)key) == thep->data;
}

void *jgk(struct ht_elem *htep)
{
	struct testhe *thep;

	thep = container_of(htep, struct testhe, the_e);
	return (void *)thep->data;
}

unsigned long tgh(void *key, uint32_t seed)
{
	return (unsigned long)key;
}

int testcmp(struct ht_elem *htep, void *key, uint32_t seed)
{
	struct testhe *thep;

	thep = container_of(htep, struct testhe, the_e);
	return ((unsigned long)key) == thep->data;
}

void *testgk(struct ht_elem *htep)
{
	struct testhe *thep;

	thep = container_of(htep, struct testhe, the_e);
	return (void *)thep->data;
}

/* Repeatedly resize a hash table. */
void *test_resize(void *arg)
{
	long els[2];
	long i = 0;
	long long t1, t2;
	long new_size;

	rcu_register_thread();
	run_on(3);
	if (repeatedly_resize) {
		els[0]= nbuckets;
		els[1] = els[0] * resizemult / resizediv;
		if (els[1] > max_nbuckets) {
			printf("ERROR: The size (%ld) of the new bucket array is larger than %d\n",
					els[1], max_nbuckets);
			BUG_ON(1);
		}
	} else {
		els[0] = els[1] = nbuckets;
	}
	printf("Resize thread (%ld <-> %ld)\n", els[0], els[1]);
	while (READ_ONCE(goflag) == GOFLAG_INIT)
		poll(NULL, 0, 1);
	while (READ_ONCE(goflag) == GOFLAG_RUN) {
		smp_mb();
		if (resizewait != 0) {
			t1 = get_microseconds();
			smp_mb();
			while (1) {
				t2 = get_microseconds();
				if (((t2 - t1) / 1000) > resizewait)
					break;
			}
			if (READ_ONCE(goflag) != GOFLAG_RUN)
				break;
		}
		if (repeatedly_resize) {
			i++;
			hash_resize_test(test_htp, els[i & 0x1]);
		} else {
			if (atomic_read(&enlarge_requests) == 0 &&
			     (atomic_read(&shrink_requests) == 0)) {
				poll(NULL, 0, 1);
				continue;
			} 
			new_size = els[i & 0x1];
			/* FIXME: Race conditions may happen in between the two reads of
			 * 	  enlarge_requests. Current version works because other
			 * 	  concurrent threads can only monotonically increment it. */
			if (atomic_read(&enlarge_requests)) {
				new_size = els[i & 0x1] * resizemult;
				if (new_size > max_nbuckets) {
					printf("ERROR: The size (%ld) of the new bucket array is larger than %d. Abort!\n",
							new_size, max_nbuckets);
					WRITE_ONCE(goflag, GOFLAG_STOP);
					break;
				}
				long total_nodes = 0;
				for (int n = 0; n < els[i & 0x1]; n++) {
					total_nodes += atomic_read(&test_htp->ht_cur->ht_bkt[n].nnodes);
				}
				//printf("We are going to resize the hash table from %ld to %ld. Total nodes: %ld\n",
						//els[i & 0x1], new_size, total_nodes);

				hash_resize_test(test_htp, new_size);
				smp_mb();
				atomic_set(&enlarge_requests, 0);
			}
			if (atomic_read(&shrink_requests)) {
				new_size = els[i & 0x1] / resizediv;
				//printf("We are going to resize the hash table from %ld to %ld.\n",
						//els[i & 0x1], new_size);
				hash_resize_test(test_htp, new_size);
				smp_mb();
				atomic_set(&shrink_requests, 0);
			}
			i++;
			els[i & 0x1] = new_size;
		}
	}
	nresizes = i;
	rcu_unregister_thread();
	return NULL;
}

/* Look up a key in the hash table. */
int test_lookup(int tid, long i)
{
	struct ht_elem *htep;
	struct testhe *thep;

	hashtab_lock_lookup(test_htp, i);
	htep = hashtab_lookup(tid, test_htp, i, (void *)i);
	thep = container_of(htep, struct testhe, the_e);
	BUG_ON(thep && thep->data != i);
	hashtab_unlock_lookup(test_htp, i);
	hashtab_lookup_done(htep);
	return !!htep;
}

/* Add an element to the hash table. */
void test_add(int tid, struct testhe *thep)  // add the thread_id tid
{
	struct ht_lock_state __attribute__((__unused__)) hlms;

	BUG_ON(thep->in_table);
	hashtab_lock_mod(test_htp, thep->data, &hlms);
	BUG_ON(hashtab_lookup(tid, test_htp, thep->data, (void *)thep->data));
	thep->in_table = 1;
	hashtab_add(test_htp, thep->data, &thep->the_e, &hlms, tid);
	hashtab_unlock_mod(test_htp, thep->data, &hlms);
}

/* Remove an element from the hash table. */
void test_del(int tid, struct testhe *thep)
{
	struct ht_lock_state __attribute__((__unused__)) hlms;

	BUG_ON(thep->in_table != 1);
	hashtab_lock_mod(test_htp, thep->data, &hlms);
#ifndef API_NO_LOCK_STATE
	hashtab_del(tid, &thep->the_e, &hlms);
#else
	hashtab_del(tid, test_htp, &thep->the_e);
#endif
	thep->in_table = 2;
	hashtab_unlock_mod(test_htp, thep->data, &hlms);
	defer_del(&thep->the_e);
}

/*
 * Performance Test (--perftest)
 */

/* Per-test-thread attribute/statistics structure. */
struct perftest_attr {
	int myid;
	long long nlookups;
	long long nlookupfails;
	long long nadds;
	long long ndels;
	int mycpu;
	long nelements;
}__attribute__((__aligned__(CACHE_LINE_SIZE)));

/* Performance test reader thread. */
void *perftest_reader(void *arg)
{
	int gf;
	long i;
	struct perftest_attr *pap = arg;
	long mydelta = primes[pap->myid]; /* Force different reader paths. */
	long ne = pap->nelements;
	int offset = (ne / mydelta) * mydelta == ne;
	long long nlookups = 0;
	long long nlookupfails = 0;
	int myid = pap->myid;

	run_on(pap->mycpu);
	rcu_register_thread();

	/* Warm up cache. */
	for (i = 0; i < ne; i++)
		test_lookup(myid, i+1);

	/* Record our presence. */
	atomic_inc(&nthreads_running);

	/* Run the test code. */
	i = 0;
	for (;;) {
		gf = READ_ONCE(goflag);
		if (gf != GOFLAG_RUN) {
			if (gf == GOFLAG_STOP)
				break;
			if (gf == GOFLAG_INIT) {
				/* Still initializing, kill statistics. */
				nlookups = 0;
				nlookupfails = 0;
			}
		}
		if (!test_lookup(myid, i+1))
			nlookupfails++;
		nlookups++;
		i += mydelta;
		if (i >= ne)
			i = i % ne + offset;
	}

	pap->nlookups = nlookups;
	pap->nlookupfails = nlookupfails;
	rcu_unregister_thread();
	return NULL;
}

/* Performance test updater thread. */
void *perftest_updater(void *arg)
{
	long i;
	long j;
	int gf;
	struct perftest_attr *pap = arg;
	int myid = pap->myid;
	int mylowkey = myid * elperupdater;
	struct testhe *thep;
	long long nadds = 0;
	long long ndels = 0;

	thep = malloc(sizeof(*thep) * elperupdater);
	BUG_ON(thep == NULL);
	for (i = 0; i < elperupdater; i++) {
		//thep[i].data = i + mylowkey;
		thep[i].data = i + mylowkey + 1;
		if (thep[i].data >= ~0UL) {
			printf("Error in initializing thep[]\n");
			exit(0);
		}
		thep[i].in_table = 0;
	}
	run_on(pap->mycpu);
	rcu_register_thread();

	/* Start with a group of elements in the hash table. */
	for (i = 0; i < (el_preinsert/nupdaters); i++) {
		j = random() % elperupdater;
		while (thep[j].in_table)
			if (++j >= elperupdater)
				j = 0;
		test_add(myid, &thep[j]);
		BUG_ON(!test_lookup(myid, thep[j].data));
	}

	/* Announce our presence and enter the test loop. */
	atomic_inc(&nthreads_running);
	i = 0;
	for (;;) {
		gf = READ_ONCE(goflag);
		if (gf != GOFLAG_RUN) {
			if (gf == GOFLAG_STOP)
				break;
			if (gf == GOFLAG_INIT) {
				/* Still initializing, kill statistics. */
				nadds = 0;
				ndels = 0;
			}
		}
		if (updatewait == 0) {
			poll(NULL, 0, 10);  /* No actual updating wanted. */
		} else if (thep[i].in_table == 1) {
			test_del(myid, &thep[i]);
			if (test_lookup(myid, thep[i].data))
				BUG_ON(1);
			ndels++;
		} else if (thep[i].in_table == 0) {
			memset(&thep[i].the_e, 0, sizeof(thep[i].the_e));
			test_add(myid, &thep[i]);
			if (!test_lookup(myid, thep[i].data))
				BUG_ON(1);
			nadds++;
		}

		/* Add requested delay. */
		if (updatewait < 0) {
			poll(NULL, 0, -updatewait);
		} else {
			for (j = 0; j < updatewait; j++)
				barrier();
		}
		if (++i >= elperupdater)
			i = 0;
		if ((i & 0xf) == 0)
			rcu_quiescent_state();
	}

	rcu_barrier();

#ifdef DHASH
	/* In DHash, the rebuild thread explicitly distributes
	 * nodes to the new hash table. So the worker threads must
	 * wait the existing rebuild thread to complete before cleaning
	 * up.  */
	struct ht *__htp;
	struct ht *__htp_new;
	do {
		__htp = rcu_dereference(test_htp->ht_cur);
		__htp_new = rcu_dereference(__htp->ht_new);
	} while (__htp_new != NULL);
#endif

	/* Test over, so remove all our elements from the hash table. */
	for (i = 0; i < elperupdater; i++) {
		if (thep[i].in_table != 1)
			continue;
		if (!test_lookup(myid, thep[i].data)) {
			BUG_ON(1);
		}
		test_del(myid, &thep[i]);
	}
	rcu_barrier();

	rcu_unregister_thread();
	free(thep);
	pap->nadds = nadds;
	pap->ndels = ndels;
	return NULL;
}

/* Run a performance test. */
void perftest(void)
{
	struct perftest_attr *pap;
	int maxcpus = sysconf(_SC_NPROCESSORS_CONF);
	long i;
	long long nlookups = 0;
	long long nlookupfails = 0;
	long long nadds = 0;
	long long ndels = 0;
	long long starttime;
	long real_resize_time = 0;

	BUG_ON(maxcpus <= 0);
	test_htp = jhash?
		hashtab_alloc(nbuckets, jcmp, jgh, jgk, 0):
		hashtab_alloc(nbuckets, testcmp, tgh, testgk, 0);
	BUG_ON(test_htp == NULL);
	pap = malloc(sizeof(*pap) * (nreaders + nupdaters));
	BUG_ON(pap == NULL);
	defer_del = defer_del_perftest;
	atomic_set(&nthreads_running, 0);
	goflag = GOFLAG_INIT;

	for (i = 0; i < nreaders + nupdaters; i++) {
		pap[i].myid = i < nreaders ? i : (i - nreaders);
		pap[i].nlookups = 0;
		pap[i].nlookupfails = 0;
		pap[i].nadds = 0;
		pap[i].ndels = 0;
		pap[i].mycpu = (i * cpustride + 5) % maxcpus;
		pap[i].nelements = nupdaters * elperupdater;
		create_thread(i < nreaders ? perftest_reader : perftest_updater,
			      &pap[i]);
	}

	/* Wait for all threads to initialize. */
	while (atomic_read(&nthreads_running) < nreaders + nupdaters)
		poll(NULL, 0, 1);
	smp_mb();

	/* Run the test. */
	starttime = get_microseconds();
	WRITE_ONCE(goflag, GOFLAG_RUN);
	poll(NULL, 0, duration);

	long total_nodes = 0;
	for (int n = 0; n < test_htp->ht_cur->ht_nbuckets; n++) {
		total_nodes += atomic_read(&test_htp->ht_cur->ht_bkt[n].nnodes);
	}
	printf("Total nodes: %ld\n", total_nodes);

	WRITE_ONCE(goflag, GOFLAG_STOP);
	starttime = get_microseconds() - starttime;
	wait_all_threads();

	/* Collect stats and output them. */
	for (i = 0; i < nreaders + nupdaters; i++) {
		nlookups += pap[i].nlookups;
		nlookupfails += pap[i].nlookupfails;
		nadds += pap[i].nadds;
		ndels += pap[i].ndels;
	}
	printf("nlookups: %lld %lld  nadds: %lld  ndels: %lld  duration: %g\n",
	       nlookups, nlookupfails, nadds, ndels, starttime / 1000.);
	printf("ns/read: %g  ns/update: %g\n",
	       (starttime * 1000. * (double)nreaders) / (double)nlookups,
	       ((starttime * 1000. * (double)nupdaters) /
	        (double)(nadds + ndels)));
	printf("microsec/op %g\n",
		starttime * (nreaders + nupdaters) / (double)(nlookups + nadds + ndels));
	printf("Mop/s %g\n",
		(double)(nlookups + nadds + ndels)/ starttime);
	if (resizediv != 0 && resizemult != 0) {
		printf("Resizes: %ld (%lld ms in total)\n",
			nresizes, starttime/1000);
		real_resize_time = (resizewait > 0) ? resizewait : 0;
		real_resize_time *= nresizes;
		real_resize_time = starttime/1000 - real_resize_time;
		printf("Resize efficiency: %ld ms/op\n",
			real_resize_time/(nresizes==0?1:nresizes));
	}

	free(pap);
	hashtab_free(test_htp);
}

/*
 * Percentage Test (--pcttest)
 */

static int pctInsert = 5;
static int pctDelete = 5;
static int pctLookup = 90;

/* Per-test thread attribute/statistics structure. */
struct pcttest_attr {
	int myid;
	long long nlookups;
	long long nlookupfails;
	long long nadds;
	long long ndels;
	int mycpu;
	long nelements;
}__attribute__((__aligned__(CACHE_LINE_SIZE)));

void *pcttest_worker(void *arg)
{
	long i;
	long j;
	int gf;
	struct pcttest_attr *pap = arg;
	int myid = pap->myid;
	long mydelta = primes[myid]; /* Force different reader paths. */
	long ne = pap->nelements;
	int offset = (ne / mydelta) * mydelta == ne;
	long long nlookups = 0;
	long long nlookupfails = 0;

	int mylowkey = myid * elperworker;
	struct testhe *thep;
	long long nadds = 0;
	long long ndels = 0;

	long long *lookup_latency_array = NULL;
	long long *update_latency_array = NULL;
	int lla_n, ula_n;
	int measure_latency_lookup = 0;
	int measure_latency_update = 0;
	long long tsc;
	FILE *latency_output = NULL;

	thep = malloc(sizeof(*thep) * elperworker);
	BUG_ON(thep == NULL);
	for (i = 0; i < elperworker; i++) {
		thep[i].data = i + mylowkey + 1;
		if ((thep[i].data == ~0UL) || (thep[i].data == 0UL)) {
			printf("Error in initializing thep[]\n");
			exit(0);
		}
		thep[i].in_table = 0;
	}
	run_on(pap->mycpu);
	rcu_register_thread();

	if (measure_latency) {
		lookup_latency_array = calloc(latency_array_size, sizeof(long long));
		update_latency_array = calloc(latency_array_size, sizeof(long long));
		lla_n = 0;
		ula_n = 0;
		measure_latency_lookup = 1;
		measure_latency_update = 1;
	}

	/* Start with some random half of the elements in the hash table. */
	for (i = 0; i < (el_preinsert/nworkers); i++) {
		j = random() % elperworker;
		while (thep[j].in_table)
			if (++j >= elperworker)
				j = 0;
		test_add(myid, &thep[j]);
		BUG_ON(!test_lookup(myid, thep[j].data));
	}

	/* Announce our presence and enter the test loop. */
	atomic_inc(&nthreads_running);
	i = 0; /* Flag for Lookup */
	j = 0; /* Flag for Insert and Delete */
	long op_type = 0;
	for (;;) {
		gf = READ_ONCE(goflag);
		if (gf != GOFLAG_RUN) {
			if (gf == GOFLAG_STOP)
				break;
			if (gf == GOFLAG_INIT) {
				nadds = 0;
				ndels = 0;
				nlookups = 0;
				nlookupfails = 0;
			}
		}
		tsc = 0;
		//op_type = random() % (pctInsert + pctLookup);
		if (op_type < pctLookup) {
			while ((i == 0UL) || (i == ~0UL)) {
				i ++;
			}
			if (measure_latency_lookup) {
				struct ht *htp_tmp = rcu_dereference(test_htp->ht_cur);
				if ((htp_tmp->ht_gethash((void *)i, htp_tmp->hash_seed) % htp_tmp->ht_nbuckets) == 0) {
					tsc = get_timestamp();
				}
			}

			if (!test_lookup(myid, i))
				nlookupfails ++;

			if (measure_latency_lookup && tsc) {
				tsc = get_timestamp() - tsc;
				lookup_latency_array[lla_n++] = tsc;
				if (lla_n >= latency_array_size) {
					printf("WARNING: latency_array_size becomes full (lookup).\n");
					measure_latency_lookup = 0;
				}
			}

			nlookups ++;
			i += mydelta;
			if (i >= ne)
				i = i % ne + offset;
		} else {
			while (thep[j].in_table != 0) {
				j = (++j >= elperworker)? 0 : j;
			}
			memset(&thep[j].the_e, 0, sizeof(thep[j].the_e));

			if (measure_latency_update) {
				struct ht *htp_tmp = rcu_dereference(test_htp->ht_cur);
				if ((htp_tmp->ht_gethash((void *)thep[j].data, htp_tmp->hash_seed) % htp_tmp->ht_nbuckets) == 0) {
					tsc = get_timestamp();
				}
			}

			test_add(myid, &thep[j]);
			BUG_ON(!test_lookup(myid, thep[j].data));
			nadds ++;

			if (measure_latency_update && tsc) {
				tsc = get_timestamp() - tsc;
				update_latency_array[ula_n++] = tsc;
				if (ula_n >= latency_array_size) {
					printf("ERROR: latency_array_size becomes full (insert).\n");
					measure_latency_update = 0;
				}
			}

			/* In the pctest model, we want to maintain a constant
			 * average load factor, such that the hash table needs
			 * to delete one node each time a new node has been
			 * inserted into the hash table.
			 * To evaluate the hash table in large scale, we typically
			 * choose a large pool of hash keys (>> 1M). Hence, if
			 * the keys are randomly generated, most delete operatoins
			 * fail, incurring unreasonable overhead due to unnecessary
			 * list traversals.
			 * So we delete the key just inserted into the hash table.
			 */
			if (measure_latency_update && tsc) {
				tsc = get_timestamp();
			}

			test_del(myid, &thep[j]);
			BUG_ON(test_lookup(myid, thep[j].data));
			ndels ++;

			if (measure_latency_update && tsc) {
				tsc = get_timestamp() - tsc;
				update_latency_array[ula_n++] = tsc;
				if (ula_n >= latency_array_size) {
					printf("ERROR: latency_array_size becomes full (delete).\n");
					measure_latency_update = 0;
				}
			}

			if (++j >= elperworker)
				j = 0;
			if ((j & 0xf) == 0)
				rcu_quiescent_state();
		}
		op_type = ((++op_type) >= (pctLookup + pctInsert))? 0 : op_type;
	}

	rcu_barrier();

#ifdef DHASH
	/* In DHash, the rebuild(resize) thread explicitly distributes
	 * nodes to the new hash table. So the worker threads must
	 * wait the existing rebuild thread to complete before cleaning
	 * up.  */
	struct ht *__htp;
	struct ht *__htp_new;
	do {
		__htp = rcu_dereference(test_htp->ht_cur);
		__htp_new = rcu_dereference(__htp->ht_new);
	} while (__htp_new != NULL);
#endif

	/* Test over, so remove all our elements from the hash table. */
	for (i = 0; i < elperworker; i++) {
		if (thep[i].in_table != 1)
			continue;
		if (!test_lookup(myid, thep[i].data)) {
			BUG_ON(1);
		}
		test_del(myid, &thep[i]);
	}
	rcu_barrier();

	if (measure_latency) {
		char output_file[158] = "latency_output_lookup_";
		char thread_id[64];
		strncat(output_file, basename(program_name), 64);
		strcat(output_file, "_");
		sprintf(thread_id, "%d", pap->myid);
		strncat(output_file, thread_id, 64);
		latency_output = fopen(output_file, "w");
		if (latency_output == NULL) {
			printf("ERROR in creating latency output file %s\n", output_file);
		}
	}

	for (int i = 0; i < lla_n; i++) {
		fprintf(latency_output, "%lld\n", lookup_latency_array[i]);
		//printf("%lld\n", lookup_latency_array[i]);
	}

	if (latency_output)
		fclose(latency_output);

	if (measure_latency) {
		char output_file[158] = "latency_output_update_";
		char thread_id[64];
		strncat(output_file, basename(program_name), 64);
		strcat(output_file, "_");
		sprintf(thread_id, "%d", pap->myid);
		strncat(output_file, thread_id, 64);
		latency_output = fopen(output_file, "w");
		if (latency_output == NULL) {
			printf("ERROR in creating latency output file %s\n", output_file);
		}
	}

	for (int i = 0; i < ula_n; i++) {
		fprintf(latency_output, "%lld\n", update_latency_array[i]);
		//printf("%lld\n", update_latency_array[i]);
	}

	if (latency_output)
		fclose(latency_output);

	rcu_unregister_thread();
	free(thep);
	pap->nlookups = nlookups;
	pap->nlookupfails = nlookupfails;
	pap->nadds = nadds;
	pap->ndels = ndels;
	return NULL;
}

/* Run a test in which the mix of different operations can be specified. */
void pcttest(void)
{
	struct pcttest_attr *pap;
	int maxcpus = sysconf(_SC_NPROCESSORS_CONF);
	long i;
	long long nlookups = 0;
	long long nlookupfails = 0;
	long long nadds = 0;
	long long ndels = 0;
	long long starttime;
	long real_resize_time = 0;

	BUG_ON(maxcpus <= 0);
	test_htp = jhash?
		hashtab_alloc(nbuckets, jcmp, jgh, jgk, 0):
		hashtab_alloc(nbuckets, testcmp, tgh, testgk, 0);
	BUG_ON(test_htp == NULL);
	pap = malloc(sizeof(*pap) * nworkers);
	BUG_ON(pap == NULL);
	defer_del = defer_del_pcttest;
	atomic_set(&nthreads_running, 0);
	goflag = GOFLAG_INIT;

	for (i = 0; i < nworkers; i++) {
		pap[i].myid = i;
		pap[i].nlookups = 0;
		pap[i].nlookupfails = 0;
		pap[i].nadds = 0;
		pap[i].ndels = 0;
		pap[i].mycpu = (i * cpustride + 5) % maxcpus;
		pap[i].nelements = nworkers * elperworker;
		create_thread(pcttest_worker, &pap[i]);
	}

	printf(" nworkers %d\n pctInsert %d\n pctDelete %d\n pctLookup %d\n",
			nworkers, pctInsert, pctDelete, pctLookup);

	/* Wait for all threads to initialize. */
	while (atomic_read(&nthreads_running) < nworkers)
		poll(NULL, 0, 1);
	smp_mb();

	/* Run the test. */
	starttime = get_microseconds();
	WRITE_ONCE(goflag, GOFLAG_RUN);
	poll(NULL, 0, duration);

	long total_nodes = 0;
	for (int n = 0; n < test_htp->ht_cur->ht_nbuckets; n++) {
		total_nodes += atomic_read(&test_htp->ht_cur->ht_bkt[n].nnodes);
	}
	printf("Total nodes: %ld\n", total_nodes);

	WRITE_ONCE(goflag, GOFLAG_STOP);
	starttime = get_microseconds() - starttime;
	wait_all_threads();

	/* Collect stats and output them. */
	for (i = 0; i < nworkers; i++) {
		nlookups += pap[i].nlookups;
		nlookupfails += pap[i].nlookupfails;
		nadds += pap[i].nadds;
		ndels += pap[i].ndels;
	}
	printf("nlookups: %lld %lld  nadds: %lld  ndels: %lld  duration: %g\n",
	       nlookups, nlookupfails, nadds, ndels, starttime / 1000.);
	printf("ns/read: %g  ns/update: %g\n",
	       (starttime * 1000. * (double)nworkers) / (double)nlookups,
	       ((starttime * 1000. * (double)nworkers) /
	        (double)(nadds + ndels)));
	printf("Mop/s %g\n",
		(double)(nlookups + nadds + ndels) / starttime);
	if (resizediv != 0 && resizemult != 0) {
		printf("Resizes: %ld (%lld ms in total)\n",
			nresizes, starttime/1000);
		real_resize_time = (resizewait > 0) ? resizewait : 0;
		real_resize_time *= nresizes;
		real_resize_time = starttime/1000 - real_resize_time;
		printf("Resize efficiency: %ld ms/op\n",
			real_resize_time/((nresizes==0)?1:nresizes));
	}

	free(pap);
	hashtab_free(test_htp);
}

void *collision_thread(void *arg)
{
	long data;
	struct testhe *thep;
	int seed;
	int maxcpus = sysconf(_SC_NPROCESSORS_CONF);
	BUG_ON(maxcpus <= 0); 

	rcu_register_thread();
	run_on(((nworkers+rebuild_threads)*cpustride)%maxcpus);

	printf("Collision thread starts\n");
	while (READ_ONCE(goflag) == GOFLAG_INIT)
		poll(NULL, 0, 1);
	while (READ_ONCE(goflag) == GOFLAG_RUN) {
		smp_mb();

		if (EOF != fscanf(fp, "%ld\t\t%d", &data, &seed)) {
			thep = calloc(1, sizeof(*thep));
			BUG_ON(!thep);
			thep->data = data;
			if ((thep->data == ~0UL) || (thep->data == 0UL)) {
				printf("Error in initializing thep in collision_thread\n");
				free(thep);
				continue;
			}
			thep->in_table = 0;

			test_add(nworkers+rebuild_threads, thep);  //
			BUG_ON(!test_lookup(nworkers+rebuild_threads, thep->data));
			//FIXME: free thep

		} else {
			printf("Reach the end of the collision log file.\n");
			break;
		}
		if (READ_ONCE(goflag) != GOFLAG_RUN)
			break;
		// Wait 1 millisecond
		poll(NULL, 0, 1);

	}
	
	rcu_unregister_thread();
	return NULL;
}


/* Common argument-parsing code. */

void usage(char *progname, const char *format, ...)
{
	va_list ap;

	va_start(ap, format);
	vfprintf(stderr, format, ap);
	va_end(ap);
	fprintf(stderr, "Usage: %s --perftest\n", progname);
	fprintf(stderr, "Usage: %s --pcttest\n", progname);
	fprintf(stderr, "\t--jhash\n");
	fprintf(stderr, "\t\tUse Bob Jenkins's hash function (lookup3).\n");
	fprintf(stderr, "\t--rebuild\n");
	fprintf(stderr, "\t\tChoose a new hash function (or seed) each time we change the size of the hash table.\n");
	fprintf(stderr, "\t--collision\n");
	fprintf(stderr, "\t\tLog file recording collision hash data\n");
	fprintf(stderr, "\t--nbuckets\n");
	fprintf(stderr, "\t\tNumber of buckets, defaults to 1024.\n");
	fprintf(stderr, "\t--nreaders\n");
	fprintf(stderr, "\t\tNumber of readers, defaults to 1 (for perftest only).\n");
	fprintf(stderr, "\t--nupdaters\n");
	fprintf(stderr, "\t\tNumber of updaters, defaults to 1.  Must be 1\n");
	fprintf(stderr, "\t\tor greater, or hash table will be empty (for perftest only).\n");
	fprintf(stderr, "\t--updatewait\n");
	fprintf(stderr, "\t\tNumber of spin-loop passes per update,\n");
	fprintf(stderr, "\t\tdefaults to -1.  If 0, the updater will not.\n");
	fprintf(stderr, "\t\tdo any updates, except for initialization.\n");
	fprintf(stderr, "\t\tIf negative, the updater waits for the\n");
	fprintf(stderr, "\t\tcorresponding number of milliseconds\n");
	fprintf(stderr, "\t\tbetween updates (for perftest only).\n");
	fprintf(stderr, "\t--nworkers\n");
	fprintf(stderr, "\t\tNumber of workers, defaults to 1.  Must be 1\n");
	fprintf(stderr, "\t\tor greater. Each worker performs a mix of different\n");
	fprintf(stderr, "\t\toperations (for pcttest only).\n");
	fprintf(stderr, "\t--percentage\n");
	fprintf(stderr, "\t\tPercentage values for Insert, Delete, and Lookup\n");
	fprintf(stderr, "\t\trespectively, default to 5 5 90 (for pcttest only).\n");
	fprintf(stderr, "\t--elems/writer\n");
	fprintf(stderr, "\t\tNumber of hash-table elements per writer,\n");
	fprintf(stderr, "\t\tdefaults to 2048.  Must be greater than zero.\n");
	fprintf(stderr, "\t--preinsert\n");
	fprintf(stderr, "\t\tNumber of hash-table elements to be inserted into the hash table,\n");
	fprintf(stderr, "\t\tdefaults to 1024.  Must be greater than zero.\n");
	fprintf(stderr, "\t--cpustride\n");
	fprintf(stderr, "\t\tStride when spreading threads across CPUs,\n");
	fprintf(stderr, "\t\tdefaults to 1.\n");
	fprintf(stderr, "\t--resizediv\n");
	fprintf(stderr, "\t\tDivisor for resized hash table,\n");
	fprintf(stderr, "\t\tdefaults to zero (don't resize).\n");
	fprintf(stderr, "\t--resizemult\n");
	fprintf(stderr, "\t\tMultiplier for resized hash table,\n");
	fprintf(stderr, "\t\tdefaults to zero (don't resize).\n");
	fprintf(stderr, "\t--resizewait\n");
	fprintf(stderr, "\t\tMilliseconds to wait between resizes,\n");
	fprintf(stderr, "\t\tdefaults to one.\n");
	fprintf(stderr, "\t--dont-repeatedly-resize\n");
	fprintf(stderr, "\t\tResize/rebuild operation is performed only when\n");
	fprintf(stderr, "\t\tthe length of any list exceeds the specified threshold,\n");
	fprintf(stderr, "\t\tor when the average load factor is lower than the specified threshold.\n");
	fprintf(stderr, "\t--max-list-length\n");
	fprintf(stderr, "\t\tPerform resize/rebuild operations,\n");
	fprintf(stderr, "\t\tif the length of any of the lists exceeds the specified limit.\n");
	fprintf(stderr, "\t\t(for dont-repeatedly-resize only)\n");
	fprintf(stderr, "\t--min-avg-load-factor\n");
	fprintf(stderr, "\t\tPerform resize/rebuild operations,\n");
	fprintf(stderr, "\t\tif the average load factor become lower than the specified threshold.\n");
	fprintf(stderr, "\t\t(for dont-repeatedly-resize only)\n");
	fprintf(stderr, "\t--max-nbuckets\n");
	fprintf(stderr, "\t\tMaximum number of buckets (must >= 1024).\n");
	fprintf(stderr, "\t--measure-latency\n");
	fprintf(stderr, "\t\tSize of the array to record latency (must >= 1024).\n");
	fprintf(stderr, "\t--duration\n");
	fprintf(stderr, "\t\tDuration of test, in milliseconds.\n");
	fprintf(stderr, "\t--rebuild-threads\n");
	fprintf(stderr, "\t\tNumber of rebuilding threads (must >=1 and <=32). Default 1.\n");
	exit(-1);
}

/*
 * Mainprogram.
 */
int main(int argc, char *argv[])
{
	int i = 1;
	void (*test_to_do)(void) = NULL;
	program_name = argv[0];

	smp_init();

	while (i < argc) {
		if (strcmp(argv[i], "--smoketest") == 0) {
			usage(argv[0],
			      "This option is obsolete.\n");
			exit(0);
		} else if (strcmp(argv[i], "--schroedinger") == 0) {
			usage(argv[0],
			      "This option is obselete.\n");
			exit(0);
		} else if (strcmp(argv[i], "--perftest") == 0) {
			test_to_do = perftest;
			if (i != 1)
				usage(argv[0],
				      "Must be first argument: %s\n", argv[i]);
		} else if (strcmp(argv[i], "--pcttest") == 0) {
			test_to_do = pcttest;
			if (i != 1)
				usage(argv[0],
				      "Must be first argument: %s\n", argv[i]);
		} else if (strcmp(argv[i], "--jhash") == 0) {
			jhash = 1;
			printf("=== jhash specified ===\n");
		} else if (strcmp(argv[i], "--rebuild") == 0) {
			rebuild = 1;
			if (!jhash)
				usage(argv[0],
				      "--jhash must be specified before specifying --rebuild\n");
			printf("=== Rebuild specified ===.\n");
		} else if (strcmp(argv[i], "--collision") == 0) {
			enable_collision= 1;
			strncpy(collision_file, argv[++i], 128);
			printf("=== Collision thread specified. Log file: %s ===\n", collision_file);
			fp = fopen(collision_file, "r");
			if (fp == NULL)
				usage(argv[0],
				      "--collision must specify log file.\n");
		} else if (strcmp(argv[i], "--nbuckets") == 0) {
			nbuckets = strtol(argv[++i], NULL, 0);
			if (nbuckets < 0)
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 1]);
		} else if (strcmp(argv[i], "--max-nbuckets") == 0) {
			max_nbuckets = strtol(argv[++i], NULL, 0);
			printf("=== max-nbuckets set: %d\n", max_nbuckets);
			if (max_nbuckets < 1024)
				usage(argv[0],
				      "%s must be >= 1024\n", argv[i - 1]);
		} else if (strcmp(argv[i], "--nreaders") == 0) {
			nreaders = strtol(argv[++i], NULL, 0);
			if (nreaders < 0)
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 1]);
		} else if (strcmp(argv[i], "--nupdaters") == 0) {
			nupdaters = strtol(argv[++i], NULL, 0);
			if (nupdaters < 1)
				usage(argv[0],
				      "%s must be >= 1\n", argv[i - 1]);
		} else if (strcmp(argv[i], "--nworkers") == 0) {
			nworkers = strtol(argv[++i], NULL, 0);
			if ((nworkers < 1) || (test_to_do != pcttest))
				usage(argv[0],
				      "%s must be >= 1 and work with pcttest\n", argv[i - 1]);
//			dcssp_init(prov, nworkers);
		} else if (strcmp(argv[i], "--percentage") == 0) {
			pctInsert = strtol(argv[++i], NULL, 0);
			if (pctInsert < 0)
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 1]);
			pctDelete = strtol(argv[++i], NULL, 0);
			if (pctDelete < 0)
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 2]);
			pctLookup = strtol(argv[++i], NULL, 0);
			if (pctLookup < 0)
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 3]);
			if (pctInsert + pctDelete + pctLookup != 100)
				usage(argv[0],
				      "Total amount of Insert, Delete, and Lookup must equal to 100.\n", argv[i - 3]);
		} else if (strcmp(argv[i], "--updatewait") == 0) {
			updatewait = strtol(argv[++i], NULL, 0);
		} else if (strcmp(argv[i], "--elems/writer") == 0) {
			elperupdater = strtol(argv[++i], NULL, 0);
			elperworker = elperupdater;
			if (elperupdater < 0)
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 1]);
		} else if (strcmp(argv[i], "--preinsert") == 0) {
			el_preinsert = strtol(argv[++i], NULL, 0);
			if (el_preinsert < 0) 
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 1]);
		} else if (strcmp(argv[i], "--cpustride") == 0) {
			cpustride = strtol(argv[++i], NULL, 0);
		} else if (strcmp(argv[i], "--resizediv") == 0) {
			resizediv = strtol(argv[++i], NULL, 0);
			if (resizediv < 0)
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 1]);
			if (resizediv != 0 && resizemult == 0)
				resizemult = 1;
		} else if (strcmp(argv[i], "--resizemult") == 0) {
			resizemult = strtol(argv[++i], NULL, 0);
			if (resizemult < 0)
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 1]);
			if (resizemult != 0 && resizediv == 0)
				resizediv = 1;
		} else if (strcmp(argv[i], "--resizewait") == 0) {
			resizewait = strtol(argv[++i], NULL, 0);
			if (resizewait < 0)
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 1]);
		} else if (strcmp(argv[i], "--duration") == 0) {
			duration = strtol(argv[++i], NULL, 0);
			if (duration < 0)
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 1]);
		} else if (strcmp(argv[i], "--dont-repeatedly-resize") == 0) {
			repeatedly_resize = 0;
			resizemult = (resizemult == 0)? 1 : resizemult;
			resizediv = (resizediv == 0)? 1 : resizediv;
			atomic_set(&enlarge_requests, 0);
			atomic_set(&shrink_requests, 0);
			printf("=== Don't repeatedly resize specified. Both resizemult and resizediv are set to 1.\n");
		} else if (strcmp(argv[i], "--max-list-length") == 0) {
			if (repeatedly_resize != 0)
				usage(argv[0],
				      "--dont-repeatedly-resize must be first specified.\n");
			max_list_length = strtol(argv[++i], NULL, 0);
			if (max_list_length < 0)
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 1]);
			printf("=== max-list-length specified. %d\n", max_list_length);
		} else if (strcmp(argv[i], "--min_avg_load_factor") == 0) {
			if (repeatedly_resize != 0)
				usage(argv[0],
				      "--dont-repeatedly-resize must be first specified.\n");
			min_avg_load_factor = strtol(argv[++i], NULL, 0);
			if (min_avg_load_factor < 0)
				usage(argv[0],
				      "%s must be >= 0\n", argv[i - 1]);
			printf("=== min_avg_load_factor specified. %d\n", min_avg_load_factor);
		} else if (strcmp(argv[i], "--measure-latency") == 0) {
			measure_latency = 1;
			latency_array_size = strtol(argv[++i], NULL, 0);
			if (latency_array_size < 0)
				usage(argv[0],
				      "%s must be >= 1024\n", argv[i - 1]);
			printf("=== Will measure latency of accesses to bucket[0]\n");
		} else if (strcmp(argv[i], "--rebuild-threads") == 0) {
			rebuild_multi_thread = 1;
			rebuild_threads = strtol(argv[++i], NULL, 0);
			if (rebuild_threads < 1 || rebuild_threads >=MAX_REBUILD_THREADS)
				usage(argv[0],
				      "%s must be >= 1 and <= 32\n", argv[i - 1]);
			printf("=== Create %d rebuilding threads.\n", rebuild_threads);
		} else {
			usage(argv[0], "Unrecognized argument: %s\n",
			      argv[i]);
		}
		i++;
	}
	if (!test_to_do)
		usage(argv[0], "No test specified\n");
	if (resizediv != 0 && resizemult != 0)
		create_thread(test_resize, NULL);
	if (enable_collision != 0)
		create_thread(collision_thread, NULL);
	INIT();
	test_to_do();
	DEINIT();
	return 0;
}
