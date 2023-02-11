/* 
 * File:   debugcounter.h
 * Author: trbot
 *
 * Created on September 27, 2015, 4:43 PM
 */

#ifndef DEBUGCOUNTER_H
#define	DEBUGCOUNTER_H

#include <string.h>
#include <stdio.h>
#include <malloc.h>
#include "plaf.h"

struct debugCounter {
    int NUM_PROCESSES;
    volatile long long * data;
};

void add(struct debugCounter *de, const int tid, const long long val) {
    de->data[tid*PREFETCH_SIZE_WORDS] += val;
}

void inc(struct debugCounter *de, const int tid) {
    add(de,tid, 1);
}

long long get(struct debugCounter *de, const int tid) {
    return de->data[tid*PREFETCH_SIZE_WORDS];
}

long long getTotal(struct debugCounter *de) {
        long long result = 0;
        for (int tid=0;tid<de->NUM_PROCESSES;++tid) {
            result += get(de, tid);
        }
        return result;
}

void clear(struct debugCounter *de) {
    for (int tid=0;tid<de->NUM_PROCESSES;++tid) {
       de->data[tid*PREFETCH_SIZE_WORDS] = 0;
    }
}

void debugCounter_init(struct debugCounter *de, const int numProcesses) {
    de->NUM_PROCESSES= numProcesses;
    de->data = (long long*)malloc(numProcesses*PREFETCH_SIZE_WORDS);
    clear(de);
}

void debugCounter_finish(struct debugCounter *de) {
    free(de);
}

#endif	/* DEBUGCOUNTER_H */
