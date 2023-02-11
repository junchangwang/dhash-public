/* 
 * File:   dcss_plus_impl.h
 * Author: Maya Arbel-Raviv
 *
 * Created on May 1, 2017, 10:52 AM
 * 
 * Changed to the version of C by Dunwei Liu on 2021.
 * Renamed to dcss.c
 * 
 */

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include "debugcounter.h"
#include "dcss.h"


#define BOOL_CAS __sync_bool_compare_and_swap
#define VAL_CAS __sync_val_compare_and_swap

#define DCSSP_TAGBIT 0x1 

static bool isDcssp(casword_t val) {
	return (val & DCSSP_TAGBIT);
}

int DESC_SNAPSHOT_2(dcsspdesc_t *descArray, dcsspdesc_t *descDest, dcssptagptr_t tagptr, int sz) {
#ifndef WIDTH_SEQ
    #define WIDTH_SEQ 48
#endif
#define OFFSET_SEQ 14 //old 14
#define MASK_SEQ ((uintptr_t)((1LL<<WIDTH_SEQ)-1)<<OFFSET_SEQ) /* cast to avoid signed bit shifting */
#define UNPACK_SEQ(tagptrOrMutables) (((uintptr_t)(tagptrOrMutables))>>OFFSET_SEQ)

#define TAGPTR_OFFSET_USER 0 // old 0
#define TAGPTR_OFFSET_TID 3 // old 3
#define TAGPTR_MASK_USER ((1<<TAGPTR_OFFSET_TID)-1) /* assumes TID is next field after USER */
#define TAGPTR_MASK_TID (((1<<OFFSET_SEQ)-1)&(~(TAGPTR_MASK_USER)))
#define TAGPTR_UNPACK_TID(tagptr) ((int) ((((tagptr_t) (tagptr))&TAGPTR_MASK_TID)>>TAGPTR_OFFSET_TID))
#define TAGPTR_UNPACK_PTR(descArray, tagptr) (&(descArray)[TAGPTR_UNPACK_TID((tagptr))])
    dcsspdesc_t *__src = TAGPTR_UNPACK_PTR((descArray), (tagptr));
    memcpy((descDest), __src, sz);
    return (UNPACK_SEQ(__src->mutables) == UNPACK_SEQ(tagptr));
}

 void MUTABLES_VAL_CAS_FIELD_2(bool* failedBit, casword_t *retval, volatile mutables_t* fldMutables, volatile mutables_t* snapMutables, casword_t oldval, casword_t *val,  casword_t mask, casword_t offset) 
{
#ifndef WIDTH_SEQ
    #define WIDTH_SEQ 48
#endif
#define OFFSET_SEQ 14 //old 14
#define MASK_SEQ ((uintptr_t)((1LL<<WIDTH_SEQ)-1)<<OFFSET_SEQ) /* cast to avoid signed bit shifting */
#define UNPACK_SEQ(tagptrOrMutables) (((uintptr_t)(tagptrOrMutables))>>OFFSET_SEQ)

#define TAGPTR_OFFSET_USER 0 // old 0
#define TAGPTR_OFFSET_TID 3 // old 3
#define TAGPTR_MASK_USER ((1<<TAGPTR_OFFSET_TID)-1) /* assumes TID is next field after USER */
#define TAGPTR_MASK_TID (((1<<OFFSET_SEQ)-1)&(~(TAGPTR_MASK_USER)))
#define TAGPTR_UNPACK_TID(tagptr) ((int) ((((tagptr_t) (tagptr))&TAGPTR_MASK_TID)>>TAGPTR_OFFSET_TID))
#define TAGPTR_UNPACK_PTR(descArray, tagptr) (&(descArray)[TAGPTR_UNPACK_TID((tagptr))])
    mutables_t __v = *(fldMutables); 
    while (1) { 
        if (UNPACK_SEQ(__v) != UNPACK_SEQ(*(snapMutables))) { 
            *(failedBit) = true; /* version number has changed, CAS cannot occur */ 
            break; 
        } 
        mutables_t __oldval = (__v & ~(mask)) | ((oldval)<<(offset)); 
        *retval = __sync_val_compare_and_swap((fldMutables), __oldval, (__v & ~(mask)) | ((*val)<<(offset))); 
        if (*(retval) == __oldval) { /* CAS SUCCESS */ 
           *(retval) = MUTABLES_UNPACK_FIELD(*(retval), (mask), (offset)); /* return contents of subfield */ 
            *(failedBit) = false; 
            break; 
        } else { /* CAS FAILURE: should we retry? */ 
            __v = *(retval); /* save the value that caused our CAS to fail, in case we need to retry */ 
            *(retval) = MUTABLES_UNPACK_FIELD(*(retval), (mask), (offset)); /* return contents of subfield */ 
            if (*(retval) != (oldval)) { /* check if we failed because the subfield's contents do not match oldval */ 
                *(failedBit) = false; 
                break; 
            } 
            /* subfield's contents DO match oldval, so we need to try again */ 
        } 
    } 
}

dcsspresult_t dcsspHelp(dcsspProvider_t *dcs, const int tid, dcssptagptr_t tagptr, dcsspptr_t snapshot, bool helpingOther) {
	// figure out what the state should be
	//struct dcsspresult result;
	casword_t state = DCSSP_STATE_FAILED;

	SOFTWARE_BARRIER;
	casword_t val1 = *(snapshot->addr1);
	SOFTWARE_BARRIER;

	if (val1 == snapshot->old1) {
		state = DCSSP_STATE_SUCCEEDED;
	}

	// try to cas the state to the appropriate value
	//dcsspptr_t ptr = TAGPTR_UNPACK_PTR(&(dcs->dcsspDescriptors), tagptr);
	dcsspptr_t ptr = TAGPTR_UNPACK_PTR(dcs->dcsspDescriptors, tagptr);
	casword_t retval;
	bool failedBit = false;
	MUTABLES_VAL_CAS_FIELD_2(&failedBit, &retval, &ptr->mutables, &snapshot->mutables, DCSSP_STATE_UNDECIDED, &state, DCSSP_MUTABLES_MASK_STATE, DCSSP_MUTABLES_OFFSET_STATE); 
	if (failedBit) {
		//result.status = DCSSP_IGNORED_RETVAL;
		//result.failed_val = 0;
		return (dcsspresult_t){DCSSP_IGNORED_RETVAL, 0};
	}
	 // failed to access the descriptor

	// finish the operation based on the descriptor's state
	if ((retval == DCSSP_STATE_UNDECIDED && state == DCSSP_STATE_SUCCEEDED) || retval == DCSSP_STATE_SUCCEEDED) {
		assert(helpingOther || ((snapshot->mutables & DCSSP_MUTABLES_MASK_STATE) >> DCSSP_MUTABLES_OFFSET_STATE) == DCSSP_STATE_SUCCEEDED);
		BOOL_CAS(snapshot->addr2, (casword_t) tagptr, snapshot->new2);
		//result.status = DCSSP_SUCCESS;
		//result.failed_val = 0;
		return (dcsspresult_t){DCSSP_SUCCESS, 0};
	} else {
		assert((retval == DCSSP_STATE_UNDECIDED && state == DCSSP_STATE_FAILED) || retval == DCSSP_STATE_FAILED);
		assert(helpingOther || ((snapshot->mutables & DCSSP_MUTABLES_MASK_STATE) >> DCSSP_MUTABLES_OFFSET_STATE) == DCSSP_STATE_FAILED);
		BOOL_CAS(snapshot->addr2, (casword_t) tagptr, snapshot->old2);
		//result.status = DCSSP_FAILED_ADDR1;
		//result.failed_val = val1;
		return (dcsspresult_t){DCSSP_FAILED_ADDR1, val1};
	}
}

void dcsspHelpOther(dcsspProvider_t *dcs, const int tid, dcssptagptr_t tagptr) {
	const int otherTid = TAGPTR_UNPACK_TID(tagptr);
	assert(otherTid >= 0 && otherTid < dcs->NUM_PROCESSES);
	dcsspdesc_t newSnapshot;
	const int sz =sizeof(mutables_t)+sizeof(casword_t*)*2+sizeof(casword_t)*3;
	//const int sz = sizeof(newSnapshot.mutables)+sizeof(newSnapshot.addr1)+sizeof(newSnapshot.old1)+sizeof(newSnapshot.addr2)+sizeof(newSnapshot.old2)+sizeof(newSnapshot.new2);
	assert((((tagptr & MASK_SEQ) >> OFFSET_SEQ) & 1) == 1);
	if (DESC_SNAPSHOT_2(dcs->dcsspDescriptors, &newSnapshot, tagptr, sz)) {
		dcsspHelp(dcs, tid, tagptr, &newSnapshot, true);
	} else {

	}
}

tagptr_t getDescriptorTagptr(dcsspProvider_t *dcs, const int otherTid) {
	dcsspptr_t ptr = &(dcs->dcsspDescriptors[otherTid]);
	tagptr_t tagptr = TAGPTR_NEW(otherTid, ptr->mutables, DCSSP_TAGBIT);
	if ((UNPACK_SEQ(tagptr) & 1) == 0) {
		// descriptor is being initialized! essentially,
		// we can think of there being NO ongoing operation,
		// so we can imagine we return NULL = no descriptor.
		return (tagptr_t) NULL;
	}
	return tagptr;
}

dcsspptr_t getDescriptorPtr(dcsspProvider_t *dcs, tagptr_t tagptr) {
	return TAGPTR_UNPACK_PTR(dcs->dcsspDescriptors, tagptr);
}

bool getDescriptorSnapshot(dcsspProvider_t *dcs, tagptr_t tagptr, dcsspptr_t const dest) {
	if (tagptr == (tagptr_t) NULL) return false;
	const int sz =sizeof(mutables_t)+sizeof(casword_t*)*2+sizeof(casword_t)*3;
	//const int sz = sizeof(newSnapshot.mutables)+sizeof(newSnapshot.addr1)+sizeof(newSnapshot.old1)+sizeof(newSnapshot.addr2)+sizeof(newSnapshot.old2)+sizeof(newSnapshot.new2);
	//return DESC_SNAPSHOT(dcsspdesc_t, dcs->dcsspDescriptors, dest, tagptr, sz);
	return DESC_SNAPSHOT_2(dcs->dcsspDescriptors, dest, tagptr, sz);	
}

void helpProcess(dcsspProvider_t *dcs, const int tid, const int otherTid) {
	tagptr_t tagptr = getDescriptorTagptr(dcs, otherTid);
	if (tagptr != (tagptr_t) NULL) dcsspHelpOther(dcs, tid, tagptr);
}

dcsspresult_t dcsspVal(dcsspProvider_t *dcs, const int tid, casword_t *addr1, casword_t old1, casword_t *addr2, casword_t old2, casword_t new2) {
	return dcsspPtr(dcs, tid, addr1, old1, addr2, old2 << DCSSP_LEFTSHIFT, new2 << DCSSP_LEFTSHIFT);
}

dcsspresult_t dcsspPtr(dcsspProvider_t *dcs, const int tid, casword_t *addr1, casword_t old1, casword_t *addr2, casword_t old2, casword_t new2) {
	// create dcssp descriptor
	//dcsspresult_t res;
	dcsspptr_t ptr = DESC_NEW(dcs->dcsspDescriptors, DCSSP_MUTABLES_NEW, tid);
	assert((((dcs->dcsspDescriptors[tid].mutables & MASK_SEQ) >> OFFSET_SEQ) & 1) == 0);
	ptr->addr1 = addr1;
	ptr->old1 = old1;
	ptr->addr2 = addr2;
	ptr->old2 = old2;
	ptr->new2 = new2;

	DESC_INITIALIZED(dcs->dcsspDescriptors, tid);

	assert((((dcs->dcsspDescriptors[tid].mutables & MASK_SEQ) >> OFFSET_SEQ) & 1) == 1); 
	tagptr_t tagptr = TAGPTR_NEW(tid, ptr->mutables, DCSSP_TAGBIT);

	// perform the dcssp operation described by our descriptor
	casword_t r;
	do {
		assert(!isDcssp(ptr->old2));
		assert(isDcssp(tagptr));
		r = VAL_CAS(ptr->addr2, ptr->old2, (casword_t) tagptr);
		if (isDcssp(r)) {
#ifdef USE_DEBUGCOUNTERS
			this->dcsspHelpCounter->inc(tid);
#endif
			dcsspHelpOther(dcs, tid, (dcssptagptr_t) r);
		}
	} while (isDcssp(r));
	if (r == ptr->old2){
		//        DELAY_UP_TO(1000);
		return dcsspHelp(dcs, tid, tagptr, ptr, false); // finish our own operation      
	} 
	//res.status = DCSSP_FAILED_ADDR2;
	//res.failed_val = r;
	return (dcsspresult_t){DCSSP_FAILED_ADDR2, r};//DCSSP_FAILED_ADDR2;
}

casword_t dcsspRead(dcsspProvider_t *dcs, const int tid, casword_t volatile *addr) {
	casword_t r;
	while (1) {
		r = *addr;
		if (isDcssp(r)){
#ifdef USE_DEBUGCOUNTERS
			inc(dcs->dcssHelpCounter, tid);
#endif
			dcsspHelpOther(dcs, tid, (dcssptagptr_t) r);
		} else {
			return r;
		}
	}
}

casword_t readPtr(dcsspProvider_t *dcs, const int tid, casword_t volatile *addr) {
	casword_t r;
	r =dcsspRead(dcs, tid, addr);
	return r;
}

casword_t readVal(dcsspProvider_t *dcs, const int tid, casword_t volatile *addr) {
	return ((casword_t) readPtr(dcs, tid, addr)) >> DCSSP_LEFTSHIFT;
}


void writePtr(casword_t volatile *addr, casword_t ptr){
	assert((ptr & DCSSP_TAGBIT) == 0);
	*addr = ptr;
}

void writeVal(casword_t volatile *addr, casword_t val) {
	writePtr(addr, val<<DCSSP_LEFTSHIFT);
}

void dcsspProvider_init(dcsspProvider_t *dcs, const int numProcesses) {
#ifdef USE_DEBUGCOUNTERS
	dcs->dcsspHelpCounter = (debugCounter*)malloc(NUM_PROCESSES);
#endif
	dcs->NUM_PROCESSES = numProcesses;
	DESC_INIT_ALL(dcs->dcsspDescriptors, DCSSP_MUTABLES_NEW, dcs->NUM_PROCESSES);
	for (int tid = 0; tid < dcs->NUM_PROCESSES; ++tid) {
		dcs->dcsspDescriptors[tid].addr1 = 0;
		dcs->dcsspDescriptors[tid].addr2 = 0;
		dcs->dcsspDescriptors[tid].new2 = 0;
		dcs->dcsspDescriptors[tid].old1 = 0;
		dcs->dcsspDescriptors[tid].old2 = 0;
	}
}

void dcsspProvider_finish(dcsspProvider_t *dcs) {
#ifdef USE_DEBUGCOUNTERS
	free(dcs->dcsspHelpCounter);
#endif	
}

void initThread(const int tid) {}

void deinitThread(const int tid) {}
