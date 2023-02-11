/* 
 * File:   dcss_plus.h
 * Author: Maya Arbel-Raviv
 *
 * Created on May 1, 2017, 10:42 AM
 * 
 * Changed to the version of C by Dunwei Liu on 2021.
 * Renamed to dcss.h
 * 
 */

#ifndef DCSS_PLUS_H
#define DCSS_PLUS_H

#include <stdarg.h>
#include <signal.h>
#include <string.h>
#include <stdbool.h>
#include "plaf.h"
#include "descriptors_new.h"

#define dcssptagptr_t uintptr_t
#define dcsspptr_t struct dcsspdesc *
#define casword_t intptr_t

#define DCSSP_STATE_UNDECIDED 0
#define DCSSP_STATE_SUCCEEDED 4
#define DCSSP_STATE_FAILED 8

#define DCSSP_LEFTSHIFT 1

#define DCSSP_IGNORED_RETVAL -1
#define DCSSP_SUCCESS 0
#define DCSSP_FAILED_ADDR1 1 
#define DCSSP_FAILED_ADDR2 2 


typedef struct dcsspresult {
	int status;
	casword_t failed_val;
}dcsspresult_t;

typedef struct dcsspdesc {
	volatile mutables_t mutables;
	casword_t volatile * volatile addr1;
	casword_t volatile old1;
	casword_t volatile * volatile addr2;
	casword_t volatile old2;
	casword_t volatile new2;
	char padding[PREFETCH_SIZE_BYTES+(((64<<10)-(sizeof(mutables_t)+sizeof(casword_t*)*2+sizeof(casword_t)*3)%64)%64)]; // add padding to prevent false sharing
}dcsspdesc_t __attribute__ ((aligned(128)));

typedef struct dcsspProvider {
	/*
	 *  Data definitions
	 */

#define DCSSP_MUTABLES_OFFSET_STATE 0 
#define DCSSP_MUTABLES_MASK_STATE 0xf
#define DCSSP_MUTABLES_NEW(mutables) ((((mutables)&MASK_SEQ)+(1<<OFFSET_SEQ)) | (DCSSP_STATE_UNDECIDED<<DCSSP_MUTABLES_OFFSET_STATE))
#include "descriptors_impl2_new.h"
	dcsspdesc_t dcsspDescriptors[LAST_TID+1];

#ifdef USE_DEBUGCOUNTERS
	debugCounter * dcsspHelpCpunter;
#endif
	int NUM_PROCESSES;
}dcsspProvider_t __attribute__ ((aligned(128)));

extern 
void initThread(const int tid);  
extern 
void deinitThread(const int tid); 

extern 
void dcsspProvider_init(dcsspProvider_t *dcs, const int numProcesses);
extern 
void dcsspProvider_finish(dcsspProvider_t *dcs);

extern 
void writePtr(casword_t volatile *addr, casword_t val);
extern 
void writeVal(casword_t volatile *addr, casword_t val);

extern 
casword_t readPtr(dcsspProvider_t *dcs, const int tid, casword_t volatile *addr);  
extern 
casword_t readVal(dcsspProvider_t *dcs, const int tid, casword_t volatile *addr); 

extern 
dcsspresult_t dcsspPtr(dcsspProvider_t *dcs, const int tid, casword_t *addr1, casword_t old1, casword_t *addr2, casword_t old2, casword_t new2);  //
extern 
dcsspresult_t dcsspVal(dcsspProvider_t *dcs, const int tid, casword_t *addr1, casword_t old1, casword_t *addr2, casword_t old2, casword_t new2); //

extern 
void debugPrint();

extern 
tagptr_t getDescriptorTagptr(dcsspProvider_t *dcs, const int otherTid);   //
extern 
dcsspptr_t getDescriptorPtr(dcsspProvider_t *dcs, tagptr_t tagptr);       //

extern 
bool getDescriptorSnapshot(dcsspProvider_t *dcs, tagptr_t tagptr, dcsspptr_t const dest); //
extern 
void helpProcess(dcsspProvider_t *dcs, const int tid, const int otherTid);    //

extern 
casword_t dcsspRead(dcsspProvider_t *dcs, const int tid, casword_t volatile *addr); //
extern 
dcsspresult_t dcsspHelp(dcsspProvider_t *dcs, const int tid, dcssptagptr_t tagptr, dcsspptr_t snapshot, bool helpingOther); //
extern 
void dcsspHelpOther(dcsspProvider_t *dcs, const int tid, dcssptagptr_t tagptr);                                              //

#endif /* DCSS_PLUS_H */
