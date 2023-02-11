/* 
 * File:   debugprinting.h
 * Author: trbot
 *
 * Created on June 24, 2016, 12:49 PM
 */

#ifndef DEBUGPRINTING_H
#define	DEBUGPRINTING_H

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define COUTATOMIC(printstr) printf("%s", printstr)
#define COUTATOMICTID(printstr) printf("tid = %d %d : %s", tid, (tid<10?" ":""), printstr)

#ifdef USE_TRACE
atomic_bool ___trace(1);
atomic_bool ___validateops(1);
#define TRACE_TOGGLE {bool ___t = ___trace; ___trace = !___t;}
#define TRACE_ON {___trace = true;}
#define TRACE if(___trace)
#else
#define TRACE if(0)
#define TRACE_TOGGLE
#define TRACE_ON 
#endif

#endif /* DEBUGPRINTING_H */
