CFLAGS += -g -O0 -Wall
PROGS =	HT-Xu HT-RHT HT-DHash-wf HT-Split HT-DHash-lf-dcss
LDFLAGS = -L/usr/local/lib -lpthread -lurcu -lurcu-signal
TORT = hashtorture-extended.h
LIBS = rculflist-split.o rcuwflist.o lookup3.o rculflist-dcss.o
arch := $(shell uname -m)

all: api.h $(LIBS) $(PROGS) test_lookup3

test_lookup3: lookup3.o

lookup3.o:

rculflist-dcss.o: rculflist-dcss.c rculflist-dcss.h api.h
	gcc $(CFLAGS) -c -o $@ $< $(LDFLAGS)

rculflist-split.o: rculflist-split.c rculflist-split.h api.h
	cc $(CFLAGS) -c -o $@ $< $(LDFLAGS)

rcuwflist.o: rcuwflist.c rcuwflist.h api.h
	cc $(CFLAGS) -c -o $@ $< $(LDFLAGS)

HT-DHash-lf-dcss: HT-DHash-lf-dcss.c rculflist-dcss.o $(TORT)
	gcc $(CFLAGS) -o $@ $< rculflist-dcss.o lookup3.o $(LDFLAGS)

HT-DHash-wf: HT-DHash-wf.c rcuwflist.o $(TORT)
	cc $(CFLAGS) -o $@ $< rcuwflist.o lookup3.o $(LDFLAGS)

HT-Split: HT-Split.c HT-Split-helper.h rculflist-split.o $(TORT)
	cc $(CFLAGS) -o $@ $< rculflist-split.o lookup3.o $(LDFLAGS)

HT-RHT: HT-RHT.h

%: %.c $(TORT) $(LIBS) 
	cc $(CFLAGS) -o $@ $< lookup3.o $(LDFLAGS)

api.h:
ifeq ($(arch),x86)
	cp ./include/api_x86.h ./api.h
else ifeq ($(arch),x86_64)
	cp ./include/api_x86.h ./api.h
else ifeq ($(arch),ppc64le)
	cp ./include/api_ppc64.h ./api.h
else ifeq ($(arch),aarch64)
	cp ./include/api_arm64.h ./api.h
endif

clean:
	rm -f $(LIBS) $(PROGS) api.h test_lookup3
