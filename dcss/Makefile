
LDFLAGS = -lpthread -lnuma

testing: testing.c
	gcc -g -o $@ $< $(LDFLAGS)

testing_new: testing_new.c
	gcc -g -o $@ $< $(LDFLAGS)

clean:
	rm -fr testing testing_new
