
OBJ = test_lookup3 generator_random generator_sequential generator_sequential_split

ALL: $(OBJ)

test_lookup3: lookup3.o

generator_random generator_sequential generator_sequential_split: lookup3.o

lookup3.o:

.PHONY: clean

clean:
	rm -fr $(OBJ) lookup3.o
