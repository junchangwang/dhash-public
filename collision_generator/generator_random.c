
#include <stdio.h>      /* defines printf for tests */
#include <time.h>       /* defines time_t for timings in the test */
#include <stdint.h>     /* defines uint32_t etc */
#include <sys/param.h>  /* attempt to define endianness */
#ifdef linux
# include <endian.h>    /* attempt to define endianness */
#endif
#include "lookup3.h"
#include "../api.h"

int main(int argc, char *argv[])
{
	unsigned long *output;
	long output_len = 0;
	unsigned long data;
	char *buf = (char *)&data;
	int len = sizeof(data)/sizeof(*buf);
	int generator_size;
	int bucket_size;

	unsigned long elperworker = 10000000;
	int nthreads = 48;

	if (argc < 3) {
		// FIXME: ./generator size bucket_size
		printf("Usage: ./generator size (should be less than 2^32) bucket_size.\n");
		exit(-1);
	}

	generator_size = strtol(argv[1], NULL, 0);
	output = calloc(generator_size, sizeof(unsigned long));
	if (!output) {
		printf("ERROR in calloc\n");
		exit(-1);
	}

	int good;
	bucket_size = strtol(argv[2], NULL, 0);

	for (int i = 0; i < generator_size; i++) {
		good = 1;
		data = random();
		if (data < (elperworker * nthreads)) {
			// The generated key(data) might conflict with the keys
			// manipulated by worker threads. To avoid interference,
			// the collistion-generation thread skips these data. 
			continue;
		}

		for (int j = 0; j < output_len; j++) {
			// The generated key(data) has been generated and recorded
			// in the output[] array, so we skip this one. 
			if (output[j] == data) {
				good = 0;
				break;
			}
		}

		if (!good)
			continue;

		uint32_t result = hashlittle(buf, len, 0);
		uint32_t bucket = result & (bucket_size-1);
		// We only record the data targeting bucket[0].
		// I.e., the collistion-generation thread injects
		// keys mapped to the first bucket of the hash table
		// to simulate malicious attacks or bursts of incoming data.
		if (bucket == 0) {
			output[output_len++] = data;
		}
	}

	char output_file_name[1024] = "collision_log_random_";

	strcat(output_file_name, argv[1]);
	strcat(output_file_name, "_");
	strcat(output_file_name, argv[2]);

	FILE *output_file = fopen(output_file_name, "w");

	for (int i = 0; i < output_len - 1; i++) {
		//printf("%lu \t 0\n", output[i]);
		// FIXME: the output should be dumpped to a file, and the
		// file name is automatically generated in the form of
		// "collision_log_random_argv[1]_argv[2]"
		fprintf(output_file, "%lu \t 0\n", output[i]);
	}

	fclose(output_file);
	
	free(output);
	return 0;
}
