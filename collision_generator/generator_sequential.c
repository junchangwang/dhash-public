
#include <stdio.h>      /* defines printf for tests */
#include <time.h>       /* defines time_t for timings in the test */
#include <stdint.h>     /* defines uint32_t etc */
#include <sys/param.h>  /* attempt to define endianness */
#ifdef linux
# include <endian.h>    /* attempt to define endianness */
#endif
#include "lookup3.h"
#include "../api.h"

struct output_item {
	uint32_t seed;
	long value;
};

int main(int argc, char *argv[])
{
	struct output_item *output;
	long output_len = 0;
	unsigned long data;
	char *buf = (char *)&data;
	int len = sizeof(data)/sizeof(*buf);
	int generator_size;
	int seed_range;
	int bucket_size;

	unsigned long elperworker = 10000000;
	int nthreads = 48;

	if (argc < 4) {
		printf("Usage: ./generator size (should be less than 2^32) seed_range [1, 16] bucket_size.\n");
		exit(-1);
	}

	generator_size = strtol(argv[1], NULL, 0);
	seed_range = strtol(argv[2], NULL, 0);
	bucket_size = strtol(argv[3], NULL, 0);

	fprintf(stderr, "./generator %d %d %d\n", generator_size, seed_range, bucket_size);

	if ((seed_range < 1) || (seed_range > 16)) {
		printf("Usage: ./generator size (should be less than 2^32) seed_range [1, 16] bucket_size.\n");
		exit(-1);
	}

	output = calloc(generator_size, sizeof(*output));
	if (!output) {
		printf("ERROR in calloc\n");
		exit(-1);
	}

	for (int i = 0; i < generator_size; i++) {
		data = (elperworker * nthreads) + i;

		for (int j = 0; j < seed_range; j++) {
			uint32_t result = hashlittle(buf, len, 0 + j);
			uint32_t bucket = result & (bucket_size-1);
			if (bucket == 0) {
				output[output_len].value = data;
				output[output_len].seed = j;
				output_len ++;
				break;
			}
		}
	}

	char output_file_name[1024] = "collision_log_sequential_";

	strcat(output_file_name, argv[1]);
	strcat(output_file_name, "_");
	strcat(output_file_name, argv[3]);

	FILE *output_file = fopen(output_file_name, "w");

	for (int i = 0; i < output_len - 1; i++) {
		fprintf(output_file, "%lu\t\t%d\n", output[i].value, output[i].seed);
		/*printf("data: %lu. value: %u, %x. bucket: %u, %x\n",*/
		/*data, result, result, bucket, bucket);*/
	}
	
	fclose(output_file);
	free(output);
	return 0;
}
