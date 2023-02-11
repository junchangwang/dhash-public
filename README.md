
### About this project

DHash is a hash table algorithm that allows programmers to dynamically change its hash function, without affecting other concurrent operations such as lookup, insert, and delete. DHash provides a strong guarantee of service-level agreement (SLA); it is efficient and robust in face of malicious attacks and/or bursts of incoming data. You can find more about DHash algorithm at [1].

This project contains the user-space implementation of DHash, and is released under GPL V3. Should you have any question, please feel free to contact us through liudunwei0924@qq.com.

### Organization

```
dhash/
  - hashtorture-extended.h: Benchmarking framework borrowed from Paul's perfbook.
                            Note that this version contains several new features
                            and optimizations, and hence the suffix ''-extended''.

  - HT-DHash-lf-dcss.c: Source code of DHash utilizing Michael's classic lock-free linked list. Note that this version uses the double-compare-single-swap (dcss) primitive.
  
  - HT-DHash-wf.c: Source code of DHash utilizing Heller et al.'s non-blocking linked list (that provides wait-free lookup operations). Note that this version does not contain the dcss primitive such that it is conceptually straightforward to understand.

  - HT-RHT.c: A user-space implementation of the resizable hash table algorithm widely used in the Linux kernel.

  - HT-Split.c: An implementation of the split-ordered-list hash table algorithm.

  - HT-Xu.c: A user-space implementation of the resizable hash table algorithm introduced by Herbert Xu into the Linux kernel.

  - rculflist-dcss.[hc]: A RCU-based, lock-free, ordered linked list based on Maged Michael's algorithm.

  - rcuwflist.[hc]: A RCU-based, wait-free (lookup), ordered linked list.
'''


### Compile the project

* This project depends on [userspace-rcu](https://liburcu.org/). Before running the code, you must first install userspace-rcu.

* Command ''make'' should be enough to compile the project on most Linux platforms.


### How to run?

  1. How to run the experiments?
      1) Execution Options
          -- perftest

            Performance Test.

          -- pcttest
          
            Percentage Test.

          -- jhash
            Use Bob Jenkins's hash function (lookup3).
          
          -- rebuild
          
            Choose a new hash function (or seed) each time we change the size of the hash table.
          
          -- collision

            Log file recording collision hash data.

          -- nbuckets  

            Number of buckets, defaults to 1024.

          -- nreaders

            Number of readers, defaults to 1 (for perftest only).

          -- nupdaters

            Number of updaters, defaults to 1. Must be 1 or greater, or hash table will be empty (for perftest only).

          -- updatewait

            Number of spin-loop passes per update, defaults to -1. If 0, the updater will not do any updates, except for initialization. If negative, the updater waits for the corresponding number of milliseconds between updates (for perftest only).

          -- nworkers

            Number of workers, defaults to 1. Must be 1 or greater. Each worker performs a mix of different operations (for pcttest only).

          -- percentage
        
            Percentage values for Insert, Delete, and Lookup respectively, default to 5 5 90 (for pcttest only).

          -- elems/writer

            Number of hash-table elements per writer, defaults to 2048. Must be greater than zero.

          -- preinsert

            Number of hash-table elements to be inserted into the hash table, defaults to 1024. Must be greater than zero.

          -- cpustride

            Stride when spreading threads across CPUs, defaults to 1.

          -- resizediv

            Divisor for resized hash table, defaults to zero (don't resize).

          -- resizemult

            Multiplier for resized hash table, defaults to zero (don't resize).

          -- resizewait

            Milliseconds to wait between resizes, defaults to one.

          -- dont-repeatedly-resize

            Resize/rebuild operation is performed only when the length of any list exceeds the specified threshold, or when the average load factor is lower than the specified threshold.

          -- max-list-length

            Perform resize/rebuild operations, if the length of any of the lists exceeds the specified limit.(for dont-repeatedly-resize only)

          -- min-avg-load-factor

            Perform resize/rebuild operations, if the average load factor become lower than the specified threshold.(for dont-repeatedly-resize only)

          -- max-nbuckets

            Maximum number of buckets (must >= 1024).

          -- measure-latency

            Size of the array to record latency (must >= 1024).

          -- duration

            Duration of test, in milliseconds.

          -- rebuild-threads

            Number of rebuilding threads (must >=1 and <=32). Default 1.
    
      2) Example

        ./HT-DHash-lf-dcss --pcttest --percentage 5 5 90 --nbuckets 1024 --elems/writer 10000000 --preinsert 16384 --duration 3000 --dont-repeatedly-resize --max-list-length 32 --resizemult 2 --nworkers 4 --collision collision_log_seq_16 --jhash --rebuild --measure-latency 1024000

        => Percentage test of HT-DHash-lf-dcss.  
        => The percentage of Insert, Delete and Lookup is 5, 5, 90, respectively.  
        => Number of buckets is 1024.  
        => Number of hash-table elements per writer is 10000000.  
        => Number of hash-table elements to be inserted into the hash table is 16384.  
        => Duration of test is 3000 milliseconds.  
        => Resize/rebuild operation is performed only when the length of any list exceeds the specified threshold, or when the average load factor is lower than the specified threshold.  
        => Perform resize/rebuild operations, if the length of any of the lists exceeds 32.  
        => Multiplier for resized hash table is 2.  
        => Number of workers is 4.  
        => Collision hash data from Log file collision_log_seq_16.  
        => Use Bob Jenkins's hash function (lookup3).  
        => Choose a new hash function (or seed) each time we change the size of the hash table.  
        => Size of the array to record latency is 1024000.
      
  2. How to generate collision traffic?

    1) collision_generator/generator_random.c
    
      ./generator_random (size) (bucket_size)

      The element size is the maximum number of data that stored in the output file. The all data will be inserted into the bucket[0]. The key of data is generated randomly. The bucket_size is the number of buckets in the hash table.  
      The size should be less than 2^32, and the bucket_size must larger than 1.
    
    2) collision_generator/generator_sequential_split.c
    
      ./generator_sequential_split (size) (seed_range) (bucket_size)

      The element size is the number of data that stored in the output file. The all data will be inserted into the bucket[0]. The key of data is sequential. The bucket_size is the number of buckets in the hash table.  
      The size should be less than 2^32, the seed_rang ranges from 1 to 16, and the bucket_size must be larger than 1.
      
    
    3) collision_generator/generator_sequential.c
    
      ./generator_sequential (size) (seed_range) (bucket_size) 

      The element size is the number of data that stored in the output file. The all data will be inserted into the bucket[0]. The key of data is sequential. The bucket_size is the number of buckets in the hash table.  
      The size should be less than 2^32, the seed_range ranges from 1 to 16, and the bucket_size must be larger than 1.
      
```

------

[1] Junchang Wang, Dunwei Liu, et al. **DHash: Dynamic Hash Tables with Non-blocking Regular Operations**. In TPDS'2022. https://ieeexplore.ieee.org/document/9714033

