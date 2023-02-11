
static hash_t
reverse_value (hash_t k)
{
        int i;
        hash_t r = 0;
        for (i = 0; i < 32; ++i) {
                hash_t bit = (k & (1 << i)) >> i;
                r |= bit << (31 - i);
        }
        return r;
}

static inline hash_t
hash_regular_key (hash_t k)
{
        return reverse_value (k | 0x80000000);
}

static inline hash_t
hash_dummy_key (hash_t k)
{
        return reverse_value (k & ~0x80000000);
}

static inline unsigned int
is_dummy_node (hash_t k)
{
        return (k & 0x1) == 0;
}

static inline unsigned int 
is_regular_node (hash_t k)
{
        return (k & 0x1) == 1;
}

#define atomic_fetch_and_inc(t) __sync_fetch_and_add (t, 1)
#define atomic_fetch_and_dec(t) __sync_fetch_and_sub (t, 1)
#define atomic_compare_and_swap(t,old,new) __sync_bool_compare_and_swap (t, old, new)

#define load_barrier __builtin_ia32_lfence
#define store_barrier __builtin_ia32_sfence
#define memory_barrier __builtin_ia32_mfence

#define atomic_load(p)  ({ typeof(*p) __tmp = *(p); load_barrier (); __tmp; })
#define atomic_store(p, v) do { store_barrier (); *(p) = v; } while (0);

static inline mark_ptr_t
mk_node (struct ht_node *n, uintptr_t bit)
{
        return (mark_ptr_t)(((uintptr_t)n) | bit);
}

static inline struct ht_node *
get_node (mark_ptr_t n)
{
        return (struct ht_node*)(((uintptr_t)n) & ~(uintptr_t)0x1);
}

static inline uintptr_t
get_bit (mark_ptr_t n)
{
        return (uintptr_t)n & 0x1;
}

static unsigned
get_parent (unsigned b)
{
        int i;
	if (b == 0)
		printf("WARNING: trying to get the parent of bkt[0] in get_parent().\n");
        for (i = 31; i >= 0; --i) {
                if (b & (1 << i))
                        return b & ~(1 << i);
        }
        return 0;
}


