/*
 * spec:
 * (1) single writer thread, multi reader thread.
 * (2) lock free
 * (3) x86_64 only, must be ready _mm_sfence(), _mm_lfence(), _mm_crc32_u32()
 * (4) support add, del, search API
 *
 */

#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include <stdatomic.h>

#include "dc_hash_tbl.h"


#define ARRAYOF(_a)	(sizeof(_a)/sizeof(_a[0]))

#if 0
# define TRACER(fmt,...)	fprintf(stderr, "%s():%d " fmt, __func__, __LINE__, ##__VA_ARGS__)
#else
# define TRACER(fmt,...)
#endif

#if 1
#define NOTIFY_CB(_tbl, _bk, _pos, _ev, _exp)                           \
        do {                                                            \
                if ((_tbl)->event_notify_cb && (_exp)) {                \
                        (_tbl)->event_notify_cb((_tbl)->arg, (_ev), (_bk), (_pos)); \
                }                                                       \
        } while (0)
#else
#define NOTIFY_CB(_tbl, _bk, _pos, _ev, _exp)
#endif

/******************************************************************
 * Table Reader|writer
 ******************************************************************/
static inline void
store_key(struct dcht_bucket_s * bk,
          int pos,
          uint32_t key)
{
        atomic_store_explicit(&bk->key[pos], key, memory_order_release);
}

static inline void
store_val(struct dcht_bucket_s * bk,
          int pos,
          uint32_t val)
{
        atomic_store_explicit(&bk->val[pos], val, memory_order_relaxed);
}

/*
 * for writer thread
 */
static inline void
store_key_val(struct dcht_bucket_s * bk,
              int pos,
              uint32_t key,
              uint32_t val)
{
        /* write the value and then the key */
        atomic_store_explicit(&bk->val[pos], val, memory_order_relaxed);
        atomic_store_explicit(&bk->key[pos], key, memory_order_release);
}

static inline uint32_t
load_key(const struct dcht_bucket_s * bk,
         int pos)
{
        uint32_t key = atomic_load_explicit(&bk->key[pos], memory_order_acquire);
        return key;
}

static inline int
load_val(const struct dcht_bucket_s * bk,
         int pos,
         uint32_t key,
         uint32_t * val_p)
{
        uint32_t val = atomic_load_explicit(&bk->val[pos], memory_order_relaxed);
        uint32_t cur = load_key(bk, pos);
        int ret = -1;

        if (cur == key) {
                *val_p = val;
                ret = 0;
        }

        return ret;
}

/**
 * @brief prefetch memory (non temporal)
 *
 * @param memory pointer
 * @return void
 */
static inline void
prefetch(const void *p)
{
        //        asm volatile ("prefetchnta %[p]" : : [p] "m" (*(const volatile char *)p));
        __builtin_prefetch(p, 0, 3);
}


#if defined(__x86_64__)
/*****************************************************************************
 * x86_64 depened code start--->
 *****************************************************************************/
#include <immintrin.h>

/*
 * @brief read barrier
 */
static inline uint32_t
hash32(uint32_t init,
         uint32_t val)
{
        return _mm_crc32_u32(init, val);
}

/*
 * @brief 32bit byte swap
 */
static inline uint32_t
bswap32(uint32_t val)
{
        return __builtin_bswap32(val);
}


/**
 * @brif population count
 *
 * @param value
 * @return return unsigned
 */
static inline unsigned
popcnt(uint32_t v)
{
        return __builtin_popcount(v);
}


/******************************************************************************
 * AVX2 code
 ******************************************************************************/
#define	KEY32_MASK	0x81818181

/*
 *  key find in 1 bucket (async)
 */
static inline int
find_key_in_bucket_AVX2(const struct dcht_bucket_s * bk,
                        uint32_t key)
{
        __m256i search_key = _mm256_set1_epi32(key);

        TRACER("bk %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk->key[0], bk->key[1], bk->key[2], bk->key[3],
               bk->key[4], bk->key[5], bk->key[6], bk->key[7]);

        __m256i keys = _mm256_load_si256((__m256i *) (volatile void *) bk->key);
        __m256i cmp_result = _mm256_cmpeq_epi32(search_key, keys);
        int mask = KEY32_MASK & _mm256_movemask_epi8(cmp_result);
        int pos = -1;

        if (mask)
                pos = _tzcnt_u32(mask) / 4;

        TRACER("key:%u pos:%d mask:%08x\n", key, pos, mask);
        return pos;
}

/*
 * key find in 2 buckets (async)
 */
static inline int
find_key_in_bucket_pair_AVX2(struct dcht_bucket_s ** bk_p,
                             uint32_t key,
                             int * pos_p)
{
        __m256i search_key = _mm256_set1_epi32(key);
        __m256i keys[2];
        __m256i cmp_result[2];

        TRACER("K0 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[0]->key[0], bk_p[0]->key[1], bk_p[0]->key[2], bk_p[0]->key[3],
               bk_p[0]->key[4], bk_p[0]->key[5], bk_p[0]->key[6], bk_p[0]->key[7]);
        TRACER("K1 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[1]->key[0], bk_p[1]->key[1], bk_p[1]->key[2], bk_p[1]->key[3],
               bk_p[1]->key[4], bk_p[1]->key[5], bk_p[1]->key[6], bk_p[1]->key[7]);

        keys[0] = _mm256_load_si256((__m256i *) (volatile void *) bk_p[0]->key);
        keys[1] = _mm256_load_si256((__m256i *) (volatile void *) bk_p[1]->key);

        cmp_result[0] = _mm256_cmpeq_epi32(search_key, keys[0]);
        cmp_result[1] = _mm256_cmpeq_epi32(search_key, keys[1]);

        for (int i = 0; i < 2; i++) {
                int mask = KEY32_MASK & _mm256_movemask_epi8(cmp_result[i]);

                if (mask) {
                        *pos_p = _tzcnt_u32(mask) / 4;

                        TRACER("key:%u bk_p:%d pos:%d mask:%08x\n", key, i, *pos_p, mask);
                        return i;
                }
        }
        TRACER("key:%u not found\n", key);
        return -1;
}

/*
 *  number of key in a bucket
 */
static inline unsigned
number_of_keys_in_bucket_AVX2(const struct dcht_bucket_s * bk,
                              uint32_t key)
{
        __m256i search_key = _mm256_set1_epi32(key);

        TRACER("bk %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk->key[0], bk->key[1], bk->key[2], bk->key[3],
               bk->key[4], bk->key[5], bk->key[6], bk->key[7]);

        __m256i keys = _mm256_load_si256((__m256i *) (volatile void *) bk->key);
        __m256i cmp_result = _mm256_cmpeq_epi32(search_key, keys);
        int mask = KEY32_MASK & _mm256_movemask_epi8(cmp_result);
        unsigned nb = __builtin_popcount(mask);

        TRACER("key:%u nb:%u mask:%08x\n", key, nb, mask);
        return nb;
}

/*
 * Return the one with more key matches (async)
 */
static inline int
which_one_most_AVX2(struct dcht_bucket_s ** bk_p,
                    uint32_t key)
{
        __m256i search_key = _mm256_set1_epi32(key);
        int n[2], ret;

        TRACER("K0 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[0]->key[0], bk_p[0]->key[1], bk_p[0]->key[2], bk_p[0]->key[3],
               bk_p[0]->key[4], bk_p[0]->key[5], bk_p[0]->key[6], bk_p[0]->key[7]);
        TRACER("K1 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[1]->key[0], bk_p[1]->key[1], bk_p[1]->key[2], bk_p[1]->key[3],
               bk_p[1]->key[4], bk_p[1]->key[5], bk_p[1]->key[6], bk_p[1]->key[7]);

        for (int i = 0; i < 2; i++) {
                __m256i keys = _mm256_load_si256((__m256i *) (volatile void *) bk_p[i]->key);
                __m256i cmp_result = _mm256_cmpeq_epi32(search_key, keys);
                int mask = KEY32_MASK & _mm256_movemask_epi8(cmp_result);

                n[i] = __builtin_popcount(mask);
        }

        if (n[0] >= n[1])
                ret = 0;
        else
                ret = 1;

        if (!n[ret])
                ret = -1;

        TRACER("key:%u ret:%d n0:%d n1:%d\n", key, ret, n[0], n[1]);
        return ret;
}

/*
 * find, for reader (sync)
 */
static inline int
find_key_val_in_bucket_pair_sync_AVX2(struct dcht_bucket_s ** bk_p,
                                      uint32_t key,
                                      uint32_t * val_p)
{
        __m256i search_key = _mm256_set1_epi32(key);
        __m256i chks[2];
        int mask;
        int loop = 3;

        TRACER("K0 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[0]->key[0], bk_p[0]->key[1], bk_p[0]->key[2], bk_p[0]->key[3],
               bk_p[0]->key[4], bk_p[0]->key[5], bk_p[0]->key[6], bk_p[0]->key[7]);
        TRACER("K1 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[1]->key[0], bk_p[1]->key[1], bk_p[1]->key[2], bk_p[1]->key[3],
               bk_p[1]->key[4], bk_p[1]->key[5], bk_p[1]->key[6], bk_p[1]->key[7]);

 retry:
        chks[0] = _mm256_load_si256((__m256i *) (volatile void *) bk_p[0]->key);
        chks[1] = _mm256_load_si256((__m256i *) (volatile void *) bk_p[1]->key);

        do {
                __m256i keys[2];
                __m256i cmp_result[2];

                keys[0] = chks[0];
                keys[1] = chks[1];

                cmp_result[0] = _mm256_cmpeq_epi32(search_key, keys[0]);
                cmp_result[1] = _mm256_cmpeq_epi32(search_key, keys[1]);

                for (int i = 0; i < 2; i++) {
                        int mask = KEY32_MASK & _mm256_movemask_epi8(cmp_result[i]);

                        TRACER("mask:%08x\n", mask);
                        if (mask) {
                                int pos = _tzcnt_u32(mask) / 4;

                                if (load_val(bk_p[i], pos, key, val_p))
                                        goto retry;

                                TRACER("key:%u bk_p:%d pos:%d val:%u mask:%08x\n",
                                       key, i, pos, *val_p, mask);
                                return i;
                        }
                }

                break;
                /* check updating */
                chks[0] = _mm256_load_si256((__m256i *) (volatile void *) bk_p[0]->key);
                chks[1] = _mm256_load_si256((__m256i *) (volatile void *) bk_p[1]->key);

                cmp_result[0] = _mm256_cmpeq_epi32(keys[0], chks[0]);
                cmp_result[1] = _mm256_cmpeq_epi32(keys[1], chks[1]);

                mask = (_mm256_movemask_epi8(cmp_result[0]) | _mm256_movemask_epi8(cmp_result[1]));
                mask &= KEY32_MASK;
                mask ^= KEY32_MASK;
                TRACER("Re-load key:%u mask:%08x\n", key, mask);
        } while (mask && loop--);

        assert(loop > 0);

        return -1;
}

/**
 * @brief initialize bucket (unused)
 *
 * @param bk: bucket
 * @return void
 */
static inline void
bucket_init_AVX2(struct dcht_bucket_s * bk)
{
        __m256i search_key = _mm256_set1_epi32(DCHT_UNUSED_KEY);
        _mm256_store_si256((__m256i *) (volatile void *) bk->key, search_key);
        __sync_synchronize();
}

/*****************************************************************************
 * <---end x86 depened code
 *****************************************************************************/

#define FIND_KEY_IN_BUCKET(_bk,_key)			find_key_in_bucket_AVX2((_bk),(_key))
#define FIND_KEY_IN_BUCKET_PAIR(_bk_p,_key,_pos_p)	find_key_in_bucket_pair_AVX2((_bk_p),(_key),(_pos_p))
#define	NB_KEYS_IN_BUCKET(_bk, _key)			number_of_keys_in_bucket_AVX2((_bk),(_key))
#define WHICH_ONE_MOST(_bk_p, _key)			which_one_most_AVX2((_bk_p),(_key))
#define	FIND_VAL_IN_BUCKET_PAIR_SYNC(_bk_p,_key,_val_p)	find_key_val_in_bucket_pair_sync_AVX2((_bk_p),(_key),(_val_p))
#define	BUCKET_INIT(_bk)				bucket_init_AVX2((_bk))

#else	/* !__x86_64__ */

/*****************************************************************************
 * start Generic Arch code--->
 *****************************************************************************/

/*
 * @brief FNV-1a hash
 */
static inline uint32_t
hash32(uint32_t init,
       uint32_t val)
{
        uint32_t value[2];
        value[0] = init;
        value[1] = val;

        uint32_t hash = 0x811c9dc5;  // FNV offset basis
        uint8_t * ptr = (uint8_t *) value;
        uint32_t prime = 0x01000193; // FNV prime

        for (unsigned i = 0; i < sizeof(value); i++) {
                hash ^= (uint32_t) ptr[i];
                hash *= prime;
        }

        return hash;
}

/*
 * @brief 32bit byte swap
 */
static inline uint32_t
bswap32(uint32_t x)
{
        x = ((x << 8) & 0xFF00FF00) | ((x >> 8) & 0x00FF00FF);
        x = (x << 16) | (x >> 16);
        return x;
}

/**
 * @brif population count
 *
 * @param value
 * @return return unsigned
 */
static inline unsigned
popcnt(uint32_t v)
{
        unsigned nb = 0;
        for (unsigned i = 0; v; i++, v >>= 1) {
                if (v & 1)
                        nb++;
        }
        return nb;
}

/*
 *  key find in 1 bucket (async)
 */
static inline int
find_key_in_bucket_GEN(const struct dcht_bucket_s * bk,
                       uint32_t key)
{
        int pos;

        TRACER("bk %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk->key[0], bk->key[1], bk->key[2], bk->key[3],
               bk->key[4], bk->key[5], bk->key[6], bk->key[7]);

        for (pos = 0; pos < (int) DCHT_BUCKET_ENTRY_SZ; pos++) {
                if (key == load_key(bk, pos))
                        goto end;
        }
        pos = -1;
 end:
        TRACER("key:%u pos:%d\n", key, pos);
        return pos;
}

/*
 * key find in 2 buckets (async)
 */
static inline int
find_key_in_bucket_pair_GEN(struct dcht_bucket_s ** bk_p,
                            uint32_t key,
                            int * pos_p)
{
        TRACER("K0 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[0]->key[0], bk_p[0]->key[1], bk_p[0]->key[2], bk_p[0]->key[3],
               bk_p[0]->key[4], bk_p[0]->key[5], bk_p[0]->key[6], bk_p[0]->key[7]);
        TRACER("K1 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[1]->key[0], bk_p[1]->key[1], bk_p[1]->key[2], bk_p[1]->key[3],
               bk_p[1]->key[4], bk_p[1]->key[5], bk_p[1]->key[6], bk_p[1]->key[7]);

        for (int i = 0; i < 2; i++) {
                int pos;

                if ((pos = find_key_in_bucket_GEN(bk_p[i], key)) >= 0) {
                        *pos_p = pos;
                        TRACER("key:%u bk_p:%d pos:%d\n", key, i, *pos_p);
                        return i;
                }
        }
        TRACER("key:%u not found\n", key);
        return -1;
}

/*
 *  number of key in a bucket
 */
static inline unsigned
number_of_keys_in_bucket_GEN(const struct dcht_bucket_s * bk,
                             uint32_t key)
{
        TRACER("bk %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk->key[0], bk->key[1], bk->key[2], bk->key[3],
               bk->key[4], bk->key[5], bk->key[6], bk->key[7]);

        unsigned nb = 0;
        for (int pos = 0; pos < (int) DCHT_BUCKET_ENTRY_SZ; pos++) {
                if (load_key(bk, pos ) == key)
                        nb += 1;
        }

        TRACER("key:%u nb:%u\n", key, nb);
        return nb;
}

/*
 * Return the one with more key matches (async)
 */
static inline int
which_one_most_GEN(struct dcht_bucket_s ** bk_p,
                   uint32_t key)
{
        int n[2], ret;

        TRACER("K0 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[0]->key[0], bk_p[0]->key[1], bk_p[0]->key[2], bk_p[0]->key[3],
               bk_p[0]->key[4], bk_p[0]->key[5], bk_p[0]->key[6], bk_p[0]->key[7]);
        TRACER("K1 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[1]->key[0], bk_p[1]->key[1], bk_p[1]->key[2], bk_p[1]->key[3],
               bk_p[1]->key[4], bk_p[1]->key[5], bk_p[1]->key[6], bk_p[1]->key[7]);

        n[0] = 0;
        n[1] = 0;

        for (int i = 0; i < 2; i++) {
                for (int pos = 0; pos < (int) DCHT_BUCKET_ENTRY_SZ; pos++) {
                        if (load_key(bk_p[i], pos ) == key)
                                n[i] += 1;
                }
        }

        if (n[0] >= n[1])
                ret = 0;
        else
                ret = 1;

        if (!n[ret])
                ret = -1;

        TRACER("key:%u ret:%d n0:%d n1:%d\n", key, ret, n[0], n[1]);
        return ret;
}

/*
 * find, for reader (sync)
 */
static inline int
find_key_val_in_bucket_pair_sync_GEN(struct dcht_bucket_s ** bk_p,
                                     uint32_t key,
                                     uint32_t * val_p)
{
        int i;
        int loop = 5;

        TRACER("K0 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[0]->key[0], bk_p[0]->key[1], bk_p[0]->key[2], bk_p[0]->key[3],
               bk_p[0]->key[4], bk_p[0]->key[5], bk_p[0]->key[6], bk_p[0]->key[7]);
        TRACER("K1 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[1]->key[0], bk_p[1]->key[1], bk_p[1]->key[2], bk_p[1]->key[3],
               bk_p[1]->key[4], bk_p[1]->key[5], bk_p[1]->key[6], bk_p[1]->key[7]);

 retry:
        assert(--loop > 0);

        for (i = 0; i < 2; i++) {
                for (int pos = 0; pos < (int) DCHT_BUCKET_ENTRY_SZ; pos++) {
                        if (load_key(bk_p[i], pos) == key) {
                                if (load_val(bk_p[i], pos, key, val_p)) {
                                        goto retry;
                                } else {
                                        TRACER("key:%u bk_p:%d pos:%d val:%u\n",
                                               key, i, pos, *val_p);
                                        return i;
                                }
                        }
                }
        }
        TRACER("not found key:%u\n", key);
        return -1;
}

/**
 * @brief initialize bucket (unused)
 *
 * @param bk: bucket
 * @return void
 */
static inline void
bucket_init_GEN(struct dcht_bucket_s * bk)
{
        for (int pos = 0; pos < (int) DCHT_BUCKET_ENTRY_SZ; pos++)
                store_key(bk, pos, DCHT_UNUSED_KEY);
}

/*****************************************************************************
 * <---endstart Generic Arch code
 *****************************************************************************/
#define FIND_KEY_IN_BUCKET(_bk,_key)			find_key_in_bucket_GEN((_bk),(_key))
#define FIND_KEY_IN_BUCKET_PAIR(_bk_p,_key,_pos_p)	find_key_in_bucket_pair_GEN((_bk_p),(_key),(_pos_p))
#define	NB_KEYS_IN_BUCKET(_bk, _key)			number_of_keys_in_bucket_GEN((_bk),(_key))
#define WHICH_ONE_MOST(_bk_p, _key)			which_one_most_GEN((_bk_p),(_key))
#define	FIND_VAL_IN_BUCKET_PAIR_SYNC(_bk_p,_key,_val_p)	find_key_val_in_bucket_pair_sync_GEN((_bk_p),(_key),(_val_p))
#define	BUCKET_INIT(_bk)				bucket_init_GEN((_bk))

#endif	/* !__x86_64__ */


/**
 * @brief find vacancy position
 *
 * @param st: current bucket's state
 * @return Returns the position where a vacancy was found.
 *         Returns negative if not found.
 */
static inline int
find_vacancy(const struct dcht_bucket_s * bk)
{
        return FIND_KEY_IN_BUCKET(bk, DCHT_UNUSED_KEY);
}

/**
 * @brief Returns if bucket is full
 *
 * @param bk: bucket pointer
 * @return if is full then true
 */
static inline bool
is_bucket_full(const struct dcht_bucket_s * bk)
{
        return (find_vacancy(bk) < 0 ? 1 : 0);
}

/**
 * @brief Returns whether pos of bucket is used
 *
 * @param bk: bucket pointer
 * @param pos: check entry position
 * @return if entried then true
 */
static inline bool
is_valid_entry(const struct dcht_bucket_s * bk,
               int pos)
{
        return (bk->key[pos] != DCHT_UNUSED_KEY);
}

/**
 * @brief delete key
 *
 * @param bk: bucket
 * @param pos: delete position
 * @return void
 */
static inline void
del_key(struct dcht_bucket_s * bk,
        int pos)
{
        assert(0 <= pos && pos < (int) DCHT_BUCKET_ENTRY_SZ);

        store_key(bk, pos, DCHT_UNUSED_KEY);
}

/**
 * @brief move bucket entry
 *
 * @param dbk: destination bucket
 * @param dpos: destination entry position in dbk
 * @param sbk: source bucket
 * @param spos: source entry position in sbk
 * @return void
 */
static inline void
move_entry(struct dcht_bucket_s * dbk,
           int dpos,
           struct dcht_bucket_s * sbk,
           int spos)
{
        uint32_t key = sbk->key[spos];
        uint32_t val = sbk->val[spos];

        store_key_val(dbk, dpos, key, val);
        del_key(sbk, spos);
}

/**
 * @brief　Returns the value associated with the key registered in the bucket
 *
 * @param bk: bucket
 * @param key: search key
 * @param val_p: Pointer to set the read value
 * @return Returns the position on success, negative on failure
 */
static inline int
find_key_val(struct dcht_bucket_s * bk,
             uint32_t key,
             uint32_t * val_p)
{
        int pos = -1;

        do {
                pos = FIND_KEY_IN_BUCKET(bk, key);
                if (pos >= 0)
                        *val_p = *(volatile uint32_t *) &bk->val[pos];

                /* Reread if state is updated during search */
        } while (pos >= 0 && *((volatile uint32_t *) &bk->key[pos]) != key);

        return pos;
}


/**
 * @brief Set all bits below MSB
 *
 * @param v: input integer
 * @return integer
 */
static inline uint64_t
combine64ms1b(uint64_t v)
{
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        return v;
}

/**
 * @brief Returns the nearest power of 2 times greater than
 *
 * @param input integer
 * @return integer
 */
static inline uint64_t
align64pow2(uint64_t v)
{
        v--;
        v = combine64ms1b(v);
        return v + 1;
}

/**
 * @brief Fetch the bucket where key is entried
 *
 * @param tbl: hash table pointer
 * @param bk_pp: bucket pointer array[2]
 * @param key: entry key
 * @return void
 */
static inline void
buckets_fetch(struct dcht_hash_table_s *tbl,
              struct dcht_bucket_s ** bk_pp,
              uint32_t key)
{
        unsigned x, y, msk = tbl->mask;
        unsigned pos[2];
        int retry = 10;

        x = hash32(0xdeadbeef, key);
        x = hash32(x, bswap32(key));
        pos[0] = x & msk;
        while (!pos[0]) {
                x = hash32(x, key);
                pos[0] = x & msk;

                assert(--retry > 0);
        }
        retry = 10;

        y = bswap32(key ^ x);
        pos[1] = y & msk;
        while (pos[0] == pos[1] || !pos[1]) {
                y = hash32(y, ~bswap32(key));
                pos[1] = y & msk;

                assert(--retry > 0);
#if 1	/* for debug */
                tbl->retry_hash += 1;
#endif
        }
        pos[0] -= 1;
        pos[1] -= 1;

        bk_pp[0] = &tbl->buckets[pos[0]];
        bk_pp[1] = &tbl->buckets[pos[1]];

        prefetch(bk_pp[0]);
        prefetch(bk_pp[1]);
}

/**
 * @brief make free space
 *
 * @param tbl: hash table pointer
 * @param src_bk: full entry bucket
 * @param depth: Number of layers to go back by recursion
 * @return an empty position, if failed then negative
 */
static inline int
cuckoo_replace(struct dcht_hash_table_s * tbl,
               struct dcht_bucket_s * bk,
               int depth)
{
        struct dcht_bucket_s * another[DCHT_BUCKET_ENTRY_SZ];

        /* setup & prefetch */
        for (int i = 0; i < (int) DCHT_BUCKET_ENTRY_SZ; i += 1) {
                struct dcht_bucket_s * bk_p[2];

                buckets_fetch(tbl, bk_p, bk->key[i]);

                if (bk_p[0] == bk)
                        another[i] = bk_p[1];
                else
                        another[i] = bk_p[0];
        }

        /* check vacancy */
        for (int i = 0; i < (int) DCHT_BUCKET_ENTRY_SZ; i += 1) {
                int pos = find_vacancy(another[i]);

                if (pos >= 0) {
                        /* move bk(i) -> another(pos) */
                        move_entry(another[i], pos, bk, i);
                        NOTIFY_CB(tbl, bk, i, DCHT_EVENT_MOVED_ENTRY, 1);
                        return i;
                }
        }

        /* make vacancy under bucket */
        if (depth > 0) {
                for (int i = 0; i < (int) DCHT_BUCKET_ENTRY_SZ; i += 1) {
                        int pos = cuckoo_replace(tbl, another[i], depth - 1);

                        if (pos >= 0) {
                                move_entry(another[i], pos, bk, i);
                                NOTIFY_CB(tbl, bk, i, DCHT_EVENT_MOVED_ENTRY, 1);
                                return i;
                        }
                }
        }

        return -1;
}

/************************************************************************
 * supported hash table API
 ************************************************************************/
size_t
dcht_hash_table_size(unsigned max_entries)
{
        if (max_entries < DCHT_NB_ENTRIES_MIN)
                max_entries = DCHT_NB_ENTRIES_MIN;
        unsigned nb_buckets = align64pow2(max_entries) >> 2;
        size_t size = sizeof(struct dcht_bucket_s) * nb_buckets;

        assert(sizeof(struct dcht_hash_table_s) ==  sizeof(struct dcht_bucket_s));

        TRACER("max:%u size:%zu\n", max_entries, size);
        return size;
}

void
dcht_hash_clean(struct dcht_hash_table_s * tbl)
{
        for (unsigned i = 0; i < tbl->nb_buckets - 1; i++) {
                prefetch(&tbl->buckets[i + 1]);
                BUCKET_INIT(&tbl->buckets[i]);
        }
        BUCKET_INIT(&tbl->buckets[tbl->nb_buckets - 1]);
        tbl->current_entries = 0;
        TRACER("cleaned tbl:%p\n", tbl);
}

int
dcht_hash_table_init(struct dcht_hash_table_s * tbl,
                     size_t size,
                     unsigned max_entries)
{
        unsigned nb_buckets = 0;
        int ret = -1;

        if (tbl) {
                if ((uintptr_t) tbl % DCHT_CACHELINE_SIZE != 0) {
                        /* invalid pointer alignment */
                        TRACER("Bad pointer alignment\n");
                        goto end;
                }
                if (size < dcht_hash_table_size(max_entries)) {
                        /* too small */
                        TRACER("Too small table size:%zu\n", size);
                        goto end;
                }
                if (max_entries < DCHT_NB_ENTRIES_MIN)
                        max_entries = DCHT_NB_ENTRIES_MIN;
                nb_buckets = align64pow2(max_entries) >> 2;

                memset(tbl, 0, sizeof(*tbl));

                tbl->nb_buckets   = nb_buckets - 1;	/* subtract header */
                tbl->mask         = nb_buckets - 1;
                tbl->size         = size;
                tbl->max_entries  = max_entries;
                tbl->nb_entries   = tbl->nb_buckets * DCHT_BUCKET_ENTRY_SZ;
                tbl->follow_depth = DCHT_FOLLOW_DEPTH_DEFAULT;

                dcht_hash_clean(tbl);
                ret = 0;
        }
 end:
        TRACER("ret:%d tbl:%p size:%zu max_entries:%u nb_buckets:%u nb_entires:%u\n",
               ret, tbl, size, max_entries, tbl->nb_buckets, tbl->nb_entries);
        return ret;
}

struct dcht_hash_table_s *
dcht_hash_table_create(unsigned max_entries)
{
        size_t size = dcht_hash_table_size(max_entries);
        struct dcht_hash_table_s * tbl = aligned_alloc(DCHT_CACHELINE_SIZE, size);

        if (dcht_hash_table_init(tbl, size, max_entries)) {
                free(tbl);
                tbl = NULL;
        }
        return tbl;
}

void
dcht_hash_buckets_prefetch(struct dcht_hash_table_s * tbl,
                           uint32_t key,
                           struct dcht_bucket_s ** bk_p)
{
        buckets_fetch(tbl, bk_p, key);
        TRACER("prefetched key:%u %p %p\n", key, bk_p[0], bk_p[1]);
}

int
dcht_hash_find_in_buckets(uint32_t key,
                          struct dcht_bucket_s ** bk_p,
                          uint32_t * val_p)
{
        int ret = FIND_VAL_IN_BUCKET_PAIR_SYNC(bk_p, key, val_p);

        TRACER("ret:%d key:%u bk:%p %p val:%u\n",
               ret, key, bk_p[0], bk_p[1], *val_p);
        return ret;
}

int
dcht_hash_find(struct dcht_hash_table_s * tbl,
               uint32_t key,
               uint32_t * val_p)
{
        struct dcht_bucket_s * bk_p[2];

        buckets_fetch(tbl, bk_p, key);

        return (dcht_hash_find_in_buckets(key, bk_p, val_p) < 0 ? -1 : 0);
}

int
dcht_hash_add_in_buckets(struct dcht_hash_table_s * tbl,
                         struct dcht_bucket_s ** bk_p,
                         uint32_t key,
                         uint32_t val,
                         bool skip_update)
{
        /* check update */
        if (skip_update) {
                int pos;
                int i = FIND_KEY_IN_BUCKET_PAIR(bk_p, key, &pos);
                if (i >= 0) {
                        /* find key, update */
                        store_key_val(bk_p[i], pos, key, val);

                        NOTIFY_CB(tbl, bk_p[i], pos, DCHT_EVENT_UPDATE_VALUE, 1);
                        TRACER("update ret:%d key:%u val:%u bk_p[0]:%p bk_p[1]:%p\n",
                               i, key, val, bk_p[0], bk_p[1]);

                        return i;
                }
        }

        /* check add */
        {
                int i = WHICH_ONE_MOST(bk_p, DCHT_UNUSED_KEY);
                if (i >= 0) {
                        int pos = find_vacancy(bk_p[i]);

                        /* find vacancy pos */
                        store_key_val(bk_p[i], pos, key, val);
                        tbl->current_entries += 1;

                        NOTIFY_CB(tbl, bk_p[i], pos, DCHT_EVENT_BUCKET_FULL,
                                  pos == (DCHT_BUCKET_ENTRY_SZ - 1));
                        TRACER("add ret:%d key:%u val:%u bk_p[0]:%p bk_p[1]:%p\n",
                               i, key, val, bk_p[0], bk_p[1]);
                        return i;
                }
        }

        /* replaced bucket */
        for (int i = 0; i < 2; i++) {
                struct dcht_bucket_s * bk = bk_p[i];
                int pos;

                if ((pos = cuckoo_replace(tbl, bk, tbl->follow_depth)) >= 0) {
                        NOTIFY_CB(tbl, bk, pos, DCHT_EVENT_CUCKOO_REPLACED, 1);

                        /* find free space */
                        store_key_val(bk, pos, key, val);
                        tbl->current_entries += 1;

                        TRACER("replaced ret:%d key:%u val:%u bk_p[0]:%p bk_p[1]:%p\n",
                               i, key, val, bk_p[0], bk_p[1]);
                        return i;
                }
        }

        TRACER("failed key:%u val:%u bk_p[0]:%p bk_p[1]:%p\n",
               key, val, bk_p[0], bk_p[1]);

        return -1;
}

int
dcht_hash_add(struct dcht_hash_table_s * tbl,
              uint32_t key,
              uint32_t val,
              bool skip_update)
{
         struct dcht_bucket_s * bk_p[2];

         buckets_fetch(tbl, bk_p, key);

         return (dcht_hash_add_in_buckets(tbl, bk_p, key, val, skip_update) < 0 ? -1 : 0);
}

int
dcht_hash_del_in_buckets(struct dcht_hash_table_s * tbl,
                         struct dcht_bucket_s ** bk_p,
                         uint32_t key)
{
        int pos = -1;
        int ret = FIND_KEY_IN_BUCKET_PAIR(bk_p, key, &pos);

        if (ret >= 0) {
                del_key(bk_p[ret], pos);
                assert(tbl->current_entries > 0);
                tbl->current_entries -= 1;
        }

        TRACER("ret:%d key:%u pos:%d\n", ret, key, pos);
        return ret;
}

int
dcht_hash_del(struct dcht_hash_table_s * tbl,
              uint32_t key)
{
        struct dcht_bucket_s * bk_p[2];

        buckets_fetch(tbl, bk_p, key);

        return dcht_hash_del_in_buckets(tbl, bk_p, key) >= 0 ? 0 : -1;
}

int
dcht_hash_walk(struct dcht_hash_table_s * tbl,
               int (*bucket_cb)(void *, const struct dcht_bucket_s *),
               void * arg)
{
        int ret = 0;
        unsigned i;

        for (i = 0; !ret && i < tbl->nb_buckets; i++) {
                struct dcht_bucket_s * bk = &tbl->buckets[i];

                if (NB_KEYS_IN_BUCKET(bk, DCHT_UNUSED_KEY) != DCHT_BUCKET_ENTRY_SZ)
                        ret = bucket_cb(arg, bk);
        }

        TRACER("ret:%d loop:%u\n", ret, i);
        return ret;
}

unsigned
dcht_hash_bucket_keys_nb(const struct dcht_bucket_s * bk)
{
        return DCHT_BUCKET_ENTRY_SZ - NB_KEYS_IN_BUCKET(bk, DCHT_UNUSED_KEY);
}

/***************************************************************************
 * unit test
 ***************************************************************************/
int
dcht_hash_utest(struct dcht_hash_table_s * tbl)
{
        struct dcht_bucket_s * bk_p[2], * bk;
        int pos, w;
        uint32_t key;
        uint32_t val;
        unsigned n;

        dcht_hash_clean(tbl);

        bk_p[0] = &tbl->buckets[0];
        bk_p[1] = &tbl->buckets[1];
        bk = bk_p[0];

        /* init test */
        memset(bk, ~(DCHT_UNUSED_KEY), sizeof(*bk));
        BUCKET_INIT(bk);
        key = DCHT_UNUSED_KEY;
        for (unsigned i = 0; i < ARRAYOF(bk->key); i++) {
                if (load_key(bk, i) != key) {
                        TRACER("failed in BUCKET_INIT() %u\n", i);
                        return -1;
                }
        }

        /* not found test */
        BUCKET_INIT(bk);
        key = ~DCHT_UNUSED_KEY;
        pos = FIND_KEY_IN_BUCKET(bk, key);
        if (pos >= 0) {
                TRACER("failed at \"not found test\" pos:%d\n", pos);
                return -1;
        }

        /* key search test in bucket */
        key = ~DCHT_UNUSED_KEY;
        for (int i = 0; i < (int) ARRAYOF(bk->key); i++) {
                BUCKET_INIT(bk);
                store_key(bk, i, key);

                pos = FIND_KEY_IN_BUCKET(bk, key);
                if (pos != i) {
                        TRACER("failed at KEY search test.%u\n", i);
                        return -1;
                }
        }

        /* bug search test */
        BUCKET_INIT(bk);
        key = ~DCHT_UNUSED_KEY;
        store_key(bk, 0, key);
        store_key(bk, DCHT_BUCKET_ENTRY_SZ - 1, key);
        pos = FIND_KEY_IN_BUCKET(bk, key);
        if (pos != 0) {
                TRACER("failed at Bug KEY search test. pos:%d\n", pos);
                return -1;
        }

        /* key search in bucket pair test */
        key = ~DCHT_UNUSED_KEY;
        pos = -1;
        for (int j = 0; j < 2; j++) {
                bk = bk_p[j];
                for (int i = 0; i < (int) ARRAYOF(bk->key); i++) {
                        TRACER("key search in bucket pair test. j:%d i:%d\n", j, i);
                        BUCKET_INIT(bk_p[0]);
                        BUCKET_INIT(bk_p[1]);

                        store_key(bk, i, key);

                        w = FIND_KEY_IN_BUCKET_PAIR(bk_p, key, &pos);
                        if (w != j || pos != i) {
                                TRACER("failed at KEY search in buckets pair. w:%d pos:%d\n", w, pos);
                                return -1;
                        }
                }
        }

        /* not found test in bucket pair */
        BUCKET_INIT(bk_p[0]);
        BUCKET_INIT(bk_p[1]);
        key = ~DCHT_UNUSED_KEY;
        pos = -1;
        w = FIND_KEY_IN_BUCKET_PAIR(bk_p, key, &pos);
        if (w >= 0) {
                TRACER("failed at \"not found bucket pair test\" w:%d\n", w);
                return -1;
        }

        /* val search in bucket pair test */
        for (unsigned j = 0; j < 2; j++) {
                bk = bk_p[j];
                for (unsigned i = 0; i < ARRAYOF(bk->val); i++)
                        store_val(bk, i, 100 + j*10 + i);
        }
        key = ~DCHT_UNUSED_KEY;
        val = ~0;
        for (int j = 0; j < 2; j++) {
                bk = bk_p[j];
                for (int i = 0; i < (int) ARRAYOF(bk->key); i++) {
                        BUCKET_INIT(bk_p[0]);
                        BUCKET_INIT(bk_p[1]);

                        store_key(bk, i, key);
                        int w = FIND_VAL_IN_BUCKET_PAIR_SYNC(bk_p, key, &val);
                        if (w != j) {
                                TRACER("failed at VAL search in buckets pair. w:%d\n", w);
                                return -1;
                        } else if (val != (uint32_t) (100 + j*10 + i)) {
                                TRACER("failed at VAL search in buckets pair. val:%u\n", val);
                                return -1;
                        }
                }
        }

        /* not found test in bucket pair sync */
        key = ~DCHT_UNUSED_KEY;
        BUCKET_INIT(bk_p[0]);
        BUCKET_INIT(bk_p[1]);

        w = FIND_VAL_IN_BUCKET_PAIR_SYNC(bk_p, key, &val);
        if (w >= 0) {
                TRACER("failed at VAL search in buckets pair. w:%d\n", w);
                return -1;
        }

        /* counting test */
        BUCKET_INIT(bk_p[0]);
        BUCKET_INIT(bk_p[1]);
        bk = bk_p[0];
        key = ~DCHT_UNUSED_KEY;
        store_key(bk, 0, key);
        store_key(bk, 1, key);
        n = NB_KEYS_IN_BUCKET(bk, key);
        if (n != 2) {
                TRACER("failed at counting test. n:%u\n", n);
                return -1;
        }

        /* which the one with more key matches */
        BUCKET_INIT(bk_p[0]);
        BUCKET_INIT(bk_p[1]);
        key = ~DCHT_UNUSED_KEY;
        w = WHICH_ONE_MOST(bk_p, key);
        if (w >= 0) {
                TRACER("failed at more key matches test. w:%d\n", w);
                return -1;
        }
        store_key(bk_p[0], 0, key);
        store_key(bk_p[1], 0, key);
        store_key(bk_p[1], 1, key);
        w = WHICH_ONE_MOST(bk_p, key);
        if (w != 1) {
                TRACER("failed at more key matches test. w:%d\n", w);
                return -1;
        }

        dcht_hash_clean(tbl);

        TRACER("All Ok.\n\n");
        return 0;
}
