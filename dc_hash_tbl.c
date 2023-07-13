/*
 * Copyright (c) 2023 deadcafe.beef@gmail.com
 *
 * cuckoo hash table
 * (1) single writer thread, multi reader thread.
 * (2) lock free
 * (3) support add, del, search API
 * (4) Zero cannot be used for Key
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

#if defined(ENABLE_HASH_TRACER)
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
        int ret = -ENOENT;

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
        __builtin_prefetch(p, 0, 3);	/* non temporal */
}

/*
 * handler for each CPU Arch
 */
struct arch_handler_s {
        uint32_t (*hash32)(uint32_t,uint32_t);		/* 32 bit hash generator */
        void (*bk_init)(struct dcht_bucket_s *);	/* bucket initializer */
        int (*find_key_bk)(const struct dcht_bucket_s *,
                           uint32_t);			/* find key in bucket */
        int (*find_key_bk_pair)(struct dcht_bucket_s **,
                                uint32_t, int *);	/* find key in buckets pair */
        unsigned (*nb_keys_bk)(const struct dcht_bucket_s *,
                               uint32_t);		/* number of keys in a bucket */
        int (*which_one_most_bk)(struct dcht_bucket_s **,
                                 uint32_t, unsigned *);	/* which the one with more key matches in buckets pair */
        int (*find_val_bk_pair_sync)(struct dcht_bucket_s **,
                                     uint32_t,
                                     uint32_t *);	/* find val in buckets pair sync */

};

/*****************************************************************************
 * start Generic Arch code--->
 *****************************************************************************/
/*
 * @brief FNV-1a hash
 */
static inline uint32_t
fnv1a(uint32_t init,
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
        pos = -ENOENT;
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
        return -ENOENT;
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
                   uint32_t key,
                   unsigned * nb_p)
{
        int ret;

        TRACER("K0 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[0]->key[0], bk_p[0]->key[1], bk_p[0]->key[2], bk_p[0]->key[3],
               bk_p[0]->key[4], bk_p[0]->key[5], bk_p[0]->key[6], bk_p[0]->key[7]);
        TRACER("K1 %08x %08x %08x %08x %08x %08x %08x %08x\n",
               bk_p[1]->key[0], bk_p[1]->key[1], bk_p[1]->key[2], bk_p[1]->key[3],
               bk_p[1]->key[4], bk_p[1]->key[5], bk_p[1]->key[6], bk_p[1]->key[7]);

        nb_p[0] = 0;
        nb_p[1] = 0;

        for (int i = 0; i < 2; i++) {
                for (int pos = 0; pos < (int) DCHT_BUCKET_ENTRY_SZ; pos++) {
                        if (load_key(bk_p[i], pos ) == key)
                                nb_p[i] += 1;
                }
        }

        if (nb_p[0] >= nb_p[1])
                ret = 0;
        else
                ret = 1;

        if (!nb_p[ret])
                ret = -ENOENT;

        TRACER("key:%u ret:%d n0:%d n1:%d\n", key, ret, nb_p[0], nb_p[1]);
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
        return -ENOENT;
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
                store_key(bk, pos, DCHT_SENTINEL_KEY);
}

static const struct arch_handler_s generic_handlers = {
        .hash32 = fnv1a,
        .bk_init = bucket_init_GEN,
        .find_key_bk = find_key_in_bucket_GEN,
        .find_key_bk_pair = find_key_in_bucket_pair_GEN,
        .nb_keys_bk = number_of_keys_in_bucket_GEN,
        .which_one_most_bk = which_one_most_GEN,
        .find_val_bk_pair_sync = find_key_val_in_bucket_pair_sync_GEN,
};

/*****************************************************************************
 * <---end Generic Arch code
 *****************************************************************************/

static const struct arch_handler_s * arch_handler = &generic_handlers;

#define BSWAP(_v)					__builtin_bswap32((_v))
#define HASH(_i,_v)					arch_handler->hash32((_i),(_v))
#define FIND_KEY_IN_BUCKET(_bk,_key)			arch_handler->find_key_bk((_bk),(_key))
#define FIND_KEY_IN_BUCKET_PAIR(_bk_p,_key,_pos_p)	arch_handler->find_key_bk_pair((_bk_p),(_key),(_pos_p))
#define	NB_KEYS_IN_BUCKET(_bk,_key)			arch_handler->nb_keys_bk((_bk),(_key))
#define WHICH_ONE_MOST(_bk_p,_key,_nb_p)		arch_handler->which_one_most_bk((_bk_p),(_key),(_nb_p))
#define	FIND_VAL_IN_BUCKET_PAIR_SYNC(_bk_p,_key,_val_p)	arch_handler->find_val_bk_pair_sync((_bk_p),(_key),(_val_p))
#define	BUCKET_INIT(_bk)				arch_handler->bk_init((_bk))


#if defined(__x86_64__)
/*****************************************************************************
 * x86_64 depened code start--->
 *****************************************************************************/
#include <immintrin.h>
#include <cpuid.h>

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
        int pos = -ENOENT;

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
        return -ENOENT;
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
                    uint32_t key,
                    unsigned * nb_p)
{
        __m256i search_key = _mm256_set1_epi32(key);
        int ret;

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

                nb_p[i] = __builtin_popcount(mask);
        }

        if (nb_p[0] >= nb_p[1])
                ret = 0;
        else
                ret = 1;

        if (!nb_p[ret])
                ret = -ENOENT;

        TRACER("key:%u ret:%d n0:%u n1:%u\n", key, ret, nb_p[0], nb_p[1]);
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

        return -ENOENT;
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
        __m256i search_key = _mm256_set1_epi32(DCHT_SENTINEL_KEY);
        _mm256_store_si256((__m256i *) (volatile void *) bk->key, search_key);
        __sync_synchronize();
}

/**
 * @brief crc32c calc
 *
 * @param initial value
 * @param target value
 * @return crc32c
 */
static inline uint32_t
crc32c32(uint32_t init,
         uint32_t val)
{
        return _mm_crc32_u32(init, val);
}

static const struct arch_handler_s x86_avx2_handlers = {
        .hash32                = crc32c32,
        .bk_init               = bucket_init_AVX2,
        .find_key_bk           = find_key_in_bucket_AVX2,
        .find_key_bk_pair      = find_key_in_bucket_pair_AVX2,
        .nb_keys_bk            = number_of_keys_in_bucket_AVX2,
        .which_one_most_bk     = which_one_most_AVX2,
        .find_val_bk_pair_sync = find_key_val_in_bucket_pair_sync_AVX2,
};

/*
 * check cpuid AVX2,BMI,SSE4_2(crc32c)
 */
const struct arch_handler_s *
 __attribute__((weak)) x86_handler_get(void)
{
        const struct arch_handler_s * handler = arch_handler;

#ifndef	DISABLE_AVX2_DRIVER
        uint32_t eax = 0, ebx, ecx, edx;

        // Get the highest function parameter.
        __get_cpuid(0, &eax, &ebx, &ecx, &edx);

        // Check if the function parameter for extended features is available.
        if (eax >= 7) {
                __cpuid_count(1, 0, eax, ebx, ecx, edx);
                if (!(ecx & bit_SSE4_2))
                        goto end;

                __cpuid_count(7, 0, eax, ebx, ecx, edx);
                if (!(ebx & bit_AVX2))
                        goto end;
                if (!(ebx & bit_BMI))
                        goto end;

                /* All Ok */
                TRACER("use X86_64 AVX2 cuckoo hash driver\n");
                handler = &x86_avx2_handlers;
        } else {
 end:
                TRACER("use generic cuckoo hash driver\n");
        }
#else	/* !DISABLE_AVX2_DRIVER */
        (void) &x86_avx2_handlers;
#endif	/* DISABLE_AVX2_DRIVER */
        return handler;
}

/*****************************************************************************
 * <---end x86 depened code
 *****************************************************************************/
#endif	/* __x86_64__ */


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
        return FIND_KEY_IN_BUCKET(bk, DCHT_SENTINEL_KEY);
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
        return (bk->key[pos] != DCHT_SENTINEL_KEY);
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

        store_key(bk, pos, DCHT_SENTINEL_KEY);
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
        int pos = -ENOENT;

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

        x = HASH(0xdeadbeef, key);
        x = HASH(x, BSWAP(key));
        pos[0] = x & msk;
        while (!pos[0]) {
                x = HASH(x, key);
                pos[0] = x & msk;

                assert(--retry > 0);
 #if 1	/* for debug */
                tbl->retry_hash += 1;
#endif
       }
        retry = 10;

        y = BSWAP(key ^ x);
        pos[1] = y & msk;
        while (pos[0] == pos[1] || !pos[1]) {
                y = HASH(y, ~BSWAP(key));
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

        return -ENOSPC;
}

/*
 * max buckets + 1
 */
static inline unsigned
nb_bcuckets(unsigned nb_entries)
{
        if (nb_entries < DCHT_NB_ENTRIES_MIN)
                nb_entries = DCHT_NB_ENTRIES_MIN;
        unsigned nb_buckets = align64pow2(nb_entries * 1.27) / DCHT_BUCKET_ENTRY_SZ;	/* full rate 80% */

        TRACER("nb buckets:%u\n", nb_buckets);
        return nb_buckets;
}

/************************************************************************
 * supported hash table API
 ************************************************************************/
size_t
dcht_hash_table_size(unsigned max_entries)
{
        unsigned nb_buckets = nb_bcuckets(max_entries);
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
        int ret = -EINVAL;

        if (arch_handler == &generic_handlers && x86_handler_get)
                arch_handler = x86_handler_get();

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
                nb_buckets = nb_bcuckets(max_entries);

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

        return (dcht_hash_find_in_buckets(key, bk_p, val_p) < 0 ? -ENOENT : 0);
}

int
dcht_hash_add_in_buckets(struct dcht_hash_table_s * tbl,
                         struct dcht_bucket_s ** bk_p,
                         uint32_t key,
                         uint32_t val,
                         bool skip_update)
{
        if (key == DCHT_SENTINEL_KEY) {
                TRACER("invalid key:%u\n", key);
                return -EINVAL;
        }

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
                unsigned nb[2];
                int i = WHICH_ONE_MOST(bk_p, DCHT_SENTINEL_KEY,nb);
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

        return -ENOSPC;
}

int
dcht_hash_add(struct dcht_hash_table_s * tbl,
              uint32_t key,
              uint32_t val,
              bool skip_update)
{
         struct dcht_bucket_s * bk_p[2];

         buckets_fetch(tbl, bk_p, key);

         return (dcht_hash_add_in_buckets(tbl, bk_p, key, val, skip_update) < 0 ? -ENOSPC : 0);
}

int
dcht_hash_del_in_buckets(struct dcht_hash_table_s * tbl,
                         struct dcht_bucket_s ** bk_p,
                         uint32_t key)
{
        int pos = -EINVAL;
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

        return dcht_hash_del_in_buckets(tbl, bk_p, key) >= 0 ? 0 : -ENOENT;
}

static inline int
_hash_bk_walk(struct dcht_hash_table_s * tbl,
              int (* bucket_cb)(struct dcht_hash_table_s *,
                                const struct dcht_bucket_s *,
                                void *),
              void * arg)
{
        int ret = 0;
        unsigned i;

        for (i = 0; !ret && i < tbl->nb_buckets; i++) {
                struct dcht_bucket_s * bk = &tbl->buckets[i];

                if (NB_KEYS_IN_BUCKET(bk, DCHT_SENTINEL_KEY) != DCHT_BUCKET_ENTRY_SZ)
                        ret = bucket_cb(tbl, bk, arg);
        }

        TRACER("ret:%d loop:%u\n", ret, i);
        return ret;
}

int
dcht_hash_bk_walk(struct dcht_hash_table_s * tbl,
                  int (* bucket_cb)(struct dcht_hash_table_s *,
                                   const struct dcht_bucket_s *,
                                   void *),
               void * arg)
{
        return _hash_bk_walk(tbl, bucket_cb, arg);
}

/*
 *
 */
struct walk_keyval_s {
        int (* func_cb)(struct dcht_hash_table_s *,
                        uint32_t, uint32_t, void *);
        void * arg;
};

static int
_bucket_cb(struct dcht_hash_table_s * tbl,
           const struct dcht_bucket_s * bk,
           void * arg)
{
        struct walk_keyval_s * walk_p = arg;
        int ret = 0;

        for (unsigned i = 0; !ret && i < DCHT_BUCKET_ENTRY_SZ; i++) {
                if (bk->key[i] == DCHT_SENTINEL_KEY)
                        continue;

                ret = walk_p->func_cb(tbl, bk->key[i], bk->val[i], arg);
        }
        return ret;
}

int
dcht_hash_walk(struct dcht_hash_table_s * tbl,
               int (* func_cb)(struct dcht_hash_table_s *,
                               uint32_t, uint32_t,
                               void *),
               void * arg)
{
        struct walk_keyval_s walk;

        walk.func_cb = func_cb;
        walk.arg = arg;
        return _hash_bk_walk(tbl, _bucket_cb, &walk);
}

static int
_bucket_verify_cb(struct dcht_hash_table_s * tbl,
                  const struct dcht_bucket_s * bk,
                  void * arg)
{
        int ret = 0;
        unsigned * nb_p = arg;

        for (int i = 0; i < (int) DCHT_BUCKET_ENTRY_SZ; i++) {
                if (bk->key[i] == DCHT_SENTINEL_KEY)
                        continue;

                uint32_t key = bk->key[i];
                struct dcht_bucket_s * bk_p[2];
                unsigned nb[2];

                /* hash check */
                dcht_hash_buckets_prefetch(tbl, key, bk_p);
                int w = WHICH_ONE_MOST(bk_p, key, nb);
                if (!(w >= 0 && bk == bk_p[w] && nb[w] == 1 && nb[(w + 1) & 1] == 0)) {
                        TRACER("invalid w:%d bk:%p key:%u nb0:%u nb1:%u \n",
                               w, bk, key, nb[0], nb[1]);
                        ret = -EINVAL;
                        break;
                }
                *nb_p += 1;
        }
        return ret;
}

int
dcht_hash_verify(struct dcht_hash_table_s * tbl)
{
        unsigned nb = 0;

        int ret = _hash_bk_walk(tbl, _bucket_verify_cb, &nb);
        if (!ret) {
                if (nb != tbl->current_entries) {
                        TRACER("mismatched number of entries:%u %u\n",
                               tbl->current_entries, nb);
                        ret = -1;
                }
        }
        return ret;
}

unsigned
dcht_hash_bucket_keys_nb(const struct dcht_bucket_s * bk)
{
        return DCHT_BUCKET_ENTRY_SZ - NB_KEYS_IN_BUCKET(bk, DCHT_SENTINEL_KEY);
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
        unsigned nb[2];

        dcht_hash_clean(tbl);

        bk_p[0] = &tbl->buckets[0];
        bk_p[1] = &tbl->buckets[1];
        bk = bk_p[0];

        /* init test */
        memset(bk, ~(DCHT_SENTINEL_KEY), sizeof(*bk));
        BUCKET_INIT(bk);
        key = DCHT_SENTINEL_KEY;
        for (unsigned i = 0; i < ARRAYOF(bk->key); i++) {
                if (load_key(bk, i) != key) {
                        TRACER("failed in BUCKET_INIT() %u\n", i);
                        return -1;
                }
        }

        /* not found test */
        BUCKET_INIT(bk);
        key = ~DCHT_SENTINEL_KEY;
        pos = FIND_KEY_IN_BUCKET(bk, key);
        if (pos >= 0) {
                TRACER("failed at \"not found test\" pos:%d\n", pos);
                return -1;
        }

        /* key search test in bucket */
        key = ~DCHT_SENTINEL_KEY;
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
        key = ~DCHT_SENTINEL_KEY;
        store_key(bk, 0, key);
        store_key(bk, DCHT_BUCKET_ENTRY_SZ - 1, key);
        pos = FIND_KEY_IN_BUCKET(bk, key);
        if (pos != 0) {
                TRACER("failed at Bug KEY search test. pos:%d\n", pos);
                return -1;
        }

        /* key search in bucket pair test */
        key = ~DCHT_SENTINEL_KEY;
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
        key = ~DCHT_SENTINEL_KEY;
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
        key = ~DCHT_SENTINEL_KEY;
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
        key = ~DCHT_SENTINEL_KEY;
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
        key = ~DCHT_SENTINEL_KEY;
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
        key = ~DCHT_SENTINEL_KEY;
        w = WHICH_ONE_MOST(bk_p, key, nb);
        if (w >= 0) {
                TRACER("failed at more key matches test. w:%d\n", w);
                return -1;
        }
        store_key(bk_p[0], 0, key);
        store_key(bk_p[1], 0, key);
        store_key(bk_p[1], 1, key);
        w = WHICH_ONE_MOST(bk_p, key, nb);
        if (w != 1) {
                TRACER("failed at more key matches test. w:%d\n", w);
                return -1;
        }

        dcht_hash_clean(tbl);

        TRACER("All Ok.\n\n");
        return 0;
}
