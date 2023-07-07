/*
 * spec:
 * (1) single writer thread, multi reader thread.
 * (2) lock free
 * (3) x86_64 only, must be ready _mm_sfence(), _mm_lfence(), _mm_crc32_u32()
 * (4) support add, del, search API
 *
 */

#include <immintrin.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>

#include "dc_hash_tbl.h"

#if 0
# define TRACER(fmt,...)	fprintf(stderr, "%s():%d " fmt, __func__, __LINE__, __VA_ARGS__)
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

/*****************************************************************************
 * x86_64 depened code start--->
 *****************************************************************************/
/*
 * @brief wrtie barrier
 */
static inline void
mem_barrier_write(void)
{
        _mm_sfence();
}

/*
 * @brief read barrier
 */
static inline void
mem_barrier_read(void)
{
        _mm_lfence();
}

/*
 * @brief read barrier
 */
static inline uint32_t
crc32c32(uint32_t init,
         uint32_t val)
{
        return _mm_crc32_u32(init, val);
}

/*
 * @brief read barrier
 */
static inline uint32_t
crc32c64(uint32_t init,
         uint64_t val)
{
        return _mm_crc32_u64(init, val);
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
 * @brief prefetch memory (non temporal)
 *
 * @param memory pointer
 * @return void
 */
static inline void
prefetch(const volatile void *p)
{
        asm volatile ("prefetchnta %[p]" : : [p] "m" (*(const volatile char *)p));
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

/*****************************************************************************
 * <---end x86 depened code
 *****************************************************************************/

/**
 * @brief read bucket state
 *
 * @param bk: bucket
 * @param st: Pointer to set the read state
 * @return Returns null if the state is the same as the previous read.
 */
static inline union dcht_bucket_state_u *
read_state(const struct dcht_bucket_s * bk,
           union dcht_bucket_state_u * st)
{
        if (st) {
                mem_barrier_read();

                uint32_t prev = *(volatile uint32_t *) &st->state32;

                st->state32 = bk->state.state32;
                if (st->state32 == prev)
                        st = NULL;
        }
        return st;
}

/**
 * @brief write bucket state
 *
 * @param bk: bucket
 * @param st: Pointer to set the write state
 * @return void
 */
static inline void
write_state(struct dcht_bucket_s * bk,
            union dcht_bucket_state_u * st)
{
        mem_barrier_write();

        st->sequence += 1;
        *(volatile uint32_t *) &bk->state.state32 = st->state32;
}

/**
 * @brief find key position
 *
 * @param bk: bucket
 * @param key: search key
 * @return Returns the position where key was found.
 *         Returns negative if not found.
 */
static inline int
find_key_pos(const struct dcht_bucket_s * bk,
             uint32_t key)
{
        int pos = 0;

        for (uint32_t flags = bk->state.validities; flags; flags >>= 1) {
                if ((flags & 1u) && (bk->key[pos] == key))
                        return pos;
                pos += 1;
        }

        return -1;
}

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
        int pos = 0;

        for (uint32_t flags = bk->state.validities; flags & 1u; flags >>= 1)
                pos += 1;

        return (pos >= (int) DCHT_BUCKET_ENTRY_SZ) ? -1 : pos;
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
        return (bk->state.validities == DCHT_BUCKET_FULL);
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
        return (bk->state.validities & (1u << pos)) ? true : false;
}

/**
 * @brief wrtie key and value pair
 *
 * @param bk: bucket
 * @param key: key
 * @param val: value
 * @return write position, if nothing vacancy then negative
 */
static inline int
set_key_val(struct dcht_bucket_s * bk,
            int pos,
            uint32_t key,
            uint32_t val)
{
        assert(0 <= pos && pos < (int) DCHT_BUCKET_ENTRY_SZ);

        union dcht_bucket_state_u st;

        read_state(bk, &st);

        bk->key[pos] = key;
        bk->val[pos] = val;

        st.validities |= (1u << pos);
        write_state(bk, &st);

        return pos;
}

/**
 * @brief delete key
 *
 * @param bk: bucket
 * @param pos: delete position
 * @return successed then deleted position, failed then negative
 */
static inline int
del_key(struct dcht_bucket_s * bk,
        int pos)
{

        if (pos < 0 || pos >= (int) DCHT_BUCKET_ENTRY_SZ) {
                pos = -1;
        } else {
                union dcht_bucket_state_u st;
                read_state(bk, &st);

                st.validities &= ~(1u << pos);
                write_state(bk, &st);
        }
        return pos;
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

        set_key_val(dbk, dpos, key, val);
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
        union dcht_bucket_state_u st;
        union dcht_bucket_state_u * st_p = &st;
        int pos;

        read_state(bk, &st);

        do {
                pos = find_key_pos(bk, key);
                if (pos >= 0)
                        *val_p = *(volatile uint32_t *) &bk->val[pos];

                /* Reread if state is updated during search */
        } while ((st_p = read_state(bk, st_p)) != NULL);

        return pos;
}

/**
 * @brief initialize bucket (unused)
 *
 * @param bk: bucket
 * @return void
 */
static inline void
bucket_init(struct dcht_bucket_s * bk)
{
        union dcht_bucket_state_u st;

        st.state32 = bk->state.state32;
        st.sequence += 1;
        st.validities = 0;

        bk->state.state32 = st.state32;
        mem_barrier_write();
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

        x = crc32c32(0xdeadbeef, key);
        x = crc32c32(x, bswap32(key));

        y = bswap32(key ^ x);

        pos[0] = x & msk;
        pos[1] = y & msk;

        while (pos[0] == pos[1]) {
                y = crc32c32(y, ~bswap32(key));
                pos[1] = y & msk;

#if 0	/* for debug */
                tbl->retry_hash += 1;
#endif
        }

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
        size_t size = sizeof(struct dcht_hash_table_s) + sizeof(struct dcht_bucket_s) * nb_buckets;

        return size;
}

struct dcht_hash_table_s *
dcht_hash_table_init(struct dcht_hash_table_s * tbl,
                     size_t size,
                     unsigned max_entries)
{
        if (tbl) {
                if (max_entries < DCHT_NB_ENTRIES_MIN)
                        max_entries = DCHT_NB_ENTRIES_MIN;
                unsigned nb_buckets = align64pow2(max_entries) >> 2;

                memset(tbl, 0, size);

                tbl->nb_buckets = nb_buckets;
                tbl->nb_entries = nb_buckets * DCHT_BUCKET_ENTRY_SZ;
                tbl->max_entries = max_entries;
                tbl->mask = nb_buckets - 1;
                tbl->follow_depth = DCHT_FOLLOW_DEPTH_DEFAULT;

                mem_barrier_write();
        }
        return tbl;
}

struct dcht_hash_table_s *
dcht_hash_table_create(unsigned max_entries)
{
        size_t size = dcht_hash_table_size(max_entries);

        return dcht_hash_table_init(aligned_alloc(DCHT_CACHELINE_SIZE, size), size, max_entries);
}

void
dcht_hash_clean(struct dcht_hash_table_s * tbl)
{
        for (unsigned i = 0; i < tbl->nb_buckets; i++) {
                prefetch(&tbl->buckets[i+1]);
                bucket_init(&tbl->buckets[i]);
        }
        tbl->current_entries = 0;
}

void
dcht_hash_buckets_prefetch(struct dcht_hash_table_s * tbl,
                           uint32_t key,
                           struct dcht_bucket_s ** bk_p)
{
        buckets_fetch(tbl, bk_p, key);
}

int
dcht_hash_find_in_buckets(uint32_t key,
                          struct dcht_bucket_s ** bk_p,
                          uint32_t * val_p)
{
        union dcht_bucket_state_u st_v[2];
        int ret;
        uintptr_t or_p;

        TRACER("start key:%u #0:%p #1:%p\n", key, bk_p[0], bk_p[1]);

        read_state(bk_p[0], &st_v[0]);
        read_state(bk_p[1], &st_v[1]);

        do {
                TRACER("#0 validity:%0x sequunce:%u #1 validity:%0x sequunce:%u\n",
                       st_v[0].validities, st_v[0].sequence,
                       st_v[1].validities, st_v[1].sequence);

                ret = -1;

                for (int i = 0; i < 2; i++) {
                        int pos = find_key_pos(bk_p[i], key);

                        if (pos >= 0) {
                                /* found */
                                uint32_t val = bk_p[i]->val[pos];

                                /* check state */
                                if (!read_state(bk_p[i], &st_v[i])) {
                                        *val_p = val;
                                        ret = i;
                                        goto end;
                                }
                        }
                }

                or_p  = (uintptr_t) read_state(bk_p[0], &st_v[0]);
                or_p |= (uintptr_t) read_state(bk_p[1], &st_v[1]);

        } while (or_p);

 end:
        TRACER("done. key:%u ret:%d\n", key, ret);
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
                         uint32_t val)
{
        /* check update */
        for (int i = 0; i < 2; i++) {
                 struct dcht_bucket_s * bk = bk_p[i];
                 int pos;

                 if ((pos = find_key_pos(bk, key)) >= 0) {
                         /* find key, update */
                         set_key_val(bk, pos, key, val);

                         NOTIFY_CB(tbl, bk, pos, DCHT_EVENT_UPDATE_VALUE, 1);
                         return i;
                 }
        }

        /* check add */
        {
                int ret;

                if ((popcnt(bk_p[0]->state.validities)) <= (popcnt(bk_p[1]->state.validities)))
                        ret = 0;
                else
                        ret = 1;

                struct dcht_bucket_s * bk = bk_p[ret];
                int pos;

                if ((pos = find_vacancy(bk)) >= 0) {
                        /* find free space */
                        set_key_val(bk, pos, key, val);
                        tbl->current_entries += 1;

                        NOTIFY_CB(tbl, bk, pos, DCHT_EVENT_BUCKET_FULL,
                                  bk->state.validities == DCHT_BUCKET_FULL);
                        return ret;
                }
        }

        /* replaced bucket */
        for (int i = 0; i < 2; i++) {
                struct dcht_bucket_s * bk = bk_p[i];
                int pos;

                if ((pos = cuckoo_replace(tbl, bk, tbl->follow_depth)) >= 0) {
                        NOTIFY_CB(tbl, bk, pos, DCHT_EVENT_CUCKOO_REPLACED, 1);

                        /* find free space */
                        set_key_val(bk, pos, key, val);
                        tbl->current_entries += 1;
                        return i;
                }
        }
        return -1;
}

int
dcht_hash_add(struct dcht_hash_table_s * tbl,
              uint32_t key,
              uint32_t val)
{
         struct dcht_bucket_s * bk_p[2];

         buckets_fetch(tbl, bk_p, key);

         return (dcht_hash_add_in_buckets(tbl, bk_p, key, val) < 0 ? -1 : 0);
}

int
dcht_hash_del_in_buckets(struct dcht_hash_table_s * tbl,
                         struct dcht_bucket_s ** bk_p,
                         uint32_t key)
{
        for (int i = 0; i < 2; i++) {
                if (del_key(bk_p[i], find_key_pos(bk_p[i], key)) >= 0) {
                        assert(tbl->current_entries > 0);
                        tbl->current_entries -= 1;
                        return i;
                }
        }
        return -1;
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

        for (unsigned i = 0; !ret && i < tbl->nb_buckets; i++) {
                struct dcht_bucket_s * bk = &tbl->buckets[i];

                if (bk->state.validities)
                        ret = bucket_cb(arg, bk);
        }

        return ret;
}

