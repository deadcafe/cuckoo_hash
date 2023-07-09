/*
 * Hash table Spec:
 * (1) single writer thread, multi reader thread.
 * (2) lock free
 * (3) x86_64 only, must be ready _mm_sfence(), _mm_lfence(), _mm_crc32_u32()
 * (4) support initialize, add, delete, search,  walk,
 */

#ifndef _DC_HASH_TBL_H_
#define _DC_HASH_TBL_H_

#include <sys/types.h>
#include <stdint.h>

/*
 * configuration some parameters
 */
#define DCHT_CACHELINE_SIZE		64

/*
 * fixed params
 */
#define DCHT_BUCKET_ENTRY_SZ		(DCHT_CACHELINE_SIZE / sizeof(uint64_t))
#define DCHT_BUCKET_FULL		((1u << DCHT_BUCKET_ENTRY_SZ) - 1)
#define DCHT_NB_ENTRIES_MIN		64
#define DCHT_FOLLOW_DEPTH_DEFAULT	3
#define	DCHT_UNUSED_KEY			0


/*
 * bucket table : must be cacheline size alignment
 */
struct dcht_bucket_s {
        /* entry keys */
        uint32_t key[DCHT_BUCKET_ENTRY_SZ];

        /* entry value */
        uint32_t val[DCHT_BUCKET_ENTRY_SZ];
} __attribute__ ((aligned(DCHT_CACHELINE_SIZE)));

/*
 * Action event for debug
 */
enum dcht_event_e {
        DCHT_EVENT_NONE = -1,

        DCHT_EVENT_BUCKET_FULL = 0,
        DCHT_EVENT_MOVED_ENTRY,
        DCHT_EVENT_CUCKOO_REPLACED,
        DCHT_EVENT_UPDATE_VALUE,

        DCHT_EVENT_NB,
};

/*
 * cuckoo hash table
 */
struct dcht_hash_table_s {
        size_t size;

        unsigned nb_buckets;
        unsigned nb_entries;

        uint32_t mask;
        unsigned max_entries;

        unsigned current_entries;
        int follow_depth;

        unsigned retry_hash;		/* for debug, retry hash counter */
        unsigned _padding;

        /* event notification callback for debug */
        void (*event_notify_cb)(void *,			/* arg */
                                enum dcht_event_e,	/* event type */
                                struct dcht_bucket_s *,	/* bucket */
                                int			/* entry pos in bucket */
                                );
        void * arg;

        struct dcht_bucket_s buckets[] __attribute__ ((aligned(DCHT_CACHELINE_SIZE)));
};

/*************************************************************************************
 * supported hash table API
 *************************************************************************************/
/**
 * @brief Calculate hash table size
 *
 * @param max_entries:Maximum registration number
 * @return Return used memory size
 */
extern size_t dcht_hash_table_size(unsigned max_entries);

/**
 * @brief Initialize the hash table
 *
 * @param tbl: hash table pointer(Must be cacheline size algined)
 * @param size: size of hash table
 * @param max_entries: Maximum registration number
 * @return success then zero, failuer thern negative
 */
extern int dcht_hash_table_init(struct dcht_hash_table_s * tbl,
                                size_t size,
                                unsigned max_entries);

/**
 * @brief create hash table
 *
 * @param max_entries: Maximum number that can be registered
 * @return created hash table pointer
 */
extern struct dcht_hash_table_s * dcht_hash_table_create(unsigned max_entries);

/**
 * @brief release all entries
 *
 * @param tbl: hash table pointer
 * @return void
 */
extern void dcht_hash_clean(struct dcht_hash_table_s * tbl);

/**
 * @brief search bucket#0,#1 and prefetch
 *
 * @param tbl: hash table
 * @param key: search key
 * @param bk_p: bucket#0, bucket#1 pointer array
 * @return void
 */
extern void dcht_hash_buckets_prefetch(struct dcht_hash_table_s * tbl,
                                       uint32_t key,
                                       struct dcht_bucket_s ** bk_p);

/**
 * @brief search key-val in bcucket#0,#1
 *
 * @param key: search key
 * @param bk_p: bucket#0,#1 pointers
 * @param val_p: Pointer to set the read value
 * @return Returns the bucket number where key was found.
 *         Returns negative if not found.
 */
extern int dcht_hash_find_in_buckets(uint32_t key,
                                     struct dcht_bucket_s ** bk_p,
                                     uint32_t * val_p);

/**
 * @brief search key-val in hash table
 *
 * @param tbl: hash table
 * @param key: search key
 * @param val_p: Pointer to set the read value
 * @return found key:0 not found:negative
 */
extern int dcht_hash_find(struct dcht_hash_table_s * tbl,
                          uint32_t key,
                          uint32_t * val_p);

/**
 * @brief search key-val in hash table
 *
 * @param tbl: hash table
 * @param key: search key
 * @param val_p: Pointer to set the read value
 * @return found key:0 not found:negative
 */
extern int dcht_hash_find(struct dcht_hash_table_s * tbl,
                          uint32_t key,
                          uint32_t * val_p);

/**
 * @brief add key and value in bucket #0 or #1
 *
 * @param tbl: hash table
 * @param bk_p: bucket pointer array
 * @param key: key
 * @param val: value
 * @return Returns the bucket number that was successfully added.
 *         Returns negative if it fails.
 */
extern int dcht_hash_add_in_buckets(struct dcht_hash_table_s * tbl,
                                    struct dcht_bucket_s ** bk_p,
                                    uint32_t key,
                                    uint32_t val,
                                    bool skip_update);

/**
 * @brief add key and value in hash table
 *
 * @param tbl: hash table
 * @param key: key
 * @param val: value
 * @return success:0 failed:negative
 */
extern int dcht_hash_add(struct dcht_hash_table_s * tbl,
                         uint32_t key,
                         uint32_t val,
                         bool skip_update);

/**
 * @brief delete key in hash table
 *
 * @param tbl: hash taable
 * @param bk_p: bucket pointer array
 * @param key: deleting key
 * @return Returns the bucket number that was successfully deleted.
 *         Returns negative if not found key.
 */
extern int dcht_hash_del_in_buckets(struct dcht_hash_table_s * tbl,
                                    struct dcht_bucket_s ** bk_p,
                                    uint32_t key);

/**
 * @brief delete key in hash table
 *
 * @param tbl: hash taable
 * @param key: deleting key
 * @return success:0 failed:negative
 */
extern int dcht_hash_del(struct dcht_hash_table_s * tbl,
                         uint32_t key);

/**
 * @brief walk in hash table entries
 *
 * @param tbl: hash table pointer
 * @param bucket_cb: active bucket callback function
 * @param arg: any argument to bucket_cb
 * @return Return the return of bucket_cb
 */
extern int dcht_hash_walk(struct dcht_hash_table_s * tbl,
                          int (*bucket_cb)(void *, const struct dcht_bucket_s *),
                          void * arg);

/**
 * @brief Unit Test in hash table
 *
 * @param tbl: hash table pointer
 * @return All Ok then Zero, else then negative
 */
extern int dcht_hash_utest(struct dcht_hash_table_s * tbl);

/*
 * return number of used keys in bucket
 */
extern unsigned dcht_hash_bucket_keys_nb(const struct dcht_bucket_s * bk);

#endif	/* !_DC_HASH_TBL_H_ */
