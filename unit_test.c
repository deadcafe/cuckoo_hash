/*
 * Unit Test for dc_hash_tbl.c
 */

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>

#include "dc_hash_tbl.h"

/*********************************************************************************
 * Unit Test
 *********************************************************************************/
struct req_s {
        uint32_t key;
        uint32_t val;
};

struct notify_s {
        struct dcht_hash_table_s * tbl;
        struct req_s * req;

        unsigned seq;
        unsigned mask;

        unsigned cnt[DCHT_EVENT_NB];
};

/**
 * @brief Read TSC
 *
 * @return tsc cycles
 */
static inline uint64_t
rdtsc(void)
{
        union {
                uint64_t tsc_64;
                struct {
                        uint32_t lo_32;
                        uint32_t hi_32;
                };
        } tsc;

        asm volatile("rdtsc" :
                     "=a" (tsc.lo_32),
                     "=d" (tsc.hi_32));
        return tsc.tsc_64;
}

#define DCHT_EVENT_BIT(_e)	(1u << (_e))
#define IS_EVENT(_m, _e)	(_m) & (1u << (_e))

static inline unsigned
bucket_id(struct dcht_hash_table_s * tbl,
          struct dcht_bucket_s * bk)
{
        return bk - tbl->buckets;
}

static inline void
bucket_dump(struct dcht_hash_table_s * tbl,
            struct dcht_bucket_s * bk)
{
        fprintf(stderr, "  bk:%p id:%u\n", bk, bucket_id(tbl, bk));

        for (unsigned pos = 0; pos < DCHT_BUCKET_ENTRY_SZ; pos++) {
                if (bk->key[pos] == DCHT_UNUSED_KEY)
                        continue;

                struct dcht_bucket_s * bk_p[2];
                unsigned id[2];

                dcht_hash_buckets_prefetch(tbl, bk->key[pos], bk_p);
                id[0] = bucket_id(tbl, bk_p[0]);
                id[1] = bucket_id(tbl, bk_p[1]);

                fprintf(stderr, "    pos:%u key:%u val:%u id#0:%u id#1:%u\n",
                        pos, bk->key[pos], bk->val[pos],
                        id[0], id[1]);
        }
}

static inline void
table_dump(const char * msg,
           struct dcht_hash_table_s * tbl)
{
        fprintf(stderr, "%s nb_bk:%u nb_ent:%u msk:0x%0x max:%u cur:%u depth:%d FullRate:%.02f%%\n",
                msg,
                tbl->nb_buckets, tbl->nb_entries,
                tbl->mask, tbl->max_entries,
                tbl->current_entries, tbl->follow_depth,
                (double) 100 * tbl->current_entries / tbl->nb_entries
                );
}

static const char *event_msg[] = {
        "Bucket Full",
        "Moved Entry",
        "Cuckoo Replaced",
        "Updated Value",

        "unknown",
};

static inline void
notify_cb(void *arg,
          enum dcht_event_e event,
          struct dcht_bucket_s * bk,
          int pos)
{
        struct notify_s *notify = arg;
        struct req_s * req = notify->req;
        unsigned seq = notify->seq;

        notify->cnt[event] += 1;

        uint32_t key = req[seq].key;
        uint32_t val = req[seq].val;

        if (notify->mask & (1u << event)) {
                switch (event) {
                case DCHT_EVENT_BUCKET_FULL:
                        fprintf(stderr,
                                "Event catch: %s bk:%p id:%u pos:%d key:%u val:%u seq:%u\n",
                                event_msg[event],
                                bk,
                                bucket_id(notify->tbl, bk),
                                pos,
                                key, val, seq);
                        table_dump("Event Bucket Full", notify->tbl);
                        bucket_dump(notify->tbl, bk);
                        break;

                case DCHT_EVENT_MOVED_ENTRY:
                        fprintf(stderr,
                                "Event catch: %s bk:%p id:%u pos:%d key:%u val:%u seq:%u\n",
                                event_msg[event],
                                bk,
                                bucket_id(notify->tbl, bk),
                                pos,
                                key, val, seq);
                        table_dump("Event Move Entry", notify->tbl);
                        bucket_dump(notify->tbl, bk);
                        break;

                case DCHT_EVENT_CUCKOO_REPLACED:
                        fprintf(stderr,
                                "Event catch: %s bk:%p id:%u pos:%d key:%u val:%u seq:%u\n",
                                event_msg[event],
                                bk,
                                bucket_id(notify->tbl, bk),
                                pos,
                                key, val, seq);
                        table_dump("Event Cuckoo Replaced", notify->tbl);
                        bucket_dump(notify->tbl, bk);
                        break;

                case DCHT_EVENT_UPDATE_VALUE:
#if 0
                        fprintf(stderr,
                                "Event catch: %s bk:%p id:%u pos:%d key:%u val:%u seq:%u\n",
                                event_msg[event],
                                bk,
                                bucket_id(notify->tbl, bk),
                                pos,
                                key, val, seq);
#endif

                        if (bk->key[pos] != key ||
                            bk->val[pos] != val) {
                                fprintf(stderr, "Bad key:%u val:%u\n", \
                                        bk->key[pos],
                                        bk->val[pos]);
                        }
#if 0
                        table_dump(notify->tbl);
                        bucket_dump(notify->tbl, bk);
#endif
                        break;

                default:
                        break;
                }
        }
}

struct walk_s {
        struct dcht_hash_table_s * tbl;
        struct req_s * req;
        unsigned nb;
};

static inline int
verify_cb(void * arg,
          const struct dcht_bucket_s * bk)
{
        struct walk_s * wk = arg;
        unsigned nb_keys = dcht_hash_bucket_keys_nb(bk);
        unsigned nb = 0;

        for (unsigned i = 0; i < DCHT_BUCKET_ENTRY_SZ; i++) {
                if (bk->key[i] == DCHT_UNUSED_KEY)
                        continue;
                nb += 1;

                struct dcht_bucket_s * bk_p[2];
                uint32_t key = bk->key[i];

                /* hash check */
                dcht_hash_buckets_prefetch(wk->tbl, key, bk_p);
                if (bk_p[0] != bk && bk_p[1] != bk) {
                        fprintf(stderr,
                                "not matched key bk:%p pos:%u key:%u val:%u\n",
                                bk, i, key, bk->val[i]);
                        return -1;
                }
        }

        if (nb != nb_keys) {
                fprintf(stderr, "not matched valid key. nb:%u keys:%u\n",
                        nb, nb_keys);
                return -1;
        }
        wk->nb += nb_keys;
        return 0;
}

static inline int
verify_tbl(struct dcht_hash_table_s * tbl,
           struct req_s * req,
           int nb,
           const char * func,
           const char * msg)
{
        struct walk_s walk;
        int ret;

        memset(&walk, 0, sizeof(walk));
        walk.tbl = tbl;
        walk.req = req;

        ret = dcht_hash_walk(tbl, verify_cb, &walk);
        if (ret) {
                fprintf(stderr, "failed to Walk:%u\n", walk.nb);
        } else {
                if (walk.nb != tbl->current_entries) {
                        fprintf(stderr, "mismatched number of entries:%u cur:%u\n",
                                walk.nb, tbl->current_entries);
                        ret = -1;
                } else if (walk.nb != (unsigned) nb) {
                        fprintf(stderr, "mismatched number of entries:%u nb:%u\n",
                                walk.nb, nb);
                        ret = -1;
                }
        }

        if (ret)
                fprintf(stderr, "%s:Verify Ng. %s\n", func, msg);
        else
                fprintf(stderr, "%s:Verify Ok. %s\n", func, msg);
        return ret;
}

/*
 * Vector Test
 */
struct vector_s {
#define VECTOR_SIZE	5
        struct dcht_bucket_s * bk_p[VECTOR_SIZE][2];
        uint32_t val[VECTOR_SIZE];
        int ret[VECTOR_SIZE];
};

static inline int
vec_prefetch(struct dcht_hash_table_s * tbl,
             int nb,
             struct req_s * req,
             struct vector_s * vec)
{
        int i;

        if (nb > VECTOR_SIZE)
                nb = VECTOR_SIZE;

        for (i = 0; i < nb; i++)
                dcht_hash_buckets_prefetch(tbl, req[i].key, vec->bk_p[i]);
        return i;
}

/*
 * Vector Search
 */
static inline int
vec_find_next(struct dcht_hash_table_s * tbl,
              int fetch_nb,
              struct req_s * cur_req,
              struct vector_s * cur_vec,
              int nb,
              struct req_s * nxt_req,
              struct vector_s * nxt_vec)
{
        int nxt_fetch = 0;

        nxt_fetch = vec_prefetch(tbl, nb, nxt_req, nxt_vec);

        for (int i = 0; i < fetch_nb; i++) {
                cur_vec->ret[i] = dcht_hash_find_in_buckets(cur_req[i].key,
                                                            cur_vec->bk_p[i],
                                                            &cur_vec->val[i]);
        }

        for (int i = 0; i < fetch_nb; i++) {
                if (cur_vec->ret[i] < 0 || cur_vec->val[i] != cur_req[i].val)
                        return -1;
        }
        return nxt_fetch;
}

static inline int
vector_search(struct dcht_hash_table_s * tbl,
              struct req_s * req,
              int nb)
{
        int cur = 0, nxt = 0;
        struct vector_s vec[2];
        int fetch_nb;
        struct req_s * fetch_req = req;

        fetch_nb = vec_prefetch(tbl, nb, fetch_req, &vec[nxt & 1]);
        nxt++;

        while (fetch_nb > 0) {
                nb -= fetch_nb;
                fetch_req += fetch_nb;

                fetch_nb = vec_find_next(tbl,
                                         fetch_nb, req, &vec[cur & 1],
                                         nb, fetch_req, &vec[nxt & 1]);
                cur++;
                nxt++;
                req = fetch_req;
        }

        return fetch_nb;
}

/*
 * Vector Add
 */
static inline int
vec_add_next(struct dcht_hash_table_s * tbl,
             int fetch_nb,
             struct req_s * cur_req,
             struct vector_s * cur_vec,
             int nb,
             struct req_s * nxt_req,
             struct vector_s * nxt_vec)
{
        int nxt_fetch = 0;

        nxt_fetch = vec_prefetch(tbl, nb, nxt_req, nxt_vec);

        for (int i = 0; i < fetch_nb; i++) {
                cur_vec->ret[i] =
                        dcht_hash_add_in_buckets(tbl, cur_vec->bk_p[i],
                                                 cur_req[i].key, cur_req[i].val,
                                                 true);
        }

        for (int i = 0; i < fetch_nb; i++) {
                if (cur_vec->ret[i] < 0)
                        return -1;
        }
        return nxt_fetch;
}

static inline int
vector_add(struct dcht_hash_table_s * tbl,
           struct req_s * req,
           int nb)
{
        int cur = 0, nxt = 0;
        struct vector_s vec[2];
        int fetch_nb;
        struct req_s * fetch_req = req;

        fetch_nb = vec_prefetch(tbl, nb, fetch_req, &vec[nxt & 1]);
        nxt++;

        while (fetch_nb > 0) {
                nb -= fetch_nb;
                fetch_req += fetch_nb;

                fetch_nb = vec_add_next(tbl,
                                        fetch_nb, req, &vec[cur & 1],
                                        nb, fetch_req, &vec[nxt & 1]);
                cur++;
                nxt++;
                req = fetch_req;
        }

        return fetch_nb;
}

/*
 * Vector Delete
 */
static inline int
vec_del_next(struct dcht_hash_table_s * tbl,
             int fetch_nb,
             struct req_s * cur_req,
             struct vector_s * cur_vec,
             int nb,
             struct req_s * nxt_req,
             struct vector_s * nxt_vec)
{
        int nxt_fetch = 0;

        nxt_fetch = vec_prefetch(tbl, nb, nxt_req, nxt_vec);

        for (int i = 0; i < fetch_nb; i++) {
                cur_vec->ret[i] = dcht_hash_del_in_buckets(tbl,
                                                           cur_vec->bk_p[i],
                                                           cur_req[i].key);
        }

        for (int i = 0; i < fetch_nb; i++) {
                if (cur_vec->ret[i] < 0)
                        return -1;
        }
        return nxt_fetch;
}

static inline int
vector_del(struct dcht_hash_table_s * tbl,
           struct req_s * req,
           int nb)
{
        int cur = 0, nxt = 0;
        struct vector_s vec[2];
        int fetch_nb;
        struct req_s * fetch_req = req;

        fetch_nb = vec_prefetch(tbl, nb, fetch_req, &vec[nxt & 1]);
        nxt++;

        while (fetch_nb > 0) {
                nb -= fetch_nb;
                fetch_req += fetch_nb;

                fetch_nb = vec_del_next(tbl,
                                        fetch_nb, req, &vec[cur & 1],
                                        nb, fetch_req, &vec[nxt & 1]);
                cur++;
                nxt++;
                req = fetch_req;
        }

        return fetch_nb;
}

/*
 * create request array
 */
static inline struct req_s *
pre_register(struct dcht_hash_table_s * tbl,
             int * nb_p)
{
        int nb = tbl->nb_entries;
        struct req_s * req = calloc(nb, sizeof(*req));
        struct notify_s notify;

        table_dump("Start Pre-Register", tbl);

        memset(&notify, 0, sizeof(notify));

        notify.tbl = tbl;
        notify.req = req;
        notify.mask = DCHT_EVENT_BIT(DCHT_EVENT_UPDATE_VALUE);

        tbl->event_notify_cb = notify_cb;
        tbl->arg = &notify;

        /* add */
        for (int i = 0; i < nb; i++) {
                uint32_t dummy;
                uint32_t key;

                req[i].val = i;
                notify.seq = i;

                do {
                        key = random();
                } while (!dcht_hash_find(tbl, key, &dummy));

                req[i].key = key;
                if (dcht_hash_add(tbl, req[i].key, req[i].val, true) < 0) {
                        fprintf(stderr, "failed to add: %d %u\n",
                                i, req[i].key);
                        nb = tbl->current_entries;
                        break;
                }
        }

        table_dump("After Add", tbl);
        if (verify_tbl(tbl, req, nb, __func__, "after add"))
                goto end;
        dcht_hash_clean(tbl);

        /* reverse test */
        for (int i = nb - 1; i >= 0; i--) {
                notify.seq = i;
                if (dcht_hash_add(tbl, req[i].key, req[i].val, true) < 0) {
                        fprintf(stderr, "XXX failed to add: %d %u\n",
                                i, req[i].key);
                        nb = tbl->current_entries;
                        break;
                }
        }
        table_dump("After Reverse", tbl);
        if (verify_tbl(tbl, req, nb, __func__, "after revers add"))
                goto end;

        int valid = 0;
        /* search */
        for (int i = 0; valid < nb; i++) {
                notify.seq = i;

                uint32_t val;

                if (!dcht_hash_find(tbl, req[i].key, &val) &&
                    val == req[i].val) {
                        valid += 1;
                }
        }

        fprintf(stderr, "fin searched:%u\n", tbl->current_entries);

        if (verify_tbl(tbl, req, valid, __func__, "after search"))
                goto end;

        fprintf(stderr, "notify cnt Full:%u Moved:%u Replaced:%u Update:%u\n",
                notify.cnt[0],
                notify.cnt[1],
                notify.cnt[2],
                notify.cnt[3]);

        *nb_p = valid;
        tbl->event_notify_cb = NULL;
        dcht_hash_clean(tbl);

 end:
        fprintf(stderr, "done:%s retry_hash:%u\n\n", __func__, tbl->retry_hash);

        return req;
}

/*
 * Single Action Speed Teat
 */
static inline int
single_speed_test(struct dcht_hash_table_s * tbl,
                  struct req_s * req,
                  int nb)
{
        uint64_t tsc;
        int ret = -1;

        fprintf(stderr, "Start Single Speed Test nb:%u >>>\n", nb);

        /* Add */
        tsc = rdtsc();
        for (int i = 0; i < nb; i++) {
                if (dcht_hash_add(tbl, req[i].key, req[i].val, true) < 0) {
                        fprintf(stderr, "%s:failed to add: %d %u\n",
                                __func__, i, req[i].key);
                        goto end;
                }
        }
        tsc = rdtsc() - tsc;

        table_dump("After Add", tbl);
        if (verify_tbl(tbl, req, nb, __func__, "after add"))
                goto end;
        fprintf(stderr, "%s: add speed %"PRIu64"tsc/add\n\n",
                __func__, tsc / nb);

        /* Search */
        tsc = rdtsc();
        for (int i = 0; i < nb; i++) {
                uint32_t val;

                if (dcht_hash_find(tbl, req[i].key, &val) < 0) {
                        fprintf(stderr, "%s:failed to search: %d %u\n",
                                __func__, i, req[i].key);
                        goto end;
                }
        }
        tsc = rdtsc() - tsc;

        table_dump("After Search", tbl);
        if (verify_tbl(tbl, req, nb, __func__, "after search"))
                goto end;
        fprintf(stderr, "%s: search speed %"PRIu64"tsc/search\n\n",
                __func__, tsc / nb);

        /* Delete */
        tsc = rdtsc();
        for (int i = 0; i < nb; i++) {
                if (dcht_hash_del(tbl, req[i].key) < 0) {
                        fprintf(stderr, "%s:failed to deleteh: %d %u\n\n",
                                __func__, i, req[i].key);
                        goto end;
                }
        }
        tsc = rdtsc() - tsc;

        table_dump("After Delete", tbl);
        if (verify_tbl(tbl, req, 0, __func__, "after delete"))
                goto end;
        fprintf(stderr, "%s: search delete %"PRIu64"tsc/delete\n",
                __func__, tsc / nb);

        ret = 0;
 end:
        fprintf(stderr, "<<< End Single Speed Test\n\n");
        dcht_hash_clean(tbl);
        return ret;
 }

static inline int
vector_speed_test(struct dcht_hash_table_s * tbl,
                  struct req_s * req,
                  int nb)
{
        uint64_t tsc;
        int ret = -1;

        fprintf(stderr, "Start Vector Speed Test >>>\n");

        /* Add */
        tsc = rdtsc();
        if (vector_add(tbl, req, nb) < 0) {
                fprintf(stderr, "%s: failed vector add\n", __func__);
                goto end;
        }
        tsc = rdtsc() - tsc;

        table_dump("After Add", tbl);
        if (verify_tbl(tbl, req, nb, __func__, "after add"))
                goto end;
        fprintf(stderr, "%s: add speed %"PRIu64"tsc/add\n\n",
                __func__, tsc / nb);

        /* Search */
        tsc = rdtsc();
        if (vector_search(tbl, req, nb) < 0) {
                fprintf(stderr, "%s: failed vector search\n", __func__);
                goto end;
        }
        tsc = rdtsc() - tsc;

        table_dump("After Search", tbl);
        if (verify_tbl(tbl, req, nb, __func__, "after search"))
                goto end;
        fprintf(stderr, "%s: search speed %"PRIu64"tsc/search\n\n",
                __func__, tsc / nb);

        /* Delete */
        tsc = rdtsc();
        if (vector_del(tbl, req, nb) < 0) {
                fprintf(stderr, "%s: failed vector del\n", __func__);
                goto end;
        }
        tsc = rdtsc() - tsc;

        table_dump("After Delete", tbl);
        if (verify_tbl(tbl, req, 0, __func__, "after delete"))
                goto end;
        fprintf(stderr, "%s: search delete %"PRIu64"tsc/delete\n",
                __func__, tsc / nb);

        ret = 0;
 end:
        fprintf(stderr, "<<< End Vector Speed Test\n\n");
        dcht_hash_clean(tbl);
        return ret;
}

static inline int
add_del_test(struct dcht_hash_table_s * tbl,
             struct req_s * req,
             int nb)
{
        int ret = -1;

        fprintf(stderr, "Start Add-Delete Test >>>\n\n");

        /* first add */
        for (int i = 0; i < nb; i++) {
                if (dcht_hash_add(tbl, req[i].key, req[i].val, true) < 0) {
                        fprintf(stderr, "failed to add: %d %u\n",
                                i, req[i].key);
                        goto end;
                }
        }

        int loop_cnt = 100;
        while (loop_cnt--) {
                fprintf(stderr, "loop:%d\n", loop_cnt);

                for (int i = 0; i < nb; i++) {
                        if (dcht_hash_del(tbl, req[i].key)) {
                                fprintf(stderr, "failed to delete: %d %u\n",
                                        i, req[i].key);
                                goto end;
                        }

                        uint32_t dummy;
                        uint32_t key;
                        do {
                                key = random();
                        } while (!dcht_hash_find(tbl, key, &dummy));

                        if (dcht_hash_add(tbl, key, req[i].val, true) < 0) {
                                fprintf(stderr, "failed to add: %d %u\n", i, key);
                                break;
                        }
                        req[i].key = key;
                }
        }

        table_dump("After Add-Delete loop", tbl);
        if (verify_tbl(tbl, req, nb, __func__, "after add-delete loop"))
                goto end;

        ret = 0;
 end:
        fprintf(stderr, "<<< End Add-Delete Test\n\n");
        dcht_hash_clean(tbl);
        return ret;
}

int
main(int ac,
     char **av)
{
        (void) ac;
        (void) av;
        char rand_state[256];

        initstate(rdtsc(), rand_state, sizeof(rand_state));

#ifndef TARGET_NB
#define TARGET_NB	1024 * 1024 * 1
#endif
        int nb = TARGET_NB;
        struct dcht_hash_table_s * tbl = dcht_hash_table_create(nb);

        fprintf(stderr, "created table:%p\n", tbl);

        if (dcht_hash_utest(tbl))
                return -1;

        struct req_s * req = pre_register(tbl, &nb);

        fprintf(stderr, "retry:%u / %d bucket:%zu\n",
                tbl->retry_hash / 4, nb, sizeof(struct dcht_bucket_s));

#if 1
        single_speed_test(tbl, req, nb);
        vector_speed_test(tbl, req, nb);
        vector_speed_test(tbl, req, tbl->nb_entries * 0.8);
        add_del_test(tbl, req, tbl->nb_entries * 0.8);
#else
        (void) req;
#endif

        return 0;
}
