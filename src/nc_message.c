/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

#include <sys/uio.h>

#include <nc_core.h>
#include <nc_server.h>
#include <proto/nc_proto.h>

#if (IOV_MAX > 128)
#define NC_IOV_MAX 128
#else
#define NC_IOV_MAX IOV_MAX
#endif

/*
 *            nc_message.[ch]
 *         message (struct msg)
 *            +        +            .
 *            |        |            .
 *            /        \            .
 *         Request    Response      .../ nc_mbuf.[ch]  (mesage buffers)
 *      nc_request.c  nc_response.c .../ nc_memcache.c; nc_redis.c (message parser)
 *
 * Messages in nutcracker are manipulated by a chain of processing handlers,
 * where each handler is responsible for taking the input and producing an
 * output for the next handler in the chain. This mechanism of processing
 * loosely conforms to the standard chain-of-responsibility design pattern
 *
 * At the high level, each handler takes in a message: request or response
 * and produces the message for the next handler in the chain. The input
 * for a handler is either a request or response, but never both and
 * similarly the output of an handler is either a request or response or
 * nothing.
 *
 * Each handler itself is composed of two processing units:
 *
 * 1). filter: manipulates output produced by the handler, usually based
 *     on a policy. If needed, multiple filters can be hooked into each
 *     location.
 * 2). forwarder: chooses one of the backend servers to send the request
 *     to, usually based on the configured distribution and key hasher.
 *
 * Handlers are registered either with Client or Server or Proxy
 * connections. A Proxy connection only has a read handler as it is only
 * responsible for accepting new connections from client. Read handler
 * (conn_recv_t) registered with client is responsible for reading requests,
 * while that registered with server is responsible for reading responses.
 * Write handler (conn_send_t) registered with client is responsible for
 * writing response, while that registered with server is responsible for
 * writing requests.
 *
 * Note that in the above discussion, the terminology send is used
 * synonymously with write or OUT event. Similarly recv is used synonymously
 * with read or IN event
 *
 *             Client+             Proxy           Server+
 *                              (nutcracker)
 *                                   .
 *       msg_recv {read event}       .       msg_recv {read event}
 *         +                         .                         +
 *         |                         .                         |
 *         \                         .                         /
 *         req_recv_next             .             rsp_recv_next
 *           +                       .                       +
 *           |                       .                       |       Rsp
 *           req_recv_done           .           rsp_recv_done      <===
 *             +                     .                     +
 *             |                     .                     |
 *    Req      \                     .                     /
 *    ===>     req_filter*           .           *rsp_filter
 *               +                   .                   +
 *               |                   .                   |
 *               \                   .                   /
 *               req_forward-//  (a) . (c)  \\-rsp_forward
 *                                   .
 *                                   .
 *       msg_send {write event}      .      msg_send {write event}
 *         +                         .                         +
 *         |                         .                         |
 *    Rsp' \                         .                         /     Req'
 *   <===  rsp_send_next             .             req_send_next     ===>
 *           +                       .                       +
 *           |                       .                       |
 *           \                       .                       /
 *           rsp_send_done-//    (d) . (b)    //-req_send_done
 *
 *
 * (a) -> (b) -> (c) -> (d) is the normal flow of transaction consisting
 * of a single request response, where (a) and (b) handle request from
 * client, while (c) and (d) handle the corresponding response from the
 * server.
 */

static uint64_t msg_id;          /* message id counter */
static uint64_t frag_id;         /* fragment id counter */
static uint32_t nfree_msgq;      /* # free msg q */
static struct msg_tqh free_msgq; /* free msg q */
static struct rbtree tmo_rbt;    /* timeout rbtree */
static struct rbnode tmo_rbs;    /* timeout rbtree sentinel */

static struct msg *
msg_from_rbe(struct rbnode *node)
{
    struct msg *msg;
    int offset;

    offset = offsetof(struct msg, tmo_rbe);
    msg = (struct msg *)((char *)node - offset);

    return msg;
}

struct msg *
msg_tmo_min(void)
{
    struct rbnode *node;

    node = rbtree_min(&tmo_rbt);
    if (node == NULL) {
        return NULL;
    }

    return msg_from_rbe(node);
}

void
msg_tmo_insert(struct msg *msg, struct conn *conn)
{
    struct rbnode *node;
    int timeout;

    ASSERT(msg->request);
    ASSERT(!msg->quit && !msg->noreply);

    timeout = server_timeout(conn);
    if (timeout <= 0) {
        return;
    }

    node = &msg->tmo_rbe;
    node->key = nc_msec_now() + timeout;
    node->data = conn;

    rbtree_insert(&tmo_rbt, node);

    log_debug(LOG_VERB, "insert msg %"PRIu64" into tmo rbt with expiry of "
              "%d msec", msg->id, timeout);
}

void
msg_tmo_delete(struct msg *msg)
{
    struct rbnode *node;

    node = &msg->tmo_rbe;

    /* already deleted */

    if (node->data == NULL) {
        return;
    }

    rbtree_delete(&tmo_rbt, node);

    log_debug(LOG_VERB, "delete msg %"PRIu64" from tmo rbt", msg->id);
}

static struct msg *
_msg_get(void)
{
    struct msg *msg;

    if (!TAILQ_EMPTY(&free_msgq)) {
        ASSERT(nfree_msgq > 0);

        msg = TAILQ_FIRST(&free_msgq);
        nfree_msgq--;
        TAILQ_REMOVE(&free_msgq, msg, m_tqe);
        goto done;
    }

    msg = nc_alloc(sizeof(*msg));
    if (msg == NULL) {
        return NULL;
    }

done:
    /* c_tqe, s_tqe, and m_tqe are left uninitialized */
    msg->id = ++msg_id;
    msg->peer = NULL;
    msg->owner = NULL;

    rbtree_node_init(&msg->tmo_rbe);

    STAILQ_INIT(&msg->mhdr);
    msg->mlen = 0;

    msg->state = 0;
    msg->pos = NULL;
    msg->token = NULL;

    msg->parser = NULL;
    msg->result = MSG_PARSE_OK;

    msg->pre_splitcopy = NULL;
    msg->post_splitcopy = NULL;
    msg->pre_coalesce = NULL;
    msg->post_coalesce = NULL;

    msg->type = MSG_UNKNOWN;

    msg->key_start = NULL;
    msg->key_end = NULL;

    msg->vlen = 0;
    msg->end = NULL;

    msg->frag_owner = NULL;
    msg->frag_seq = NULL;
    msg->nfrag = 0;
    msg->nfrag_done = 0;
    msg->frag_id = 0;

    msg->narg_start = NULL;
    msg->narg_end = NULL;
    msg->narg = 0;
    msg->rnarg = 0;
    msg->rlen = 0;
    msg->integer = 0;

    msg->err = 0;
    msg->error = 0;
    msg->ferror = 0;
    msg->request = 0;
    msg->quit = 0;
    msg->noreply = 0;
    msg->done = 0;
    msg->fdone = 0;
    msg->first_fragment = 0;
    msg->last_fragment = 0;
    msg->swallow = 0;
    msg->redis = 0;

    return msg;
}

struct msg *
msg_get(struct conn *conn, bool request, bool redis)
{
    struct msg *msg;

    msg = _msg_get();
    if (msg == NULL) {
        return NULL;
    }

    msg->owner = conn;
    msg->request = request ? 1 : 0;
    msg->redis = redis ? 1 : 0;

    if (redis) {
        if (request) {
            msg->parser = redis_parse_req;
        } else {
            msg->parser = redis_parse_rsp;
        }
        msg->pre_splitcopy = redis_pre_splitcopy;
        msg->post_splitcopy = redis_post_splitcopy;
        msg->pre_coalesce = redis_pre_coalesce;
        msg->post_coalesce = redis_post_coalesce;
    } else {
        if (request) {
            msg->parser = memcache_parse_req;
        } else {
            msg->parser = memcache_parse_rsp;
        }
        msg->pre_splitcopy = memcache_pre_splitcopy;
        msg->post_splitcopy = memcache_post_splitcopy;
        msg->pre_coalesce = memcache_pre_coalesce;
        msg->post_coalesce = memcache_post_coalesce;
    }

    log_debug(LOG_VVERB, "get msg %p id %"PRIu64" request %d owner sd %d",
              msg, msg->id, msg->request, conn->sd);

    return msg;
}

struct msg *
msg_get_error(bool redis, err_t err)
{
    struct msg *msg;
    struct mbuf *mbuf;
    int n;
    char *errstr = err ? strerror(err) : "unknown";
    char *protstr = redis ? "-ERR" : "SERVER_ERROR";

    msg = _msg_get();
    if (msg == NULL) {
        return NULL;
    }

    msg->state = 0;
    msg->type = MSG_RSP_MC_SERVER_ERROR;

    mbuf = mbuf_get();
    if (mbuf == NULL) {
        msg_put(msg);
        return NULL;
    }
    mbuf_insert(&msg->mhdr, mbuf);

    n = nc_scnprintf(mbuf->last, mbuf_size(mbuf), "%s %s"CRLF, protstr, errstr);
    mbuf->last += n;
    msg->mlen = (uint32_t)n;

    log_debug(LOG_VVERB, "get msg %p id %"PRIu64" len %"PRIu32" error '%s'",
              msg, msg->id, msg->mlen, errstr);

    return msg;
}

static void
msg_free(struct msg *msg)
{
    ASSERT(STAILQ_EMPTY(&msg->mhdr));

    log_debug(LOG_VVERB, "free msg %p id %"PRIu64"", msg, msg->id);
    nc_free(msg);
}

void
msg_put(struct msg *msg)
{
    log_debug(LOG_VVERB, "put msg %p id %"PRIu64"", msg, msg->id);

    while (!STAILQ_EMPTY(&msg->mhdr)) {
        struct mbuf *mbuf = STAILQ_FIRST(&msg->mhdr);
        mbuf_remove(&msg->mhdr, mbuf);
        mbuf_put(mbuf);
    }

    nfree_msgq++;
    TAILQ_INSERT_HEAD(&free_msgq, msg, m_tqe);
}

void
msg_dump(struct msg *msg, int level)
{
    struct mbuf *mbuf;

    if (log_loggable(level) == 0) {
        return;
    }

    loga("msg dump id %"PRIu64" request %d len %"PRIu32" type %d done %d "
         "error %d (err %d)", msg->id, msg->request, msg->mlen, msg->type,
         msg->done, msg->error, msg->err);

    STAILQ_FOREACH(mbuf, &msg->mhdr, next) {
        uint8_t *p, *q;
        long int len;

        p = mbuf->start;
        q = mbuf->last;
        len = q - p;

        loga_hexdump(p, len, "mbuf [%p] with %ld bytes of data", p, len);
    }
}

void
msg_reset_mbufs(struct msg *msg)
{
    struct mbuf *mbuf;

    STAILQ_FOREACH(mbuf, &msg->mhdr, next) {
        mbuf->pos = mbuf->last = mbuf->start;
    }
}

void
msg_init(void)
{
    log_debug(LOG_DEBUG, "msg size %d", sizeof(struct msg));
    msg_id = 0;
    frag_id = 0;
    nfree_msgq = 0;
    TAILQ_INIT(&free_msgq);
    rbtree_init(&tmo_rbt, &tmo_rbs);
}

void
msg_deinit(void)
{
    struct msg *msg, *nmsg;

    for (msg = TAILQ_FIRST(&free_msgq); msg != NULL;
         msg = nmsg, nfree_msgq--) {
        ASSERT(nfree_msgq > 0);
        nmsg = TAILQ_NEXT(msg, m_tqe);
        msg_free(msg);
    }
    ASSERT(nfree_msgq == 0);
}

bool
msg_empty(struct msg *msg)
{
    return msg->mlen == 0 ? true : false;
}

static struct mbuf *
get_mbuf(struct msg *msg)
{
    struct mbuf *mbuf;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (mbuf == NULL || mbuf_full(mbuf)) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NULL;
        }

        mbuf_insert(&msg->mhdr, mbuf);
        msg->pos = mbuf->pos;
    }
    ASSERT(mbuf->end - mbuf->last > 0);
    return mbuf;
}

static void
msg_make_reply(struct context *ctx, struct conn *conn, struct msg *req)
{
    struct mbuf *mbuf;
    struct msg *msg;

    msg = msg_get(conn, true, conn->redis); /* replay */
    if (msg == NULL) {
        conn->err = errno;
        return;
    }

    mbuf = get_mbuf(msg);
    if (mbuf == NULL) {
        msg_put(msg);
        return;
    }

    req->peer = msg;
    msg->peer = req;
    msg->request = 0;

    req->done = 1;
    /* event_add_out(ctx->ep, conn); */
    /* conn->dequeue_inq(ctx, conn, req); */
    conn->enqueue_outq(ctx, conn, req);
}

static uint32_t
key_to_idx(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    /* hash_tag */
    if (!string_empty(&pool->hash_tag)) {
        struct string *tag = &pool->hash_tag;
        uint8_t *tag_start, *tag_end;

        tag_start = nc_strchr(key, key + keylen, tag->data[0]);
        if (tag_start != NULL) {
            tag_end = nc_strchr(tag_start + 1, key + keylen, tag->data[1]);
            if (tag_end != NULL) {
                key = tag_start + 1;
                keylen = (uint32_t)(tag_end - key);
            }
        }
    }
    return server_pool_idx(pool, key, keylen);
}

/*
 * parse next key in mget request, update
 * r->key_start
 * r->key_end
 * */
static rstatus_t
msg_fragment_argx_update_keypos(struct msg *r)
{
    struct mbuf *buf;
    uint8_t *p;
    uint32_t len = 0;
    uint32_t keylen = 0;

    for (buf = STAILQ_FIRST(&r->mhdr); buf->pos >= buf->last; buf = STAILQ_FIRST(&r->mhdr)) {
        mbuf_remove(&r->mhdr, buf);
        mbuf_put(buf);
    }

    p = buf->pos;
    ASSERT(*p == '$');
    p++;

    len = 0;
    for (; p < buf->last && isdigit(*p); p++) {
        len = len * 10 + (uint32_t)(*p - '0');
    }

    keylen = len;
    len += (uint32_t)CRLF_LEN * 2;
    len += (uint32_t)(p - buf->pos);

    if (mbuf_length(buf) < len - CRLF_LEN) { /* key no in this buf, remove it. */
        len -= mbuf_length(buf);
        mbuf_remove(&r->mhdr, buf);
        mbuf_put(buf);

        buf = STAILQ_FIRST(&r->mhdr);
    }

    r->key_end = buf->pos + len - CRLF_LEN;
    r->key_start = r->key_end - keylen;
    buf->pos += len - CRLF_LEN;

    len = CRLF_LEN;
    while (mbuf_length(buf) < len) {        /* eat CRLF */
        len -= mbuf_length(buf);
        buf->pos = buf->last;

        buf = STAILQ_NEXT(buf, next);
    }
    buf->pos += len;

    return NC_OK;
}

static struct mbuf *
msg_ensure_mbuf(struct msg *msg, uint32_t len)
{
    struct mbuf *mbuf;

    if (STAILQ_EMPTY(&msg->mhdr)
        || mbuf_size(STAILQ_LAST(&msg->mhdr, mbuf, next)) < len) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NULL;
        }
        mbuf_insert(&msg->mhdr, mbuf);
    } else {
        mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    }
    return mbuf;
}

static rstatus_t
msg_append_key(struct msg *msg, uint8_t *key, uint32_t keylen)
{
    uint32_t len;
    struct mbuf *mbuf;
    uint8_t printbuf[32];

    /* 1. keylen */
    len = (uint32_t)nc_snprintf(printbuf, sizeof(printbuf), "$%d\r\n", keylen);
    mbuf = msg_ensure_mbuf(msg, len);
    if (mbuf == NULL) {
        nc_free(msg);
        return NC_ENOMEM;
    }
    mbuf_copy(mbuf, printbuf, len);
    msg->mlen += len;

    /* 2. key */
    mbuf = msg_ensure_mbuf(msg, keylen);
    if (mbuf == NULL) {
        nc_free(msg);
        return NC_ENOMEM;
    }
    msg->key_start = mbuf->last;   /* update key_start */
    mbuf_copy(mbuf, key, keylen);
    msg->mlen += keylen;
    msg->key_end = mbuf->last;     /* update key_start */

    /* 3. CRLF */
    mbuf = msg_ensure_mbuf(msg, CRLF_LEN);
    if (mbuf == NULL) {
        nc_free(msg);
        return NC_ENOMEM;
    }
    mbuf_copy(mbuf, (uint8_t *)CRLF, CRLF_LEN);
    msg->mlen += (uint32_t)CRLF_LEN;
    return NC_OK;
}


static rstatus_t
msg_fragment_argx(struct context *ctx, struct conn *conn, struct msg *msg, struct msg *nmsg, uint32_t key_step)
{
    struct server_pool *pool;
    struct mbuf *mbuf, *sub_msg_mbuf;
    struct msg **sub_msgs;
    uint32_t i;

    ASSERT(conn->client && !conn->proxy);
    ASSERT(conn->owner != NULL);

    /* init sub_msgs and msg->frag_seq */
    pool = conn->owner;
    sub_msgs = nc_alloc(pool->ncontinuum * sizeof(void *));
    for (i = 0; i < pool->ncontinuum; i++) {
        sub_msgs[i] = msg_get(msg->owner, msg->request, conn->redis);
    }
    msg->frag_seq = nc_alloc(sizeof(struct msg *) * msg->narg); /* the point for each key, point to sub_msgs elements */

    mbuf = STAILQ_FIRST(&msg->mhdr);
    mbuf->pos = mbuf->start;

    for (i = 0; i < 3; i++) {                 /* eat *narg\r\n$4\r\nMGET\r\n */
        for (; *(mbuf->pos) != '\n';) {
            mbuf->pos++;
        }
        mbuf->pos++;
    }

    msg->frag_id = ++frag_id;
    msg->first_fragment = 1;
    msg->nfrag = 0;
    msg->frag_owner = msg;

    for (i = 1; i < msg->narg; i++) {        /* for each  key */
        uint8_t *key;
        uint32_t keylen;
        uint32_t idx;
        struct msg *sub_msg;
        uint32_t len;

        msg_fragment_argx_update_keypos(msg);
        key = msg->key_start;
        keylen = (uint32_t)(msg->key_end - msg->key_start);
        idx = key_to_idx(pool, key, keylen);
        sub_msg = sub_msgs[idx];

        msg->frag_seq[i] = sub_msgs[idx];

        sub_msg->narg++;
        if (NC_OK != msg_append_key(sub_msg, key, keylen)) {
            nc_free(sub_msgs);
            return NC_ENOMEM;
        }

        if (key_step == 1) {            /* mget,del */
            continue;
        } else {                        /* mset, msetex */
            len = redis_copy_bulk(sub_msg, msg);
            log_debug(LOG_VVERB, "redis_copy_bulk for mset copy bytes: %d", len);
            i++;
            sub_msg->narg++;
        }
    }

    msg_make_reply(ctx, conn, msg);

    for (i = 0; i < pool->ncontinuum; i++) {     /* prepend mget header, and forward it */
        struct msg *sub_msg = sub_msgs[i];
        if (STAILQ_EMPTY(&sub_msg->mhdr)) {
            msg_put(sub_msg);
            continue;
        }

        sub_msg_mbuf = mbuf_get();
        if (sub_msg_mbuf == NULL) {
            nc_free(sub_msgs);
            return NC_ENOMEM;
        }
        if (msg->type == MSG_REQ_REDIS_MGET) {
            sub_msg_mbuf->last += nc_snprintf(sub_msg_mbuf->last, mbuf_size(sub_msg_mbuf), "*%d\r\n$4\r\nmget\r\n",
                                              sub_msg->narg + 1);
        } else if (msg->type == MSG_REQ_REDIS_DEL) {
            sub_msg_mbuf->last += nc_snprintf(sub_msg_mbuf->last, mbuf_size(sub_msg_mbuf), "*%d\r\n$3\r\ndel\r\n",
                                              sub_msg->narg + 1);
        } else if (msg->type == MSG_REQ_REDIS_MSET) {
            sub_msg_mbuf->last += nc_snprintf(sub_msg_mbuf->last, mbuf_size(sub_msg_mbuf), "*%d\r\n$4\r\nmset\r\n",
                                              sub_msg->narg + 1);
        }
        sub_msg->mlen += mbuf_length(sub_msg_mbuf);

        sub_msg->type = msg->type;
        sub_msg->frag_id = msg->frag_id;
        sub_msg->frag_owner = msg->frag_owner;
        STAILQ_INSERT_HEAD(&sub_msg->mhdr, sub_msg_mbuf, next);

        conn->recv_done(ctx, conn, sub_msg, nmsg);
        msg->nfrag++;
    }

    nc_free(sub_msgs);
    return NC_OK;
}





/*
 * parse next key in mget request, update
 * r->key_start
 * r->key_end
 * */
static rstatus_t
msg_fragment_retrieval_update_keypos(struct msg *r)
{
    struct mbuf *buf;
    uint8_t *p;

    for (buf = STAILQ_FIRST(&r->mhdr); buf->pos >= buf->last; buf = STAILQ_FIRST(&r->mhdr)) {
        mbuf_remove(&r->mhdr, buf);
        mbuf_put(buf);
    }

    p = buf->pos;
    for (; p < buf->last && isspace(*p); p++) {
        /* eat spaces */
    }

    r->key_start = p;
    for (; p < buf->last && !isspace(*p); p++) {
        /* read key */
    }
    r->key_end = p;

    for (; p < buf->last && isspace(*p); p++) {
        /* eat spaces */
    }
    buf->pos = p;
    return NC_OK;
}

static rstatus_t
msg_append_memcache_key(struct msg *msg, uint8_t *key, uint32_t keylen)
{
    struct mbuf *mbuf;

    mbuf = msg_ensure_mbuf(msg, keylen + 2);
    if (mbuf == NULL) {
        nc_free(msg);
        return NC_ENOMEM;
    }
    msg->key_start = mbuf->last;   /* update key_start */
    mbuf_copy(mbuf, key, keylen);
    msg->mlen += keylen;
    msg->key_end = mbuf->last;     /* update key_start */

    mbuf_copy(mbuf, (uint8_t *)" ", 1);
    msg->mlen += 1;
    return NC_OK;
}


static rstatus_t
msg_fragment_retrieval(struct context *ctx, struct conn *conn, struct msg *msg, struct msg *nmsg)
{
    struct server_pool *pool;
    struct mbuf *mbuf, *sub_msg_mbuf;
    struct msg **sub_msgs;
    uint32_t i;

    ASSERT(conn->client && !conn->proxy);
    ASSERT(conn->owner != NULL);

    /* init sub_msgs and msg->frag_seq */
    pool = conn->owner;
    sub_msgs = nc_alloc(pool->ncontinuum * sizeof(void *));
    for (i = 0; i < pool->ncontinuum; i++) {
        sub_msgs[i] = msg_get(msg->owner, msg->request, conn->redis);
    }

    log_debug(LOG_VERB, "msg:%p, msg->narg:%d", msg, msg->narg);
    msg->frag_seq = nc_alloc(sizeof(struct msg *) * msg->narg); /* the point for each key, point to sub_msgs elements */

    mbuf = STAILQ_FIRST(&msg->mhdr);
    mbuf->pos = mbuf->start;

    for (; *(mbuf->pos) != ' ';) { /* eat 'get ' */
        mbuf->pos++;
    }
    mbuf->pos++;

    msg->frag_id = ++frag_id;
    msg->first_fragment = 1;
    msg->nfrag = 0;
    msg->frag_owner = msg;

    for (i = 1; i < msg->narg; i++) {        /* for each  key */
        uint8_t *key;
        uint32_t keylen;
        uint32_t idx;
        struct msg *sub_msg;

        msg_fragment_retrieval_update_keypos(msg);
        key = msg->key_start;
        keylen = (uint32_t)(msg->key_end - msg->key_start);
        idx = key_to_idx(pool, key, keylen);
        sub_msg = sub_msgs[idx];

        msg->frag_seq[i] = sub_msgs[idx];

        sub_msg->narg++;
        if (NC_OK != msg_append_memcache_key(sub_msg, key, keylen)) {
            nc_free(sub_msgs);
            return NC_ENOMEM;
        }
    }

    if (STAILQ_EMPTY(&msg->mhdr)) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            nc_free(sub_msgs);
            return NC_ENOMEM;
        }
        mbuf_insert(&msg->mhdr, mbuf);
    }

    /* conn->recv_done(ctx, conn, msg, nmsg); */
    msg_make_reply(ctx, conn, msg);

    for (i = 0; i < pool->ncontinuum; i++) {     /* prepend mget header, and forward it */
        struct msg *sub_msg = sub_msgs[i];
        if (STAILQ_EMPTY(&sub_msg->mhdr)) {
            msg_put(sub_msg);
            continue;
        }

        /* prepend get/gets (TODO: use a function) */
        sub_msg_mbuf = mbuf_get();
        if (sub_msg_mbuf == NULL) {
            nc_free(sub_msgs);
            return NC_ENOMEM;
        }
        if (msg->type == MSG_REQ_MC_GET) {
            sub_msg_mbuf->last += nc_snprintf(sub_msg_mbuf->last, mbuf_size(sub_msg_mbuf), "get ");
        } else if (msg->type == MSG_REQ_MC_GETS) {
            sub_msg_mbuf->last += nc_snprintf(sub_msg_mbuf->last, mbuf_size(sub_msg_mbuf), "gets ");
        }
        sub_msg->mlen += mbuf_length(sub_msg_mbuf);
        STAILQ_INSERT_HEAD(&sub_msg->mhdr, sub_msg_mbuf, next);

        /* append \r\n */
        sub_msg_mbuf = mbuf_get();
        if (sub_msg_mbuf == NULL) {
            nc_free(sub_msgs);
            return NC_ENOMEM;
        }
        sub_msg_mbuf->last += nc_snprintf(sub_msg_mbuf->last, mbuf_size(sub_msg_mbuf), "\r\n");
        sub_msg->mlen += mbuf_length(sub_msg_mbuf);
        STAILQ_INSERT_TAIL(&sub_msg->mhdr, sub_msg_mbuf, next);

        sub_msg->type = msg->type;
        sub_msg->frag_id = msg->frag_id;
        sub_msg->frag_owner = msg->frag_owner;

        /* msg_dump(sub_msg, LOG_VERB); */
        conn->recv_done(ctx, conn, sub_msg, nmsg);
        msg->nfrag++;
    }

    nc_free(sub_msgs);
    return NC_OK;
}

static rstatus_t
msg_parsed(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *nmsg; /* next msg */
    struct mbuf *mbuf, *nbuf;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (msg->pos == mbuf->last) {
        /* no more data to parse */
        nmsg = NULL;
    } else {
        /*
         * Input mbuf has un-parsed data. Split mbuf of the current message msg
         * into (mbuf, nbuf), where mbuf is the portion of the message that has
         * been parsed and nbuf is the portion of the message that is un-parsed.
         * Parse nbuf as a new message nmsg in the next iteration.
         */
        nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
        if (nbuf == NULL) {
            return NC_ENOMEM;
        }

        nmsg = msg_get(msg->owner, msg->request, conn->redis);
        if (nmsg == NULL) {
            mbuf_put(nbuf);
            return NC_ENOMEM;
        }
        mbuf_insert(&nmsg->mhdr, nbuf);
        nmsg->pos = nbuf->pos;

        /* update length of current (msg) and new message (nmsg) */
        nmsg->mlen = mbuf_length(nbuf);
        msg->mlen -= nmsg->mlen;
    }

    if (redis_argx(msg)) {
        rstatus_t status = msg_fragment_argx(ctx, conn, msg, nmsg, 1);
        if (status != NC_OK) {
            return status;
        }
        return NC_OK;
    } else if (redis_arg2x(msg)) {
        rstatus_t status = msg_fragment_argx(ctx, conn, msg, nmsg, 2);
        if (status != NC_OK) {
            return status;
        }
        return NC_OK;
    } else if (memcache_retrieval(msg)) {
        rstatus_t status = msg_fragment_retrieval(ctx, conn, msg, nmsg);
        if (status != NC_OK) {
            return status;
        }
        return NC_OK;
    } else {
        conn->recv_done(ctx, conn, msg, nmsg);
    }

    return NC_OK;
}

static rstatus_t
msg_fragment(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;   /* return status */
    struct msg *nmsg;   /* new message */
    struct mbuf *nbuf;  /* new mbuf */

    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);

    nbuf = mbuf_split(&msg->mhdr, msg->pos, msg->pre_splitcopy, msg);
    if (nbuf == NULL) {
        return NC_ENOMEM;
    }

    status = msg->post_splitcopy(msg);
    if (status != NC_OK) {
        mbuf_put(nbuf);
        return status;
    }

    nmsg = msg_get(msg->owner, msg->request, msg->redis);
    if (nmsg == NULL) {
        mbuf_put(nbuf);
        return NC_ENOMEM;
    }
    mbuf_insert(&nmsg->mhdr, nbuf);
    nmsg->pos = nbuf->pos;

    /* update length of current (msg) and new message (nmsg) */
    nmsg->mlen = mbuf_length(nbuf);
    msg->mlen -= nmsg->mlen;

    /*
     * Attach unique fragment id to all fragments of the message vector. All
     * fragments of the message, including the first fragment point to the
     * first fragment through the frag_owner pointer. The first_fragment and
     * last_fragment identify first and last fragment respectively.
     *
     * For example, a message vector given below is split into 3 fragments:
     *  'get key1 key2 key3\r\n'
     *  Or,
     *  '*4\r\n$4\r\nmget\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n'
     *
     *   +--------------+
     *   |  msg vector  |
     *   |(original msg)|
     *   +--------------+
     *
     *       frag_owner         frag_owner
     *     /-----------+      /------------+
     *     |           |      |            |
     *     |           v      v            |
     *   +--------------------+     +---------------------+
     *   |   frag_id = 10     |     |   frag_id = 10      |
     *   | first_fragment = 1 |     |  first_fragment = 0 |
     *   | last_fragment = 0  |     |  last_fragment = 0  |
     *   |     nfrag = 3      |     |      nfrag = 0      |
     *   +--------------------+     +---------------------+
     *               ^
     *               |  frag_owner
     *               \-------------+
     *                             |
     *                             |
     *                  +---------------------+
     *                  |   frag_id = 10      |
     *                  |  first_fragment = 0 |
     *                  |  last_fragment = 1  |
     *                  |      nfrag = 0      |
     *                  +---------------------+
     *
     *
     */
    if (msg->frag_id == 0) {
        msg->frag_id = ++frag_id;
        msg->first_fragment = 1;
        msg->nfrag = 1;
        msg->frag_owner = msg;
    }
    nmsg->frag_id = msg->frag_id;
    msg->last_fragment = 0;
    nmsg->last_fragment = 1;
    nmsg->frag_owner = msg->frag_owner;
    msg->frag_owner->nfrag++;

    stats_pool_incr(ctx, conn->owner, fragments);

    log_debug(LOG_VERB, "fragment msg into %"PRIu64" and %"PRIu64" frag id "
              "%"PRIu64"", msg->id, nmsg->id, msg->frag_id);

    conn->recv_done(ctx, conn, msg, nmsg);

    return NC_OK;
}

static rstatus_t
msg_repair(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct mbuf *nbuf;

    nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
    if (nbuf == NULL) {
        return NC_ENOMEM;
    }
    mbuf_insert(&msg->mhdr, nbuf);
    msg->pos = nbuf->pos;

    return NC_OK;
}

static rstatus_t
msg_parse(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    if (msg_empty(msg)) {
        /* no data to parse */
        conn->recv_done(ctx, conn, msg, NULL);
        return NC_OK;
    }

    msg->parser(msg);

    switch (msg->result) {
    case MSG_PARSE_OK:
        status = msg_parsed(ctx, conn, msg);
        break;

    case MSG_PARSE_FRAGMENT:
        status = msg_fragment(ctx, conn, msg);
        break;

    case MSG_PARSE_REPAIR:
        status = msg_repair(ctx, conn, msg);
        break;

    case MSG_PARSE_AGAIN:
        status = NC_OK;
        break;

    default:
        status = NC_ERROR;
        conn->err = errno;
        break;
    }

    return conn->err != 0 ? NC_ERROR : status;
}

static rstatus_t
msg_recv_chain(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *nmsg;
    struct mbuf *mbuf;
    size_t msize;
    ssize_t n;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (mbuf == NULL || mbuf_full(mbuf)) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NC_ENOMEM;
        }
        mbuf_insert(&msg->mhdr, mbuf);
        msg->pos = mbuf->pos;
    }
    ASSERT(mbuf->end - mbuf->last > 0);

    msize = mbuf_size(mbuf);

    n = conn_recv(conn, mbuf->last, msize);
    if (n < 0) {
        if (n == NC_EAGAIN) {
            return NC_OK;
        }
        return NC_ERROR;
    }

    ASSERT((mbuf->last + n) <= mbuf->end);
    mbuf->last += n;
    msg->mlen += (uint32_t)n;

    for (;;) {
        status = msg_parse(ctx, conn, msg);
        if (status != NC_OK) {
            return status;
        }

        /* get next message to parse */
        nmsg = conn->recv_next(ctx, conn, false);
        if (nmsg == NULL || nmsg == msg) {
            /* no more data to parse */
            break;
        }

        msg = nmsg;
    }

    return NC_OK;
}

rstatus_t
msg_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg;

    ASSERT(conn->recv_active);

    conn->recv_ready = 1;
    do {
        msg = conn->recv_next(ctx, conn, true);
        if (msg == NULL) {
            return NC_OK;
        }

        status = msg_recv_chain(ctx, conn, msg);
        if (status != NC_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return NC_OK;
}

static rstatus_t
msg_send_chain(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg_tqh send_msgq;            /* send msg q */
    struct msg *nmsg;                    /* next msg */
    struct mbuf *mbuf, *nbuf;            /* current and next mbuf */
    size_t mlen;                         /* current mbuf data length */
    struct iovec *ciov, iov[NC_IOV_MAX]; /* current iovec */
    struct array sendv;                  /* send iovec */
    size_t nsend, nsent;                 /* bytes to send; bytes sent */
    size_t limit;                        /* bytes to send limit */
    ssize_t n;                           /* bytes sent by sendv */

    TAILQ_INIT(&send_msgq);

    array_set(&sendv, iov, sizeof(iov[0]), NC_IOV_MAX);

    /* preprocess - build iovec */

    nsend = 0;
    /*
     * readv() and writev() returns EINVAL if the sum of the iov_len values
     * overflows an ssize_t value Or, the vector count iovcnt is less than
     * zero or greater than the permitted maximum.
     */
    limit = SSIZE_MAX;

    for (;;) {
        ASSERT(conn->smsg == msg);

        TAILQ_INSERT_TAIL(&send_msgq, msg, m_tqe);

        for (mbuf = STAILQ_FIRST(&msg->mhdr);
             mbuf != NULL && array_n(&sendv) < NC_IOV_MAX && nsend < limit;
             mbuf = nbuf) {
            nbuf = STAILQ_NEXT(mbuf, next);

            if (mbuf_empty(mbuf)) {
                continue;
            }

            mlen = mbuf_length(mbuf);
            if ((nsend + mlen) > limit) {
                mlen = limit - nsend;
            }

            ciov = array_push(&sendv);
            ciov->iov_base = mbuf->pos;
            ciov->iov_len = mlen;

            nsend += mlen;
        }

        if (array_n(&sendv) >= NC_IOV_MAX || nsend >= limit) {
            break;
        }

        msg = conn->send_next(ctx, conn);
        if (msg == NULL) {
            break;
        }
    }

    conn->smsg = NULL;
    if (!TAILQ_EMPTY(&send_msgq) && nsend != 0) {
        n = conn_sendv(conn, &sendv, nsend);
    } else {
        n = 0;
    }

    nsent = n > 0 ? (size_t)n : 0;

    /* postprocess - process sent messages in send_msgq */

    for (msg = TAILQ_FIRST(&send_msgq); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, m_tqe);

        TAILQ_REMOVE(&send_msgq, msg, m_tqe);

        if (nsent == 0) {
            if (msg->mlen == 0) {
                conn->send_done(ctx, conn, msg);
            }
            continue;
        }

        /* adjust mbufs of the sent message */
        for (mbuf = STAILQ_FIRST(&msg->mhdr); mbuf != NULL; mbuf = nbuf) {
            nbuf = STAILQ_NEXT(mbuf, next);

            if (mbuf_empty(mbuf)) {
                continue;
            }

            mlen = mbuf_length(mbuf);
            if (nsent < mlen) {
                /* mbuf was sent partially; process remaining bytes later */
                mbuf->pos += nsent;
                ASSERT(mbuf->pos < mbuf->last);
                nsent = 0;
                break;
            }

            /* mbuf was sent completely; mark it empty */
            mbuf->pos = mbuf->last;
            nsent -= mlen;
        }

        /* message has been sent completely, finalize it */
        if (mbuf == NULL) {
            conn->send_done(ctx, conn, msg);
        }
    }

    ASSERT(TAILQ_EMPTY(&send_msgq));

    if (n >= 0) {
        return NC_OK;
    }

    return (n == NC_EAGAIN) ? NC_OK : NC_ERROR;
}

rstatus_t
msg_send(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg;

    ASSERT(conn->send_active);

    conn->send_ready = 1;
    do {
        msg = conn->send_next(ctx, conn);
        if (msg == NULL) {
            /* nothing to send */
            return NC_OK;
        }

        status = msg_send_chain(ctx, conn, msg);
        if (status != NC_OK) {
            return status;
        }

    } while (conn->send_ready);

    return NC_OK;
}
