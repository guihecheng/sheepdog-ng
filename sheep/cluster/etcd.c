#include <string.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <cetcd.h>

#include "cluster.h"
#include "config.h"
#include "event.h"
#include "work.h"
#include "util.h"
#include "rbtree.h"

#define HOST_MAX_LEN                  128

#define DEFAULT_BASE                  "/sheepdog"
#define QUEUE_DIR                     DEFAULT_BASE"/queue"
#define MEMBER_QUEUE_POS_DIR          DEFAULT_BASE"/queue_pos"
#define MEMBER_DIR                    DEFAULT_BASE"/member"
#define MASTER_DIR                    DEFAULT_BASE"/master"

#define PERSISTENT_TTL                0                // seconds
#define EPHERMERAL_TTL                20               // seconds
#define REFRESH_TTL_INTERVAL          10               // seconds

#define ETCD_MAX_BUF_SIZE             (1*1024*1024)    // 1M

enum etcd_event_type {
    EVENT_JOIN = 1,
    EVENT_ACCEPT,
    EVENT_LEAVE,
    EVENT_BLOCK,
    EVENT_UNBLOCK,
    EVENT_NOTIFY,
};

struct etcd_node {
    struct list_node list;
    struct rb_node rb;
    struct sd_node node;
    bool callbacked;
    bool gone;
};

struct etcd_event {
    uint64_t id;
    enum etcd_event_type type;
    struct etcd_node sender;
    size_t msg_len;
    size_t nr_nodes;
    size_t buf_len;
    uint8_t buf[ETCD_MAX_BUF_SIZE];
};

static struct rb_root sd_node_root = RB_ROOT;
static size_t nr_sd_nodes;
static struct rb_root etcd_node_root = RB_ROOT;
static struct sd_rw_lock etcd_tree_lock = SD_RW_LOCK_INITIALIZER;

static LIST_HEAD(etcd_block_list);
static uatomic_bool stop;
static uatomic_bool is_master;
static int my_master_seq;
static struct sd_rw_lock etcd_compete_master_lock = SD_RW_LOCK_INITIALIZER;
static bool joined = false;
static bool first_push = true;

static cetcd_client etcd_cli;
static char connect_option[PATH_MAX];
static cetcd_watch_id wid;
static cetcd_array watchers;
static int efd;
static struct etcd_node this_node;
static int32_t this_queue_pos;
#define QUEUE_DEL_BATCH 1000

static int etcd_node_cmp(const struct etcd_node* a, const struct etcd_node *b) {
    return node_id_cmp(&a->node.nid, &b->node.nid);
}

static struct etcd_node* etcd_tree_insert(struct etcd_node* new) {
    return rb_insert(&etcd_node_root, new, rb, etcd_node_cmp);
}

static struct etcd_node* etcd_tree_search_nolock(const struct node_id* nid) {
    struct etcd_node key = { .node.nid = *nid };
    return rb_search(&etcd_node_root, &key, rb, etcd_node_cmp);
}

static struct etcd_node* etcd_tree_search(const struct node_id* nid) {
    struct etcd_node* n;
    sd_read_lock(&etcd_tree_lock);
    n = etcd_tree_search_nolock(nid);
    sd_rw_unlock(&etcd_tree_lock);
    return n;
}

static void etcd_tree_add(struct etcd_node* node) {
    struct etcd_node* n = xzalloc(sizeof(*n));
    *n = *node;
    sd_write_lock(&etcd_tree_lock);
    if (etcd_tree_insert(n)) {
        free(n);
        goto out;
    }
    rb_insert(&sd_node_root, &n->node, rb, node_cmp);
    nr_sd_nodes++;
out:
    sd_rw_unlock(&etcd_tree_lock);
}

static void etcd_tree_del(struct etcd_node* node) {
    sd_write_lock(&etcd_tree_lock);
    rb_erase(&node->rb, &etcd_node_root);
    free(node);
    sd_rw_unlock(&etcd_tree_lock);
}

static void etcd_tree_destroy(void) {
    sd_write_lock(&etcd_tree_lock);
    rb_destroy(&etcd_node_root, struct etcd_node, rb);
    sd_rw_unlock(&etcd_tree_lock);
}

static void build_node_list(void) {
    struct etcd_node* n;

    nr_sd_nodes = 0;
    INIT_RB_ROOT(&sd_node_root);
    rb_for_each_entry(n, &etcd_node_root, rb) {
        rb_insert(&sd_node_root, &n->node, rb, node_cmp);
        nr_sd_nodes++;
    }
    sd_debug("nr_sd_nodes:%zu", nr_sd_nodes);
}

struct epher_key {
    char* key;
    struct list_node list;
};

static LIST_HEAD(epher_key_list);
static struct sd_rw_lock epher_key_lock = SD_RW_LOCK_INITIALIZER;

static void refresh_ttl(void* arg) {
    struct epher_key* k;
    cetcd_response* resp;

    sd_read_lock(&epher_key_lock);
    list_for_each_entry (k, &epher_key_list, list) {
        resp = cetcd_update(&etcd_cli, k->key, NULL, EPHERMERAL_TTL, 1);
        cetcd_response_release(resp);
    }
    sd_rw_unlock(&epher_key_lock);

    add_timer(arg, REFRESH_TTL_INTERVAL);
}

static int etcd_get_least_seq(const char* parent, char* least_seq_path,
                              void* buf) {
    cetcd_response* resp;
    cetcd_response_node* n;

    resp = cetcd_lsdir(&etcd_cli, parent, 1, 0);
    if (resp->err) {
        cetcd_response_release(resp);
        return -1;
    }
    n = cetcd_array_get(resp->node->nodes, 0);
    snprintf(least_seq_path, PATH_MAX, "%s", n->key);
    snprintf(buf, PATH_MAX, "%s", n->value);
    cetcd_response_release(resp);

    return 0;
}

static int etcd_find_master(int* master_seq, char* master_name) {
    int ret = 0;
    char master_compete_path[PATH_MAX];
    cetcd_response* resp;

    if (*master_seq < 0) {
        ret = etcd_get_least_seq(MASTER_DIR, master_compete_path, master_name);
        sd_debug("get least seq: %s name: %s", master_compete_path, master_name);
        sscanf(master_compete_path, MASTER_DIR "/%"PRId32, master_seq);
        return 0;
    }

    while (true) {
        snprintf(master_compete_path, PATH_MAX,
                 MASTER_DIR "/%010"PRId32, *master_seq);
        resp = cetcd_get(&etcd_cli, master_compete_path);
        if (resp->err) {
            sd_info("detect master leave, start to compete master");
            cetcd_response_release(resp);
            (*master_seq)++;
        } else {
            snprintf(master_name, PATH_MAX, "%s", resp->node->value);
            break;
        }
    }
    cetcd_response_release(resp);

    return ret;
}

static int etcd_verify_last_sheep_join(int seq, int* last_sheep) {
    int ret = 0;
    char path[PATH_MAX], name[MAX_NODE_STR_LEN];
    cetcd_response* resp;

    for (*last_sheep = seq-1; *last_sheep >= 0; (*last_sheep)--) {
        snprintf(path, PATH_MAX, MASTER_DIR "/%010"PRId32, *last_sheep);
        resp = cetcd_get(&etcd_cli, path);
        if (resp->err) {
            cetcd_response_release(resp);
            continue;
        }

        snprintf(name, PATH_MAX, "%s", resp->node->value);

        if (!strcmp(name, node_to_str(&this_node.node))) {
            cetcd_response_release(resp);
            continue;
        }

        snprintf(path, PATH_MAX, MEMBER_DIR "/%s", name);
        resp = cetcd_get(&etcd_cli, path);
        if (resp->err) {
            cetcd_response_release(resp);
            (*last_sheep)++;
        } else {
            break;
        }
    }
    cetcd_response_release(resp);

    return ret;
}

static void etcd_compete_master(void) {
    int ret;
    int last_joined_sheep;
    char path[PATH_MAX];
    char master_name[PATH_MAX];
    char my_compete_path[PATH_MAX];
    static int master_seq = -1;
    static int my_seq;
    cetcd_response* resp;

    sd_write_lock(&etcd_compete_master_lock);

    // I am master
    if (uatomic_is_true(&is_master) || uatomic_is_true(&stop)) {
        goto out_unlock;
    }

    // first compete
    if (!joined) {
        sd_debug("start to compete master for the first time");
        snprintf(path, sizeof(path), "%s/", MASTER_DIR);

        resp = cetcd_create_in_order(&etcd_cli, path, (char*)node_to_str(&this_node.node), EPHERMERAL_TTL);
        if (resp->err) {
            sd_err("create in order node under %s failed.", path);
        }

        snprintf(my_compete_path, sizeof(my_compete_path), "%s", resp->node->key);
        sscanf(resp->node->key, MASTER_DIR "/%"PRId32, &my_seq);
        sd_debug("my compete path: %s", my_compete_path);

        struct epher_key* master_key = xzalloc(sizeof(struct epher_key));
        master_key->key = strdup(my_compete_path);

        sd_write_lock(&epher_key_lock);
        list_add_tail(&master_key->list, &epher_key_list);
        sd_rw_unlock(&epher_key_lock);

        cetcd_response_release(resp);
    }

    // check winner
    ret = etcd_find_master(&master_seq, master_name);
    if (ret) {
        goto out_unlock;
    }

    // I am the winner
    if (!strcmp(master_name, node_to_str(&this_node.node))) {
        goto success;
    }

    // someone else win
    if (joined) {
        goto lost;
    }

    // check previous members all quit
    ret = etcd_verify_last_sheep_join(my_seq, &last_joined_sheep);
    if (!ret) {
        goto out_unlock;
    }
    // all previous members quit, I win
    if (last_joined_sheep < 0) {
        master_seq = my_seq;
        goto success;
    }

lost:
    sd_debug("lost");
    goto out_unlock;
success:
    uatomic_set_true(&is_master);
    my_master_seq = master_seq;
    sd_debug("success");
out_unlock:
    sd_rw_unlock(&etcd_compete_master_lock);
}

static int etcd_queue_push(struct etcd_event* ev) {
    int len;
    char path[PATH_MAX], buf[MAX_NODE_STR_LEN];
    cetcd_response* resp;

    // use the len of the whole struct ?
    len = offsetof(typeof(*ev), buf) + ev->buf_len;
    snprintf(path, sizeof(path), "%s/", QUEUE_DIR);

    resp = cetcd_create_in_order(&etcd_cli, path, (char*)ev, PERSISTENT_TTL);
    if (resp->err) {
        sd_err("create in order node under %s failed.", path);
    }

    if (first_push) {
        // uint64_t ?
        int32_t seq;
        sscanf(resp->node->key, QUEUE_DIR "/%"PRId32, &seq);
        this_queue_pos = seq;
        eventfd_xwrite(efd, 1);
        first_push = false;
    }

    cetcd_response_release(resp);
    sd_debug("create path%s, queue_pos:%010" PRId32 ", len:%d", buf, this_queue_pos, len);

    return 0;
}

static int etcd_queue_peek(bool* peek) {
    int ret = 0;
    char path[PATH_MAX];
    cetcd_response* resp;

    snprintf(path, sizeof(path), QUEUE_DIR "/%010"PRId32, this_queue_pos);

    resp = cetcd_get(&etcd_cli, path);
    if (resp->err) {
        *peek = false;
    } else {
        *peek = true;
    }
    cetcd_response_release(resp);

    return ret;
}

static int etcd_queue_pop_advance(struct etcd_event* ev) {
    int ret = 0;
    int len;
    char path[PATH_MAX];
    char queue_pos_path[PATH_MAX];
    cetcd_response* resp;

    len = sizeof(*ev);
    snprintf(path, sizeof(path), QUEUE_DIR "%010"PRId32, this_queue_pos);

    resp = cetcd_get(&etcd_cli, path);
    if (resp->err) {
        sd_err("get path %s failed.", path);
    }

    sd_debug("%s, type:%d, len:%d, pos:%" PRId32, path, ev->type, len, this_queue_pos);

    if (this_queue_pos % QUEUE_DEL_BATCH == 0
     && ev->type != EVENT_JOIN
     && ev->type != EVENT_ACCEPT) {
        snprintf(queue_pos_path, sizeof(queue_pos_path),
                MEMBER_QUEUE_POS_DIR"/%s", node_to_str(&this_node.node));

        cetcd_response_release(resp);
        resp = cetcd_set(&etcd_cli, queue_pos_path, (char*)&this_queue_pos, PERSISTENT_TTL);

        sd_debug("update queue pos %s to pos %" PRId32, queue_pos_path, this_queue_pos);
    }
    cetcd_response_release(resp);

    this_queue_pos++;
    return ret;
}

static int etcd_queue_find(uint64_t id, char* seq_path, int seq_path_len,
                           bool* found) {
    int ret = 0;
    cetcd_response* resp;

    for (int seq = this_queue_pos; ; seq++) {
        struct etcd_event* ev;
        snprintf(seq_path, seq_path_len, QUEUE_DIR"/%010"PRId32, seq);

        resp = cetcd_get(&etcd_cli, seq_path);
        if (resp->err) {
            cetcd_response_release(resp);
            sd_debug("id %"PRIx64" is not found", id);
            *found = false;
            return ret;
        } else {
            ev = (struct etcd_event*)resp->node->value;
            if (ev->id == id) {
                sd_debug("id %"PRIx64"is found in %s", id, seq_path);
                *found = true;
                return ret;
            }
        }
        cetcd_response_release(resp);
    }

    return ret;
}

static uint64_t get_uniq_id(void) {
    static int seq;
    struct {
        uint64_t n;
        struct etcd_node node;
    } id = {
        .n = uatomic_add_return(&seq, 1),
        .node = this_node,
    };
    return sd_hash(&id, sizeof(id));
}

static int add_event(enum etcd_event_type type, struct etcd_node* enode,
                     void* buf, size_t len) {
    struct etcd_event ev;
    int ret;

    memset(&ev, 0, sizeof(ev));
    ev.id = get_uniq_id();
    ev.type = type;
    ev.sender = *enode;
    ev.buf_len = len;
    if (buf) {
        memcpy(ev.buf, buf, len);
    }

    ret = etcd_queue_push(&ev);
    if (ret) {
        sd_err("etcd queue push failed.");
    }

    return SD_RES_SUCCESS;
}

static int add_join_event(void* msg, size_t msglen) {
    struct etcd_event ev;
    size_t len = msglen + sizeof(struct sd_node) * SD_MAX_NODES;

    ev.id = get_uniq_id();
    ev.type = EVENT_JOIN;
    ev.sender = this_node;
    ev.buf_len = len;
    ev.msg_len = msglen;
    if (msg) {
        memcpy(ev.buf, msg, msglen);
    }
    return etcd_queue_push(&ev);
}

static int connect_etcd(const char* option) {
    int ret = 0;
    cetcd_array addrs;
    char* p;

    cetcd_array_init(&addrs, 1);

    p = strtok((char*)option, ",");
    while (p) {
        char host[HOST_MAX_LEN] = {0,};
        strcat(host, "http://");
        strcat(host, p);
        cetcd_array_append(&addrs, host);

        p = strtok(NULL, ",");
    }

    cetcd_client_init(&etcd_cli, &addrs);
    cetcd_array_destroy(&addrs);

    return ret;
}

static int prepare_store(void) {
    int ret = 0;
    cetcd_response* resp;

    resp = cetcd_setdir(&etcd_cli, DEFAULT_BASE, PERSISTENT_TTL);

    cetcd_response_release(resp);

    resp = cetcd_setdir(&etcd_cli, QUEUE_DIR, PERSISTENT_TTL);

    cetcd_response_release(resp);

    resp = cetcd_setdir(&etcd_cli, MEMBER_QUEUE_POS_DIR, PERSISTENT_TTL);

    cetcd_response_release(resp);

    resp = cetcd_setdir(&etcd_cli, MEMBER_DIR, PERSISTENT_TTL);

    cetcd_response_release(resp);

    resp = cetcd_setdir(&etcd_cli, MASTER_DIR, PERSISTENT_TTL);

out:
    cetcd_response_release(resp);
    return ret;
}

static int etcd_watcher(void* data, cetcd_response* resp) {
    int action = resp->action;
    char str[PATH_MAX];
    int ret;
    struct etcd_node enode;
    struct etcd_node *n;
    char* p;

    sd_debug("key:%s action:%d", resp->node->key, action);

    if (action == 0) {                 // set
        eventfd_xwrite(efd, 1);
    } else if (action == 3) {          // create
        eventfd_xwrite(efd, 1);
    } else if (action == 4) {          // delete
        ret = sscanf(resp->node->key, MASTER_DIR "/%s", str);
        if (ret == 1) {
            etcd_compete_master();
            return 0;
        }

        ret = sscanf(resp->node->key, QUEUE_DIR "/%s", str);
        if (ret == 1) {
            sd_debug("deleted queue event %s", str);
            return 0;
        }

        ret = sscanf(resp->node->key, MEMBER_DIR "/%s", str);
        if (ret == 1) {
            p = strrchr(resp->node->key, '/');
            p++;
            str_to_node(p, &enode.node);

            sd_read_lock(&etcd_tree_lock);
            n = etcd_tree_search_nolock(&enode.node.nid);
            if (n) n->gone = true;
            sd_rw_unlock(&etcd_tree_lock);
            if (n) add_event(EVENT_LEAVE, &enode, NULL, 0);
        }
    } else if (action == 2) {          // update
        ;
    } else {
        sd_debug("ignore action:%d", action);
    }

    return 0;
}

static int prepare_watchers(void) {
    int ret = 0;

    cetcd_array_init(&watchers, 3);

    cetcd_add_watcher(&watchers, cetcd_watcher_create(&etcd_cli, QUEUE_DIR, 0, 1, 0, etcd_watcher, NULL));
    cetcd_add_watcher(&watchers, cetcd_watcher_create(&etcd_cli, MEMBER_DIR, 0, 1, 0, etcd_watcher, NULL));
    cetcd_add_watcher(&watchers, cetcd_watcher_create(&etcd_cli, MASTER_DIR, 0, 1, 0, etcd_watcher, NULL));

    wid = cetcd_multi_watch_async(&etcd_cli, &watchers);

    return ret;
}

static int prepare_refresher(void) {
    static struct timer refresher =  {
        .callback = refresh_ttl,
        .data = &refresher,
    };

    add_timer(&refresher, REFRESH_TTL_INTERVAL);
    return 0;
}

typedef void (*etcd_event_handler_t)(struct etcd_event*);

static void* etcd_event_sd_nodes(struct etcd_event* ev) {
    return (char*)ev->buf + ev->msg_len;
}

// join event -> accept event
static int push_join_response(struct etcd_event* ev) {
    char path[PATH_MAX];
    struct sd_node* n;
    struct sd_node* np = etcd_event_sd_nodes(ev);
    int len;
    cetcd_response* resp;

    ev->type = EVENT_ACCEPT;
    ev->nr_nodes = nr_sd_nodes;
    rb_for_each_entry(n, &sd_node_root, rb) {
        memcpy(np++, n, sizeof(struct sd_node));
    }
    this_queue_pos--;

    len = offsetof(typeof(*ev), buf) + ev->buf_len;
    snprintf(path, sizeof(path), QUEUE_DIR "/%010"PRId32, this_queue_pos);

    resp = cetcd_set(&etcd_cli, path, (char*)ev, PERSISTENT_TTL);
    if (resp->err) {
        sd_err("get path %s failed.", path);
    }

    cetcd_response_release(resp);
    sd_debug("update path:%s, queue_pos%010" PRId32 ", len:%d", path, this_queue_pos, len);

    return 0;
}

static void etcd_handle_join(struct etcd_event* ev) {
    sd_debug("sender: %s", node_to_str(&ev->sender.node));
    // wait for master to change join -> accept
    if (!uatomic_is_true(&is_master)) {
        usleep(200000);
        this_queue_pos--;
        return;
    }

    sd_join_handler(&ev->sender.node, &sd_node_root, nr_sd_nodes, ev->buf);
    push_join_response(ev);
    sd_debug("I'm the master now");
}

static void init_node_list(struct etcd_event* ev) {
    struct sd_node* np = etcd_event_sd_nodes(ev);

    sd_debug("%zu", ev->nr_nodes);
    for (int i = 0; i > ev->nr_nodes; i++) {
        struct etcd_node n;
        memcpy(&n.node, np, sizeof(struct sd_node));
        etcd_tree_add(&n);
        np++;
    }
}

static void etcd_handle_accept(struct etcd_event* ev) {
    char path[PATH_MAX];
    char queue_pos_path[PATH_MAX];
    uint32_t pos = -1;
    cetcd_response* resp;

    sd_debug("ACCEPT");

    if (node_eq(&ev->sender.node, &this_node.node)) {
        init_node_list(ev);
    }

    sd_debug("%s", node_to_str(&ev->sender.node));

    snprintf(path, sizeof(path), MEMBER_DIR"/%s", node_to_str(&ev->sender.node));
    snprintf(queue_pos_path, sizeof(queue_pos_path), MEMBER_QUEUE_POS_DIR "/%s", node_to_str(&ev->sender.node));

    if (node_eq(&ev->sender.node, &this_node.node)) {
        joined = true;
        sd_debug("create path:%s",  path);
        resp = cetcd_set(&etcd_cli, path, (char*)connect_option, EPHERMERAL_TTL);
        if (resp->err) {
            sd_err("create path:%s failed.", path);
        }
        cetcd_response_release(resp);

        sd_debug("create path:%s", queue_pos_path);
        resp = cetcd_set(&etcd_cli, path, (char*)&pos, EPHERMERAL_TTL);
        if (resp->err) {
            sd_err("create path:%s failed.", queue_pos_path);
        }
        cetcd_response_release(resp);

        struct epher_key* member_key = xzalloc(sizeof(struct epher_key));
        struct epher_key* queue_pos_key = xzalloc(sizeof(struct epher_key));
        member_key->key = strdup(path);
        queue_pos_key->key = strdup(queue_pos_path);

        sd_write_lock(&epher_key_lock);
        list_add_tail(&member_key->list, &epher_key_list);
        list_add_tail(&queue_pos_key->list, &epher_key_list);
        sd_rw_unlock(&epher_key_lock);
    } else {
        ;
    }

    etcd_tree_add(&ev->sender);
    build_node_list();
    sd_accept_handler(&ev->sender.node, &sd_node_root, nr_sd_nodes, ev->buf);
}

static void block_event_list_del(struct etcd_node* n) {
    struct etcd_node* ev;

    list_for_each_entry(ev, &etcd_block_list, list) {
        if (node_eq(&ev->node, &n->node)) {
            list_del(&ev->list);
            free(ev);
        }
    }
}

static void etcd_handle_leave(struct etcd_event* ev) {
    struct etcd_node* n = etcd_tree_search(&ev->sender.node.nid);

    if (!n) {
        sd_debug("can't find the leave node:%s, ignore it.", node_to_str(&ev->sender.node));
        return;
    }
    block_event_list_del(n);
    etcd_tree_del(n);
    build_node_list();
    sd_leave_handler(&ev->sender.node, &sd_node_root, nr_sd_nodes);
}

static void etcd_handle_block(struct etcd_event* ev) {
    struct etcd_node* block = xzalloc(sizeof(*block));

    sd_debug("BLOCK");
    block->node = ev->sender.node;
    list_add_tail(&block->list, &etcd_block_list);
    block = list_first_entry(&etcd_block_list, typeof(*block), list);
    if (!block->callbacked) {
        block->callbacked = sd_block_handler(&block->node);
    }
}

static void etcd_handle_unblock(struct etcd_event* ev) {
    struct etcd_node* block;

    sd_debug("UNBLOCK");
    if (list_empty(&etcd_block_list)) {
        return;
    }
    block = list_first_entry(&etcd_block_list, typeof(*block), list);
    sd_notify_handler(&ev->sender.node, ev->buf, ev->buf_len);
    list_del(&block->list);
    free(block);
}

static void etcd_handle_notify(struct etcd_event* ev) {
    sd_debug("NOTIFY");
    sd_notify_handler(&ev->sender.node, ev->buf, ev->buf_len);
}

static const etcd_event_handler_t etcd_event_handlers[] = {
    [EVENT_JOIN]    = etcd_handle_join,
    [EVENT_ACCEPT]  = etcd_handle_accept,
    [EVENT_LEAVE]   = etcd_handle_leave,
    [EVENT_BLOCK]   = etcd_handle_block,
    [EVENT_UNBLOCK] = etcd_handle_unblock,
    [EVENT_NOTIFY]  = etcd_handle_notify,
};

static const int etcd_max_event_handlers = ARRAY_SIZE(etcd_event_handlers);

static void etcd_event_handler(int listen_fd, int events, void* data) {
    struct etcd_event ev;
    bool peek;
    int ret;

    sd_debug("%d, %d", events, this_queue_pos);

    if (events & EPOLLHUP) {
        sd_err("etcd driver received EPOLLHUP event, exiting.");
        log_close();
        exit(1);
    }

    if (events & EPOLLOUT) {
        sd_err("etcd driver received EPOLLOUT event, exiting.");
        log_close();
        exit(1);
    }

    eventfd_xread(efd);

    ret = etcd_queue_peek(&peek);
    if (ret || !peek) {
        return;
    }

    ret = etcd_queue_pop_advance(&ev);
    if (ret) {
        sd_err("etcd queue pop advance failed.");
        return;
    }
    if (ev.type < 0 || ev.type > etcd_max_event_handlers
     || !etcd_event_handlers[ev.type]) {
        panic("unhandled type %d", ev.type);
    }

    etcd_event_handlers[ev.type](&ev);

    ret = etcd_queue_peek(&peek);
    if (peek) {
        eventfd_xwrite(efd, 1);
        return;
    }
}

static int register_event_handler(void) {
    int ret = 0;

    efd = eventfd(0, EFD_NONBLOCK);

    ret = register_event(efd, etcd_event_handler, NULL);

    return ret;
}

static int etcd_init(const char* option) {
    int ret = 0;

    if (!option) {
        sd_err("You must specify etcd servers.");
        return -1;
    }

    // create etcd client
    snprintf(connect_option, PATH_MAX, "%s", option);
    ret = connect_etcd(option);
    if (ret) {
        sd_err("connect etcd failed.");
        goto out;
    }

    // init store structures
    ret = prepare_store();
    if (ret) {
        sd_err("prepare store failed.");
        goto out;
    }

    // setup watchers
    ret = prepare_watchers();
    if (ret) {
        sd_err("prepare watchers failed.");
        goto out;
    }

    // heartbeat ephermeral nodes
    ret = prepare_refresher();
    if (ret) {
        sd_err("prepare refresher failed.");
        goto out;
    }

    uatomic_set_false(&stop);
    uatomic_set_false(&is_master);

    // register event
    ret = register_event_handler();
    if (ret) {
        sd_err("prepare refresher failed.");
    }

out:
    return ret;
}

static int etcd_join(const struct sd_node* myself, void* opaque,
                     size_t opaque_len) {
    int ret;
    char path[PATH_MAX];
    cetcd_response *resp1, *resp2;

    // check exist
    this_node.node = *myself;

    snprintf(path, sizeof(path), MEMBER_DIR "/%s", node_to_str(myself));
    resp1 = cetcd_get(&etcd_cli, path);

    snprintf(path, sizeof(path), MEMBER_QUEUE_POS_DIR "/%s", node_to_str(myself));
    resp2 = cetcd_get(&etcd_cli, path);

    if (!resp1->err || !resp2->err) {
        sd_err("shoot myself.");
        exit(1);
    }

    cetcd_response_release(resp1);
    cetcd_response_release(resp2);

    // compete master
    etcd_compete_master();

    // add join event
    ret = add_join_event(opaque, opaque_len);
    if (ret) {
        sd_err("add join event failed.");
    }
    return ret;
}

static int etcd_leave(void) {
    int ret;
    char path[PATH_MAX];
    char queue_pos_path[PATH_MAX];
    cetcd_response* resp;
    struct epher_key *k, *member_k = NULL, *queue_pos_k = NULL;

    sd_info("leaving cluster");
    uatomic_set_true(&stop);

    // check master and delete master node
    if (uatomic_is_true(&is_master)) {
        snprintf(path, sizeof(path), MASTER_DIR "/%010"PRId32, my_master_seq);
        resp = cetcd_delete(&etcd_cli, path);
        if (resp->err) {
            sd_warn("delete %s failed.", path);
        }
        cetcd_response_release(resp);
    }

    // add leave event
    ret = add_event(EVENT_LEAVE, &this_node, NULL, 0);
    if (ret) {
        sd_err("add leave event failed.");
    }

    // delete member node & queue_pos_path
    snprintf(path, sizeof(path), MEMBER_DIR "/%s", node_to_str(&this_node.node));
    resp = cetcd_delete(&etcd_cli, path);
    if (resp->err) {
        sd_warn("delete %s failed.", path);
    }

    cetcd_response_release(resp);

    snprintf(queue_pos_path, sizeof(queue_pos_path), MEMBER_QUEUE_POS_DIR "/%s", node_to_str(&this_node.node));
    resp = cetcd_delete(&etcd_cli, queue_pos_path);
    if (resp->err) {
        sd_warn("delete %s failed.", queue_pos_path);
    }

    cetcd_response_release(resp);

    // stop refreshing me
    sd_write_lock(&epher_key_lock);
    list_for_each_entry (k, &epher_key_list, list) {
        if (!strcmp(k->key, path)) {
            member_k = k;
        } else if (!strcmp(k->key, queue_pos_path)) {
            queue_pos_k = k;
        }
    }
    if (member_k) list_del(&member_k->list);
    if (queue_pos_k) list_del(&queue_pos_k->list);
    sd_rw_unlock(&epher_key_lock);
    return 0;
}

static int etcd_notify(void* msg, size_t msg_len) {
    return add_event(EVENT_NOTIFY, &this_node, msg, msg_len);
}

static int etcd_block(void) {
    return add_event(EVENT_BLOCK, &this_node, NULL, 0);
}

static int etcd_unblock(void* msg, size_t msg_len) {
    return add_event(EVENT_UNBLOCK, &this_node, msg, msg_len);
}

static uint8_t etcd_block_event_number(void) {
    struct list_node *tmp;
    int num = 0;
    list_for_each(tmp, &etcd_block_list) num++;
    return num > UINT8_MAX ? UINT8_MAX : num;
}

static struct cluster_driver cdrv_etcd = {
    .name               = "etcd",
    .init               = etcd_init,
    .join               = etcd_join,
    .leave              = etcd_leave,
    .notify             = etcd_notify,
    .block              = etcd_block,
    .unblock            = etcd_unblock,
    .get_local_addr     = get_local_addr,
    .block_event_number = etcd_block_event_number,
};

cdrv_register(cdrv_etcd);
