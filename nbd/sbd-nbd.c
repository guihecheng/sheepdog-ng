#include "sheepdog.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <getopt.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <linux/nbd.h>
#include <pthread.h>
#include <syslog.h>

typedef enum {
    SBD_NBD_LOG_CRIT   = 1,
    SBD_NBD_LOG_ERR    = 3,
    SBD_NBD_LOG_WARN   = 7,
    SBD_NBD_LOG_INFO   = 15,
    SBD_NBD_LOG_DEBUG  = 31,
} log_level_t;

#define SYSLOG_LEVEL(level)                                              \
        ((level == SBD_NBD_LOG_DEBUG) ? LOG_DEBUG :                      \
         (level == SBD_NBD_LOG_INFO)  ? LOG_INFO :                       \
         (level == SBD_NBD_LOG_WARN)  ? LOG_WARNING :                    \
         (level == SBD_NBD_LOG_ERR)   ? LOG_ERR : LOG_CRIT)

#define DoLog(level, fmt, ...)                                         \
    do {                                                               \
        if ((wanted_level & level) == level) {                          \
            syslog(SYSLOG_LEVEL(level), "[%s]%s:%s(%d): " fmt "%s", "sbd-nbd", __FILE__, __func__, __LINE__, __VA_ARGS__);   \
        }                                                              \
    } while(0)

#define LogCrit(fmt, ...)  DoLog(SBD_NBD_LOG_CRIT,  fmt, ##__VA_ARGS__, "")
#define LogErr(fmt, ...)   DoLog(SBD_NBD_LOG_ERR,   fmt, ##__VA_ARGS__, "")
#define LogWarn(fmt, ...)  DoLog(SBD_NBD_LOG_WARN,  fmt, ##__VA_ARGS__, "")
#define LogInfo(fmt, ...)  DoLog(SBD_NBD_LOG_INFO,  fmt, ##__VA_ARGS__, "")
#define LogDebug(fmt, ...) DoLog(SBD_NBD_LOG_DEBUG, fmt, ##__VA_ARGS__, "")

static log_level_t wanted_level = SBD_NBD_LOG_INFO;

#define SBD_NBD_BLKSIZE   512UL

typedef enum {
    OP_MAP,
    OP_UNMAP,
    OP_LIST,
    OP_UNKNOWN,
    OP_MAX,
} operation_t;

static operation_t op = OP_UNKNOWN;

static int nbds_max = -1;
static int max_part = -1;
static char* endpoint = (char*)"127.0.0.1:7000";
static char* vdi_name = NULL;
static char* dev_path = NULL;
static bool readonly = false;
static bool json = false;

static uatomic_bool terminated;

static int nbd_fd = -1;
static int nbd_idx = -1;

static struct sd_cluster* cluster;
static struct sd_vdi* vdi;
static uint64_t vdi_size;
static uint64_t nbd_flags = NBD_FLAG_HAS_FLAGS;
static int fds[2];

// -----------------
// io request queue
struct io_context {
    struct list_node io_list;
    struct nbd_request req;
    struct nbd_reply   rep;
    int cmd;
    void* data;
};

static LIST_HEAD(io_pending);
static LIST_HEAD(io_finished);
static pthread_mutex_t io_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t io_cv = PTHREAD_COND_INITIALIZER;

static void io_start(struct io_context* ctx) {
    pthread_mutex_lock(&io_mtx);
    list_add_tail(&ctx->io_list, &io_pending);
    pthread_mutex_unlock(&io_mtx);
}

static void io_finish(struct io_context* ctx) {
    pthread_mutex_lock(&io_mtx);
    list_del(&ctx->io_list);
    list_add_tail(&ctx->io_list, &io_finished);
    pthread_cond_signal(&io_cv);
    pthread_mutex_unlock(&io_mtx);
}

static struct io_context* wait_io_finish(void) {
    pthread_mutex_lock(&io_mtx);
    while (list_empty(&io_finished) && !uatomic_is_true(&terminated)) {
        pthread_cond_wait(&io_cv, &io_mtx);
    }

    if (list_empty(&io_finished)) {
        pthread_mutex_unlock(&io_mtx);
        return NULL;
    }

    struct io_context* ctx = list_first_entry(&io_finished,
                                              struct io_context, io_list);
    list_del(&ctx->io_list);
    pthread_mutex_unlock(&io_mtx);
    return ctx;
}

static void wait_for_cleanup(void) {
    pthread_mutex_lock(&io_mtx);
    while (!list_empty(&io_pending)) {
        pthread_cond_wait(&io_cv, &io_mtx);
    }
    while (!list_empty(&io_finished)) {
        struct io_context* ctx = list_first_entry(&io_finished,
                                                  struct io_context, io_list);
        list_del(&ctx->io_list);
        free(ctx);
    }
    pthread_mutex_unlock(&io_mtx);
}

// -----------------
// reader & writer
static void aio_done(void *opaque, int ret) {
    struct io_context* ctx = (struct io_context*)opaque;

    ctx->rep.error = htobe32(ret);
    io_finish(ctx);
}

static void* reader_routine(void* arg) {
    LogInfo("reader running...");

    while (!uatomic_is_true(&terminated)) {
        struct io_context* ctx = malloc(sizeof(struct io_context));

        int ret = xread(fds[1], &ctx->req, sizeof(struct nbd_request));
        if (ret < 0) {
            LogErr("failed to read nbd request %d", ret);
            goto out;
        }

        if (ctx->req.magic != htobe32(NBD_REQUEST_MAGIC)) {
            LogErr("invalid request received");
            goto out;
        }

        ctx->req.from = be64toh(ctx->req.from);
        ctx->req.type = be32toh(ctx->req.type);
        ctx->req.len = be32toh(ctx->req.len);

        ctx->rep.magic = htobe32(NBD_REPLY_MAGIC);
        memcpy(ctx->rep.handle, ctx->req.handle, sizeof(ctx->rep.handle));
        ctx->cmd = ctx->req.type & 0x0000ffff;

        switch (ctx->cmd) {
            case NBD_CMD_DISC:
                goto out;
            case NBD_CMD_WRITE:
                ctx->data = malloc(ctx->req.len);
                ret = xread(fds[1], ctx->data, ctx->req.len);
                if (ret < 0) {
                    LogErr("failed to read data for write %d", ret);
                    goto out;
                }
                break;
            case NBD_CMD_READ:
                // fill zero for short read
                ctx->data = calloc(1, ctx->req.len);
            default:
                break;
        }

        // do sheepdog request
        io_start(ctx);

        switch (ctx->cmd) {
            case NBD_CMD_WRITE:
                sd_vdi_awrite(vdi, ctx->data, ctx->req.len, ctx->req.from,
                              aio_done, ctx);
                break;
            case NBD_CMD_READ:
                sd_vdi_aread(vdi, ctx->data, ctx->req.len, ctx->req.from,
                             aio_done, ctx);
                break;
            default:
                LogWarn("invalid command %d", (int)ctx->cmd);
                break;
        }
    }
out:
    return NULL;
}

static void* writer_routine(void* arg) {
    LogInfo("writer running...");

    while (!uatomic_is_true(&terminated)) {
        struct io_context* ctx = wait_io_finish();
        if (!ctx) {
            LogInfo("no more io requests");
            goto out;
        }

        int ret = xwrite(fds[1], &ctx->rep, sizeof(struct nbd_reply));
        if (ret < 0) {
            LogErr("faild to write nbd reply %d", ret);
            goto out;
        }

        if (ctx->cmd == NBD_CMD_READ && ctx->rep.error == htobe32(0)) {
            // write data to fds[1]
            ret = xwrite(fds[1], ctx->data, ctx->req.len);
            if (ret < 0) {
                LogErr("faild to write nbd data back %d", ret);
                goto out;
            }
        }

        // ctx completed
        free(ctx);
    }
out:
    return NULL;
}

static pthread_t reader_thread;
static pthread_t writer_thread;

static void start_reader(void) {
    int ret;

    ret = pthread_create(&reader_thread, NULL, reader_routine, NULL);
    if (ret) {
        LogCrit("failed to create reader thread");
    }
}

static void join_reader(void) {
    pthread_join(reader_thread, NULL);
}

static void start_writer(void) {
    int ret;

    ret = pthread_create(&writer_thread, NULL, writer_routine, NULL);
    if (ret) {
        LogCrit("failed to create reader thread");
    }
}

static void join_writer(void) {
    pthread_join(writer_thread, NULL);
}

static void terminate(void) {
    if (uatomic_cmpxchg(&(&terminated)->val, 0, 1) == 0) {
        shutdown(fds[1], SHUT_RDWR);

        // notify reader
        pthread_mutex_lock(&io_mtx);
        pthread_cond_signal(&io_cv);
        pthread_mutex_unlock(&io_mtx);
    }
}

// -----------------
// nbd setup
static int module_load(const char* module, const char* params) {
    int ret;
    char cmd[128];

    snprintf(cmd, sizeof(cmd), "/sbin/modprobe %s %s", module, params);

    ret = system(cmd);
    if (ret >= 0 && WIFEXITED(ret)) {
        return WEXITSTATUS(ret);
    }
    return -1;
}

static int load_nbd_module(void) {
    int ret = 0;
    char params[64] = {0,};

    if (!access("sys/module/nbd", F_OK)) {
        fprintf(stderr, "warning: nbd already loaded, parameters ignored");
        return ret;
    }

    if (nbds_max > 0) {
        char p[32];
        snprintf(p, 32, "nbds_max=%d ", nbds_max);
        strncat(params, p, strlen(p));
    }

    if (max_part > 0) {
        char p[32];
        snprintf(p, 32, "max_part=%d", max_part);
        strncat(params, p, strlen(p));
    }

    return module_load("nbd", params);
}

static char* read_sys_param(const char* path) {
    char* val = NULL;
    int fd;
    int ret;

    fd = open(path, O_RDONLY);
    if (fd < 0) {
        goto out;
    }

    val = malloc(sizeof(char)*32);
    ret = read(fd, val, 32);
    if (ret <= 0) {
        free(val);
        val = NULL;
    } else {
        val[ret] = '\0';
    }

    close(fd);
out:
    return val;
}

static char* find_unused_device(int fd) {
    int ret;
    char* dev = NULL;
    const char* bound_path = "/sys/module/nbd/parameters/nbds_max";
    int bound = -1;
    int tmp_fd;

    if (!access(bound_path, F_OK)) {
        bound = atoi(read_sys_param(bound_path));
    }

    if (bound == -1) {
        goto out;
    }

    dev = malloc(sizeof(char)*32);

    for (int i = 0; i < bound; i++) {
        snprintf(dev, 32, "/dev/nbd%d", i);

        tmp_fd = open(dev, O_RDWR);
        if (tmp_fd < 0) {
            continue;
        }

        ret = ioctl(tmp_fd, NBD_SET_SOCK, fd);
        if (ret < 0) {
            close(tmp_fd);
            continue;
        }

        // found unused one
        return dev;
    }

    free(dev);
out:
    return NULL;
}

static int parse_nbd_index(const char* dev) {
    int ret;
    int idx;

    ret = sscanf(dev, "/dev/nbd%d", &idx);
    if (ret <= 0) {
        if (ret == 0) {
            ret = -EINVAL;
        }
        return ret;
    }
    return idx;
}

static int ioctl_nbd_setup(int fd, uint64_t size, uint64_t flags) {
    int ret = 0;
    int idx;

    if (!dev_path) {
        dev_path = find_unused_device(fd);
    }

    if (!dev_path) {
        ret = -1;
        fprintf(stderr, "error: no unused device available\n");
        goto out;
    }

    ret = parse_nbd_index(dev_path);
    if (ret < 0) {
        fprintf(stderr, "error: invalid device path: %s\n", dev_path);
        goto out;
    }

    idx = ret;

    nbd_fd = open(dev_path, O_RDWR);
    if (nbd_fd < 0) {
        ret = -errno;
        fprintf(stderr, "error: faild to open device: %s\n", dev_path);
        goto out;
    }

    ret = ioctl(nbd_fd, NBD_SET_SOCK, fd);
    if (ret < 0) {
        ret = -errno;
        fprintf(stderr, "error: failed to set sock: %s\n", dev_path);
        goto close;
    }

    ret = ioctl(nbd_fd, NBD_SET_BLKSIZE, SBD_NBD_BLKSIZE);
    if (ret < 0) {
        ret = -errno;
        fprintf(stderr, "error: failed to set block size: %s\n", dev_path);
        goto clear;
    }

    ret = ioctl(nbd_fd, NBD_SET_SIZE, size);
    if (ret < 0) {
        ret = -errno;
        fprintf(stderr, "error: failed to set size: %s\n", dev_path);
        goto clear;
    }

    ret = ioctl(nbd_fd, NBD_SET_FLAGS, flags);
    if (ret < 0) {
        ret = -errno;
        fprintf(stderr, "error: failed to set flags: %s\n", dev_path);
        goto clear;
    }

    nbd_idx = idx;
out:
    return ret;

clear:
    ioctl(nbd_fd, NBD_CLEAR_SOCK);
close:
    close(nbd_fd);
    return ret;
}

static void wait_for_disconnect(void) {
    ioctl(nbd_fd, NBD_DO_IT);
}

static void log_init(void) {
    openlog("sbd-nbd", LOG_PID | LOG_ODELAY | LOG_NOWAIT, LOG_USER);
}

static void log_exit(void) {
    closelog();
}

// -----------------
// do map
static int do_map(void) {
    int ret = 0;

    // unix socketpair
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == -1) {
        ret = -errno;
        goto out;
    }

    // connect to cluster
    cluster = sd_connect(endpoint);
    if (!cluster) {
        ret = -1;
        fprintf(stderr, "error: failed to connect to cluster %s\n", endpoint);
        goto close;
    }

    // open vdi
    vdi = sd_vdi_open(cluster, vdi_name, NULL);
    if (!cluster) {
        ret = -1;
        fprintf(stderr, "error: failed to open vdi %s\n", vdi_name);
        goto disconnect;
    }

    // get vdi size
    vdi_size = sd_vdi_getsize(vdi);

    // load nbd module
    ret = load_nbd_module();
    if (ret < 0) {
        fprintf(stderr, "error: failed to load nbd kernel module: %d\n", ret);
        goto close_vdi;
    }

    if (readonly) {
        nbd_flags |= NBD_FLAG_READ_ONLY;
    }

    // nbd setup
    ret = ioctl_nbd_setup(fds[0], vdi_size, nbd_flags);
    if (ret < 0) {
        fprintf(stderr, "error: failed to setup nbd via ioctl: %d\n", ret);
        goto close_vdi;
    }

    // daemonize
    ret = daemon(0, 0);
    if (ret < 0) {
        fprintf(stderr, "error: daemon failed: %d\n", ret);
        goto close_vdi;
    }

    log_init();

    // start reader & writer
    uatomic_set_false(&terminated);
    start_reader();
    start_writer();

    // wait for disconnect
    wait_for_disconnect();

    terminate();

    join_reader();
    join_writer();

    // wait for pending io done
    wait_for_cleanup();
close_vdi:
    sd_vdi_close(vdi);
disconnect:
    sd_disconnect(cluster);
close:
    close(fds[0]);
    close(fds[1]);
    log_exit();
out:
    return ret;
}

// -----------------
// do unmap
static int do_unmap(void) {
    int ret = 0;

    nbd_fd = open(dev_path, O_RDWR);
    if (nbd_fd < 0) {
        ret = -errno;
        fprintf(stderr, "error: failed to open device: %d\n", ret);
        goto out;
    }

    ret = ioctl(nbd_fd, NBD_DISCONNECT);
    if (ret < 0) {
        ret = -errno;
        fprintf(stderr, "error: failed to disconnect: %d\n", ret);
        goto out;
    }

    ret = ioctl(nbd_fd, NBD_CLEAR_SOCK);
    if (ret < 0) {
        ret = -errno;
        fprintf(stderr, "error: failed to clear sock: %d\n", ret);
        goto out;
    }

    close(nbd_fd);
out:
    return ret;
}

// -----------------
// do list
static int do_list(void) {
    int ret = 0;
    return ret;
}

static const char* help = 
"Usage: sbd-nbd [options] map   <vdi>            Map an vdi to nbd device\n"
"                         unmap <device>         Unmap nbd device\n"
"               [options] list                   List mapped nbd devices\n"
"Map options:\n"
"  -e,--endpoint <endpoint>    Specify the sheepdog cluster endpoint(e.g. 127.0.0.1:7000)\n"
"  -d,--device <device path>   Specify nbd device path (/dev/nbd{num})\n"
"  -r,--read-only              Map read-only\n"
"  -n,--nbds_max <limit>       Override module param nbds_max\n"
"  -p,--max_part <limit>       Overried module param max_part\n"
"\n"
"List options:\n"
"  -j,--json\n";

static void usage(void) {
    printf("%s\n", help);
}

static void parse_args(int argc, char* argv[]) {
    if (strcmp(argv[1], "map") == 0) {
        op = OP_MAP;
        if (argc < 3) {
            fprintf(stderr, "error: no vdi specified for map\n");
            exit(-1);
        }
        vdi_name = strdup(argv[2]);
    } else if (strcmp(argv[1], "unmap") == 0) {
        op = OP_UNMAP;
        if (argc < 3) {
            fprintf(stderr, "error: no vdi | device specified for unmap\n");
            exit(-1);
        }
        dev_path = strdup(argv[2]);
    } else if (strcmp(argv[1], "list") == 0) {
        op = OP_LIST;
    } else {
        op = OP_UNKNOWN;
        fprintf(stderr, "error: unknown operation: %s\n", argv[1]);
        exit(-1);
    }

    static const char* short_options = "e:d:rn:p:j";
    static struct option long_options[] = {
        {"endpoint",  required_argument, 0, 'e'},
        {"device",    required_argument, 0, 'd'},
        {"read-only", no_argument,       0, 'r'},
        {"nbds_max",  required_argument, 0, 'n'},
        {"max_part",  required_argument, 0, 'p'},
        {"json",      no_argument,       0, 'j'},
        {0,0,0,0},
    };

    int c;
    int idx;

    while (true) {
        c = getopt_long(argc, argv, short_options, long_options, &idx);
        if (c == -1) {
            break;
        }

        switch(c) {
            case 'e':
                endpoint = strdup(optarg);
                break;
            case 'd':
                dev_path = strdup(optarg);
                break;
            case 'r':
                readonly = true;
                break;
            case 'n':
                nbds_max = atoi(optarg);
                break;
            case 'p':
                max_part = atoi(optarg);
                break;
            case 'j':
                json = true;
                break;
            default:
                break;
        }
    }
}

int main(int argc, char* argv[]) {
    int ret = 0;

    if (argc < 2) {
        usage();
        goto out;
    }

    parse_args(argc, argv);

    if (op == OP_MAP) {
        ret = do_map();
    } else if (op == OP_UNMAP) {
        ret = do_unmap();
    } else {
        ret = do_list();
    }

out:
    return ret;
}
