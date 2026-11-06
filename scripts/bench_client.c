// ============================================================================
// bench_client.c — 真实跨连接 RPC 压测工具
// 用法：./bench_client <host> <port> <N> <M>
//   N 个 provider 各注册一个独立服务，N 个 consumer 各发起 M 次请求，
//   每次请求走「consumer → 网关 → 另一条连接的 provider → 网关 → consumer」
//   完整链路，并校验 RESPONSE 的 client_id 与 error_code，统计成功率/延迟。
// 返回码：成功率 < 99% 时返回 1（供 CI 断言）。
// ============================================================================
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define K_MAGIC      0xABCDEF01u
#define BASE_SERVICE 0x00001000u
#define MSG_REQUEST  0x00
#define MSG_RESPONSE 0x02
#define MSG_REGISTER 0x08
#define MSG_HEARTBEAT 0x10
#define ERR_OK       0x00
#define MAX_PAYLOAD  (1u << 20)
#define HB_INTERVAL_US 1000000   /* provider 心跳间隔 1s（< 网关 hb_timeout） */

typedef struct {
    uint32_t magic;
    uint32_t service_id;
    uint32_t method_id;
    uint32_t length;
    uint32_t client_id;
    uint8_t  msg_type;
    uint8_t  error_code;
    uint8_t  version;
    uint8_t  reserved[2];
} __attribute__((packed)) Header;   /* sizeof == 25，与 include/protocol.h 保持一致 */

static const char* g_host;
static int g_port;
static int g_n;            /* provider 数 = consumer 数 */
static int g_per;          /* 每个 consumer 的请求数 */
static volatile int g_stop = 0;
static volatile int g_registered = 0;
static volatile int64_t g_ok = 0;
static volatile int64_t g_lat_us = 0;

/* ---- 字节序（大端，与协议一致）------------------------------------------ */
static int is_little_endian(void) {
    const uint16_t one = 1;
    return *(const uint8_t*)&one == 1;
}
static uint32_t host_to_net32(uint32_t v) {
    if (!is_little_endian()) return v;
    return ((v & 0x000000FFu) << 24) |
           ((v & 0x0000FF00u) << 8)  |
           ((v & 0x00FF0000u) >> 8)  |
           ((v & 0xFF000000u) >> 24);
}
static void header_hton(Header* h) {
    h->magic      = host_to_net32(h->magic);
    h->service_id = host_to_net32(h->service_id);
    h->method_id  = host_to_net32(h->method_id);
    h->length     = host_to_net32(h->length);
    h->client_id  = host_to_net32(h->client_id);
}
static void header_ntoh(Header* h) { header_hton(h); }  /* 转换自反 */

/* ---- 基础 IO ------------------------------------------------------------- */
static int64_t now_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

static int connect_tcp(const char* host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd); return -1;
    }
    return fd;
}

/* 带重试的连接（网关可能刚启动，listen backlog 未就绪） */
static int connect_tcp_retry(const char* host, int port) {
    for (int a = 0; a < 20; ++a) {
        int fd = connect_tcp(host, port);
        if (fd >= 0) return fd;
        usleep(50000);
    }
    return -1;
}

static int readn(int fd, void* buf, int n) {
    int got = 0;
    while (got < n) {
        ssize_t r = read(fd, (char*)buf + got, n - got);
        if (r <= 0) return -1;
        got += (int)r;
    }
    return got;
}

static int write_all(int fd, const void* buf, size_t n) {
    const char* p = (const char*)buf;
    while (n > 0) {
        ssize_t w = write(fd, p, n);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        p += w; n -= (size_t)w;
    }
    return 0;
}

static int write_frame(int fd, Header h, const uint8_t* payload, uint32_t plen) {
    header_hton(&h);
    if (write_all(fd, &h, sizeof(Header)) != 0) return -1;
    if (plen > 0 && write_all(fd, payload, plen) != 0) return -1;
    return 0;
}

/* 读一帧：返回 0 成功，-1 失败；payload 由调用方 free（可传 NULL） */
static int read_frame(int fd, Header* h, uint8_t** payload, uint32_t* plen) {
    uint8_t hbuf[sizeof(Header)];
    if (readn(fd, hbuf, sizeof(Header)) != (int)sizeof(Header)) return -1;
    memcpy(h, hbuf, sizeof(Header));
    header_ntoh(h);
    if (h->magic != K_MAGIC) return -1;
    if (h->length > MAX_PAYLOAD) return -1;
    *plen = h->length;
    if (*plen > 0) {
        *payload = (uint8_t*)malloc(*plen);
        if (!*payload) return -1;
        if (readn(fd, *payload, (int)*plen) != (int)*plen) { free(*payload); *payload = NULL; return -1; }
    } else {
        *payload = NULL;
    }
    return 0;
}

/* ---- Provider：注册服务 + 心跳保活 + 响应请求 ------------------------------ */
static void* provider_main(void* arg) {
    long idx = (long)arg;
    uint32_t svc = BASE_SERVICE + (uint32_t)idx;

    int fd = connect_tcp_retry(g_host, g_port);
    if (fd < 0) return NULL;

    /* REGISTER：payload = service_id（大端 4 字节） */
    Header hreg = {0};
    hreg.magic = K_MAGIC;
    hreg.service_id = svc;
    hreg.length = 4;
    hreg.client_id = 0x80000000u + (uint32_t)idx;
    hreg.msg_type = MSG_REGISTER;
    uint8_t reg_payload[4] = {
        (uint8_t)(svc >> 24), (uint8_t)(svc >> 16), (uint8_t)(svc >> 8), (uint8_t)svc
    };
    if (write_frame(fd, hreg, reg_payload, 4) != 0) { close(fd); return NULL; }

    /* 读 ACK */
    Header ack; uint8_t* pp = NULL; uint32_t plen = 0;
    if (read_frame(fd, &ack, &pp, &plen) != 0) { close(fd); return NULL; }
    if (pp) free(pp);
    __sync_add_and_fetch(&g_registered, 1);

    /* 服务循环：poll 500ms 超时用于空闲唤醒；无论有无流量都周期性发心跳，
     * 避免压测期间请求不断、provider 忙到不发心跳而被网关超时误踢。 */
    int64_t last_hb = now_us();
    struct pollfd pfd = { fd, POLLIN, 0 };
    while (!g_stop) {
        int r = poll(&pfd, 1, 500);
        if (r < 0) { if (errno == EINTR) continue; break; }
        if (r > 0) {
            Header req; uint8_t* p = NULL; uint32_t l = 0;
            if (read_frame(fd, &req, &p, &l) != 0) break;
            if (p) { free(p); p = NULL; }
            if (req.msg_type == MSG_REQUEST) {
                Header rsp = req;
                rsp.msg_type = MSG_RESPONSE;
                rsp.error_code = ERR_OK;
                rsp.length = 0;
                if (write_frame(fd, rsp, NULL, 0) != 0) break;
            }
        }
        if (now_us() - last_hb > HB_INTERVAL_US) {
            Header hb = {0};
            hb.magic = K_MAGIC;
            hb.msg_type = MSG_HEARTBEAT;
            if (write_frame(fd, hb, NULL, 0) != 0) break;
            last_hb = now_us();
        }
    }
    close(fd);
    return NULL;
}

/* ---- Consumer：向独立 provider 发请求，校验完整 RESPONSE ------------------ */
static void* consumer_main(void* arg) {
    long idx = (long)arg;
    uint32_t svc = BASE_SERVICE + (uint32_t)idx;

    int fd = connect_tcp_retry(g_host, g_port);
    if (fd < 0) return NULL;

    for (int k = 0; k < g_per && !g_stop; ++k) {
        uint32_t cid = (uint32_t)(idx * g_per + k);
        Header req = {0};
        req.magic = K_MAGIC;
        req.service_id = svc;
        req.method_id = 1;
        req.length = 0;
        req.client_id = cid;
        req.msg_type = MSG_REQUEST;

        int64_t t0 = now_us();
        if (write_frame(fd, req, NULL, 0) != 0) break;

        Header rsp; uint8_t* p = NULL; uint32_t l = 0;
        if (read_frame(fd, &rsp, &p, &l) == 0 &&
            rsp.magic == K_MAGIC &&
            rsp.msg_type == MSG_RESPONSE &&
            rsp.error_code == ERR_OK &&
            rsp.client_id == cid) {
            __sync_add_and_fetch(&g_ok, 1);
            __sync_add_and_fetch(&g_lat_us, now_us() - t0);
        }
        if (p) free(p);
    }
    close(fd);
    return NULL;
}

/* ---- main ---------------------------------------------------------------- */
int main(int argc, char** argv) {
    if (argc < 5) {
        fprintf(stderr, "usage: %s host port N M   (N providers + N consumers, M req/consumer)\n", argv[0]);
        return 2;
    }
    g_host = argv[1];
    g_port = atoi(argv[2]);
    g_n    = atoi(argv[3]);
    g_per  = atoi(argv[4]);
    if (g_n <= 0 || g_per <= 0) return 2;

    int64_t t0 = now_us();
    pthread_t* prov = (pthread_t*)malloc(sizeof(pthread_t) * g_n);
    pthread_t* cons = (pthread_t*)malloc(sizeof(pthread_t) * g_n);
    if (!prov || !cons) return 2;

    for (long i = 0; i < g_n; ++i) pthread_create(&prov[i], NULL, provider_main, (void*)i);

    /* 等所有 provider 注册完成（最多 5s） */
    int waited = 0;
    while (__sync_add_and_fetch(&g_registered, 0) < g_n && waited < 5000) {
        usleep(10000); waited += 10;
    }

    for (long i = 0; i < g_n; ++i) pthread_create(&cons[i], NULL, consumer_main, (void*)i);
    for (long i = 0; i < g_n; ++i) pthread_join(cons[i], NULL);

    g_stop = 1;
    for (long i = 0; i < g_n; ++i) pthread_join(prov[i], NULL);

    int64_t elapsed_us = now_us() - t0;
    free(prov); free(cons);

    int64_t ok = g_ok;
    int64_t total = (int64_t)g_n * g_per;
    double elapsed_s = (double)elapsed_us / 1e6;
    double avg_lat_ms = ok ? (g_lat_us / (double)ok) / 1000.0 : 0.0;

    printf("== Bench Result (cross-connection RPC) ==\n");
    printf("  providers=%d  consumers=%d  req_per_consumer=%d\n", g_n, g_n, g_per);
    printf("  elapsed: %.3f s\n", elapsed_s);
    printf("  ok reqs: %ld / %ld (%.2f%%)\n", (long)ok, (long)total, total ? (100.0 * ok / total) : 0.0);
    printf("  QPS:     %.0f req/s\n", elapsed_s > 0 ? ok / elapsed_s : 0.0);
    printf("  avg latency: %.3f ms\n", avg_lat_ms);

    /* 成功率门槛：< 99% 判失败，供 CI 断言 */
    if (total > 0 && ok * 100 < total * 99) return 1;
    return 0;
}
