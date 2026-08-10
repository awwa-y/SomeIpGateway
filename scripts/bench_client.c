// ============================================================================
// bench_client.c — 轻量压测工具：N连接M请求，打印QPS和平均延迟
// 用法：./bench_client 127.0.0.1 9090 100 1000    # 100连接，每连接1000请求
// ============================================================================
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static const uint32_t kMagic = 0xABCDEF01;

typedef struct {
    uint32_t magic;
    uint32_t service_id;
    uint32_t method_id;
    uint32_t length;
    uint32_t client_id;
    uint8_t  msg_type;
    uint8_t  error_code;
} __attribute__((packed)) Header;

static const char* g_host;
static int g_port;
static int g_conns;
static int g_per_conn;
static volatile int64_t g_total_ok = 0;
static volatile int64_t g_total_latency_us = 0;

static int64_t nowUs(void) {
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

static int readn(int fd, void* buf, int n) {
    int got = 0;
    while (got < n) {
        ssize_t r = read(fd, (char*)buf + got, n - got);
        if (r <= 0) return -1;
        got += (int)r;
    }
    return got;
}

static void* worker(void* arg) {
    (void)arg;
    for (int c = 0; c < g_per_conn; ++c) {
        int fd = connect_tcp(g_host, g_port);
        if (fd < 0) { usleep(1000); continue; }
        // 先发 REGISTER（模拟 Provider）再发 REQUEST
        Header hreg = {kMagic, 0x00001000 + c, 0, 4, (uint32_t)pthread_self()*1000+(uint32_t)c, 0x08, 0};
        uint8_t payload[4] = {0x00,0x00,0x10,(uint8_t)c};
        write(fd, &hreg, 25);
        write(fd, payload, 4);
        Header ack; readn(fd, &ack, 25);

        int64_t t0 = nowUs();
        Header req = {kMagic, 0x00001000 + c, 1, 0, (uint32_t)pthread_self()*1000+(uint32_t)c, 0x00, 0};
        write(fd, &req, 25);
        Header rsp;
        if (readn(fd, &rsp, 25) > 0 && rsp.magic == kMagic) {
            int64_t lat = nowUs() - t0;
            __sync_add_and_fetch(&g_total_ok, 1);
            __sync_add_and_fetch(&g_total_latency_us, lat);
        }
        close(fd);
    }
    return NULL;
}

int main(int argc, char** argv) {
    if (argc < 5) {
        fprintf(stderr, "usage: %s host port conns req_per_conn\n", argv[0]);
        return 1;
    }
    g_host     = argv[1];
    g_port     = atoi(argv[2]);
    g_conns    = atoi(argv[3]);
    g_per_conn = atoi(argv[4]);

    int64_t t0 = nowUs();
    pthread_t* ts = malloc(sizeof(pthread_t) * g_conns);
    for (int i = 0; i < g_conns; ++i) pthread_create(&ts[i], NULL, worker, NULL);
    for (int i = 0; i < g_conns; ++i) pthread_join(ts[i], NULL);
    int64_t elapsed_us = nowUs() - t0;
    free(ts);

    double elapsed_s = (double)elapsed_us / 1e6;
    int64_t ok = g_total_ok;
    double avg_lat_ms = ok ? (g_total_latency_us / (double)ok) / 1000.0 : 0;
    printf("== Bench Result ==\n");
    printf("  conns=%d  req_per_conn=%d\n", g_conns, g_per_conn);
    printf("  elapsed: %.3f s\n", elapsed_s);
    printf("  ok reqs: %ld\n", (long)ok);
    printf("  QPS:     %.0f req/s\n", elapsed_s > 0 ? ok / elapsed_s : 0);
    printf("  avg latency: %.3f ms\n", avg_lat_ms);
    return 0;
}
