/*
 * RemoteOps Controller (client) - IE3090 Network Programming
 * Registration number : IT24103381   (Agent port 9410, SID:1833)
 *
 * Usage: ./controller_381 [agent_host] [agent_port]
 *
 * Type protocol commands exactly as in the brief:
 *   AUTH OPS-3381 | SYSINFO | LISTPROC | EXEC <name> | MONITOR START <udp_port>
 *   MONITOR STOP | QUIT
 * Convenience forms for file transfer (the Controller adds the size and
 * the raw bytes on the wire):
 *   PUT <local_file>            -> sends "PUT <basename> <size>\n" + bytes
 *   GET <name> [save_as]        -> saves to "dl_<name>" if save_as omitted
 * Optional extension: PUT/GET report throughput in bytes/second.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_PORT 9410
#define LINE_MAX_LEN 8192

static int g_sock = -1;
static char g_rbuf[4096];
static size_t g_rpos, g_rlen;

/* UDP monitor listener state */
static int g_usock = -1;
static pthread_t g_uthread;
static atomic_int g_ustop;
static int g_urunning;

static int send_all(const void *buf, size_t n)
{
    const char *p = buf;
    while (n > 0) {
        ssize_t k = send(g_sock, p, n, MSG_NOSIGNAL);
        if (k < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += k;
        n -= (size_t)k;
    }
    return 0;
}

static int read_line(char *out, size_t max)
{
    size_t o = 0;
    for (;;) {
        while (g_rpos < g_rlen) {
            char c = g_rbuf[g_rpos++];
            if (c == '\n') {
                out[o] = '\0';
                if (o > 0 && out[o - 1] == '\r') out[--o] = '\0';
                return (int)o;
            }
            if (o + 1 < max) out[o++] = c;
        }
        ssize_t k = recv(g_sock, g_rbuf, sizeof g_rbuf, 0);
        if (k < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (k == 0) return -1;
        g_rpos = 0;
        g_rlen = (size_t)k;
    }
}

static ssize_t read_some(void *buf, size_t n)
{
    if (g_rpos < g_rlen) {
        size_t avail = g_rlen - g_rpos;
        if (avail > n) avail = n;
        memcpy(buf, g_rbuf + g_rpos, avail);
        g_rpos += avail;
        return (ssize_t)avail;
    }
    for (;;) {
        ssize_t k = recv(g_sock, buf, n, 0);
        if (k < 0 && errno == EINTR) continue;
        return k;
    }
}

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + ts.tv_nsec / 1e9;
}

/* ---------------- UDP monitor receiver ---------------- */
static void *udp_thread(void *arg)
{
    (void)arg;
    char buf[512];
    while (!atomic_load(&g_ustop)) {
        ssize_t k = recvfrom(g_usock, buf, sizeof buf - 1, 0, NULL, NULL);
        if (k > 0) {
            buf[k] = '\0';
            printf("\n[UDP monitor] %s\n", buf);
            fflush(stdout);
        }
    }
    return NULL;
}

static int start_udp_listener(int port)
{
    if (g_urunning) return 0;
    g_usock = socket(AF_INET, SOCK_DGRAM, 0);
    if (g_usock < 0) return -1;
    int yes = 1;
    setsockopt(g_usock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
    struct timeval tv = { 0, 500000 };            /* wake every 0.5 s */
    setsockopt(g_usock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons((uint16_t)port);
    if (bind(g_usock, (struct sockaddr *)&a, sizeof a) < 0) {
        perror("udp bind");
        close(g_usock);
        g_usock = -1;
        return -1;
    }
    atomic_store(&g_ustop, 0);
    if (pthread_create(&g_uthread, NULL, udp_thread, NULL) != 0) {
        close(g_usock);
        g_usock = -1;
        return -1;
    }
    g_urunning = 1;
    return 0;
}

static void stop_udp_listener(void)
{
    if (!g_urunning) return;
    atomic_store(&g_ustop, 1);
    pthread_join(g_uthread, NULL);
    close(g_usock);
    g_usock = -1;
    g_urunning = 0;
}

/* ---------------- helpers ---------------- */
static int xchg(const char *cmd, char *resp, size_t n)
{
    char line[LINE_MAX_LEN];
    snprintf(line, sizeof line, "%s\n", cmd);
    if (send_all(line, strlen(line)) < 0) return -1;
    if (read_line(resp, n) < 0) return -1;
    return 0;
}

static void lost(void)
{
    fprintf(stderr, "Connection to Agent lost.\n");
    stop_udp_listener();
    exit(1);
}

static void do_put(const char *path)
{
    if (!path) { puts("usage: PUT <local_file>"); return; }
    FILE *f = fopen(path, "rb");
    struct stat st;
    if (!f || stat(path, &st) != 0) { perror(path); if (f) fclose(f); return; }
    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;
    unsigned long long size = (unsigned long long)st.st_size;

    char hdr[512];
    snprintf(hdr, sizeof hdr, "PUT %s %llu\n", base, size);
    double t0 = now_sec();
    if (send_all(hdr, strlen(hdr)) < 0) lost();
    char buf[8192];
    size_t k;
    while ((k = fread(buf, 1, sizeof buf, f)) > 0)
        if (send_all(buf, k) < 0) lost();
    fclose(f);

    char resp[LINE_MAX_LEN];
    if (read_line(resp, sizeof resp) < 0) lost();
    double dt = now_sec() - t0;
    printf("%s\n", resp);
    if (strncmp(resp, "OK", 2) == 0 && dt > 0)
        printf("[PUT] %llu bytes in %.4f s = %.0f bytes/s\n", size, dt,
               size / dt);
}

static void do_get(const char *name, const char *save_as)
{
    if (!name) { puts("usage: GET <name> [save_as]"); return; }
    char cmd[512];
    snprintf(cmd, sizeof cmd, "GET %s\n", name);
    double t0 = now_sec();
    if (send_all(cmd, strlen(cmd)) < 0) lost();
    char resp[LINE_MAX_LEN];
    if (read_line(resp, sizeof resp) < 0) lost();
    printf("%s\n", resp);

    char rname[256];
    unsigned long long size;
    if (sscanf(resp, "OK FILE_SEND %255s %llu", rname, &size) != 2) return;

    char outname[600];
    if (save_as) snprintf(outname, sizeof outname, "%s", save_as);
    else snprintf(outname, sizeof outname, "dl_%s", name);
    FILE *f = fopen(outname, "wb");
    if (!f) { perror(outname); lost(); }   /* must still consume the bytes */

    unsigned long long remaining = size;
    char buf[8192];
    while (remaining > 0) {
        size_t want = remaining > sizeof buf ? sizeof buf : (size_t)remaining;
        ssize_t k = read_some(buf, want);
        if (k <= 0) { fclose(f); lost(); }
        fwrite(buf, 1, (size_t)k, f);
        remaining -= (unsigned long long)k;
    }
    fclose(f);
    double dt = now_sec() - t0;
    printf("[GET] saved %llu bytes to %s in %.4f s = %.0f bytes/s\n", size,
           outname, dt, dt > 0 ? size / dt : 0.0);
}

int main(int argc, char **argv)
{
    const char *host = argc > 1 ? argv[1] : "127.0.0.1";
    int port = argc > 2 ? atoi(argv[2]) : DEFAULT_PORT;

    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    char ps[16];
    snprintf(ps, sizeof ps, "%d", port);
    if (getaddrinfo(host, ps, &hints, &res) != 0) {
        fprintf(stderr, "cannot resolve %s\n", host);
        return 1;
    }
    g_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (g_sock < 0 || connect(g_sock, res->ai_addr, res->ai_addrlen) < 0) {
        perror("connect");
        return 1;
    }
    freeaddrinfo(res);
    printf("Connected to %s:%d. Start with: AUTH <token>\n", host, port);

    int tty = isatty(STDIN_FILENO);
    char in[LINE_MAX_LEN], resp[LINE_MAX_LEN];

    for (;;) {
        if (tty) { printf("remoteops> "); fflush(stdout); }
        if (!fgets(in, sizeof in, stdin)) break;
        in[strcspn(in, "\r\n")] = '\0';
        if (in[0] == '\0') continue;

        char copy[LINE_MAX_LEN];
        strcpy(copy, in);
        char *sp = NULL;
        char *cmd = strtok_r(copy, " ", &sp);
        char *a1 = strtok_r(NULL, " ", &sp);
        char *a2 = strtok_r(NULL, " ", &sp);
        for (char *p = cmd; *p; p++) *p = (char)toupper((unsigned char)*p);

        if (strcmp(cmd, "PUT") == 0) { do_put(a1); continue; }
        if (strcmp(cmd, "GET") == 0) { do_get(a1, a2); continue; }

        if (strcmp(cmd, "MONITOR") == 0 && a1 &&
            strcasecmp(a1, "START") == 0) {
            int up = a2 ? atoi(a2) : 0;
            if (up < 1 || up > 65535) { puts("usage: MONITOR START <udp_port>"); continue; }
            if (start_udp_listener(up) < 0) { puts("cannot open UDP port"); continue; }
            char line[64];
            snprintf(line, sizeof line, "MONITOR START %d", up);
            if (xchg(line, resp, sizeof resp) < 0) lost();
            printf("%s\n", resp);
            if (strncmp(resp, "OK", 2) != 0) stop_udp_listener();
            continue;
        }
        if (strcmp(cmd, "MONITOR") == 0 && a1 && strcasecmp(a1, "STOP") == 0) {
            if (xchg("MONITOR STOP", resp, sizeof resp) < 0) lost();
            printf("%s\n", resp);
            stop_udp_listener();
            continue;
        }

        /* Everything else is sent as typed (so error cases can be tested). */
        if (xchg(in, resp, sizeof resp) < 0) lost();
        printf("%s\n", resp);
        if (strcmp(cmd, "QUIT") == 0) break;
    }

    stop_udp_listener();
    close(g_sock);
    return 0;
}

