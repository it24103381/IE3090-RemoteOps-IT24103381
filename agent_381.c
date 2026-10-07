/*
 * RemoteOps Agent (server) - IE3090 Network Programming
 * Registration number : IT24103381
 *
 * Personalised values (derived from IT24103381):
 *   Listening port : 7000 + 2410      = 9410
 *   SID tag        : 3381 reversed    = SID:1833
 *   Auth token     : OPS-3381
 *   Log file       : remoteops_IT24103381.log
 *   Storage path   : ./agentfiles/IT24103381/<filename>
 *
 * Concurrency model: one detached POSIX thread per Controller connection.
 * Each connection has its own session_t (no shared mutable state except the
 * log file, which is protected by a mutex).
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/* ---- personalised constants ---- */
#define REG_NO        "IT24103381"
#define LISTEN_PORT   9410
#define SID           "1833"
#define AUTH_TOKEN    "OPS-3381"
#define LOG_FILE      "remoteops_IT24103381.log"
#define STORE_ROOT    "./agentfiles"
#define STORE_DIR     "./agentfiles/IT24103381"

/* ---- tunables ---- */
#define MAX_FILE_SIZE       (10UL * 1024 * 1024)   /* 10 MB upload limit   */
#define DRAIN_LIMIT         (200UL * 1024 * 1024)  /* never drain more     */
#define MONITOR_INTERVAL_MS 2000                   /* UDP stats every 2 s  */
#define RBUF_SIZE           4096
#define LINE_MAX_LEN        1024
#define OUT_MAX             4096

typedef struct {
    int fd;
    struct sockaddr_in peer;
    char peer_str[64];
    int authed;
    int quit_clean;
    char rbuf[RBUF_SIZE];        /* receive buffer (framing)               */
    size_t rpos, rlen;
    int mon_active;
    int mon_port;
    pthread_t mon_thread;
    atomic_int mon_stop;
} session_t;

static volatile sig_atomic_t g_running = 1;
static FILE *g_log;
static pthread_mutex_t g_log_mu = PTHREAD_MUTEX_INITIALIZER;

/* ------------------------------------------------------------------ */
/* Logging: every line timestamped, written to file and stdout         */
/* ------------------------------------------------------------------ */
static void log_msg(const char *fmt, ...)
{
    char ts[32];
    time_t t = time(NULL);
    struct tm tmv;
    localtime_r(&t, &tmv);
    strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", &tmv);

    pthread_mutex_lock(&g_log_mu);
    va_list ap;
    if (g_log) {
        va_start(ap, fmt);
        fprintf(g_log, "[%s] ", ts);
        vfprintf(g_log, fmt, ap);
        fputc('\n', g_log);
        fflush(g_log);
        va_end(ap);
    }
    va_start(ap, fmt);
    printf("[%s] ", ts);
    vprintf(fmt, ap);
    putchar('\n');
    fflush(stdout);
    va_end(ap);
    pthread_mutex_unlock(&g_log_mu);
}

/* ------------------------------------------------------------------ */
/* Low-level I/O helpers                                               */
/* ------------------------------------------------------------------ */
static int send_all(int fd, const void *buf, size_t n)
{
    const char *p = buf;
    while (n > 0) {
        ssize_t k = send(fd, p, n, MSG_NOSIGNAL);
        if (k < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += k;
        n -= (size_t)k;
    }
    return 0;
}

/* Every TCP response ends with " SID:<sid>\n". */
static int reply(session_t *s, const char *fmt, ...)
{
    char body[OUT_MAX + 128];
    char line[OUT_MAX + 192];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);
    int n = snprintf(line, sizeof line, "%s SID:%s\n", body, SID);
    if (n < 0) return -1;
    if ((size_t)n >= sizeof line) n = (int)sizeof line - 1;
    return send_all(s->fd, line, (size_t)n);
}

/*
 * Read one '\n'-terminated line. Handles partial lines and several lines
 * arriving in one recv() because leftover bytes stay in s->rbuf.
 * Returns length, -1 on disconnect/error, -2 if line too long.
 */
static int read_line(session_t *s, char *out, size_t max)
{
    size_t o = 0;
    for (;;) {
        while (s->rpos < s->rlen) {
            char c = s->rbuf[s->rpos++];
            if (c == '\n') {
                out[o] = '\0';
                if (o > 0 && out[o - 1] == '\r') out[--o] = '\0';
                return (int)o;
            }
            if (o + 1 >= max) return -2;
            out[o++] = c;
        }
        ssize_t k = recv(s->fd, s->rbuf, sizeof s->rbuf, 0);
        if (k < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (k == 0) return -1;
        s->rpos = 0;
        s->rlen = (size_t)k;
    }
}

/* Read up to n raw bytes; uses bytes already buffered by read_line first. */
static ssize_t read_some(session_t *s, void *buf, size_t n)
{
    if (s->rpos < s->rlen) {
        size_t avail = s->rlen - s->rpos;
        if (avail > n) avail = n;
        memcpy(buf, s->rbuf + s->rpos, avail);
        s->rpos += avail;
        return (ssize_t)avail;
    }
    for (;;) {
        ssize_t k = recv(s->fd, buf, n, 0);
        if (k < 0 && errno == EINTR) continue;
        return k;
    }
}

static int drain(session_t *s, unsigned long long n)
{
    char buf[8192];
    while (n > 0) {
        size_t want = n > sizeof buf ? sizeof buf : (size_t)n;
        ssize_t k = read_some(s, buf, want);
        if (k <= 0) return -1;
        n -= (unsigned long long)k;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* System information (real values from /proc, simulated as fallback)  */
/* ------------------------------------------------------------------ */
static void get_sysinfo(char *out, size_t n)
{
    double load = -1.0, up = -1.0;
    long total = 0, avail = -1;
    FILE *f;

    if ((f = fopen("/proc/loadavg", "r"))) {
        if (fscanf(f, "%lf", &load) != 1) load = -1.0;
        fclose(f);
    }
    if ((f = fopen("/proc/meminfo", "r"))) {
        char line[256];
        while (fgets(line, sizeof line, f)) {
            sscanf(line, "MemTotal: %ld", &total);
            sscanf(line, "MemAvailable: %ld", &avail);
        }
        fclose(f);
    }
    if ((f = fopen("/proc/uptime", "r"))) {
        if (fscanf(f, "%lf", &up) != 1) up = -1.0;
        fclose(f);
    }
    long mem_mb;
    if (avail >= 0 && total > 0) mem_mb = (total - avail) / 1024;
    else mem_mb = 512 + rand() % 256;           /* simulated */
    if (load < 0) load = 0.10 + (rand() % 50) / 100.0;   /* simulated */
    if (up < 0) up = (double)(time(NULL) % 100000);      /* simulated */

    snprintf(out, n, "SYSINFO %.2f %ld %ld", load, mem_mb, (long)up);
}

/* Process snapshot straight from /proc: "pid:name,pid:name,..." */
static void get_procs(char *out, size_t n)
{
    DIR *d = opendir("/proc");
    size_t used = 0;
    out[0] = '\0';
    if (!d) { snprintf(out, n, "unavailable"); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!isdigit((unsigned char)e->d_name[0])) continue;
        char path[300], name[64] = "?";
        snprintf(path, sizeof path, "/proc/%s/comm", e->d_name);
        FILE *f = fopen(path, "r");
        if (f) {
            if (fgets(name, sizeof name, f)) name[strcspn(name, "\n")] = '\0';
            fclose(f);
        }
        for (char *p = name; *p; p++)
            if (*p == ' ' || *p == ',') *p = '_';
        int w = snprintf(out + used, n - used, "%s%s:%s",
                         used ? "," : "", e->d_name, name);
        if (w < 0 || (size_t)w >= n - used) break;   /* buffer full */
        used += (size_t)w;
    }
    closedir(d);
    if (used == 0) snprintf(out, n, "none");
}

/* ------------------------------------------------------------------ */
/* EXEC: strict whitelist. The client text is never put in a command.  */
/* ------------------------------------------------------------------ */
static const struct { const char *name; const char *cmd; } WHITELIST[] = {
    { "DATE",     "date" },
    { "UPTIME",   "uptime -p" },
    { "DISKFREE", "df -h /" },
    { "HOSTNAME", "hostname" },
    { "WHOAMI",   "whoami" },
};

static const char *whitelist_lookup(const char *name)
{
    for (size_t i = 0; i < sizeof WHITELIST / sizeof WHITELIST[0]; i++)
        if (strcmp(name, WHITELIST[i].name) == 0) return WHITELIST[i].cmd;
    return NULL;
}

static void run_fixed(const char *cmd, char *out, size_t n)
{
    FILE *p = popen(cmd, "r");
    size_t used = 0;
    out[0] = '\0';
    if (!p) { snprintf(out, n, "exec_failed"); return; }
    int c;
    while ((c = fgetc(p)) != EOF && used + 1 < n)
        out[used++] = (c == '\n') ? '|' : (char)c;   /* keep one-line rule */
    out[used] = '\0';
    pclose(p);
    while (used > 0 && out[used - 1] == '|') out[--used] = '\0';
    if (used == 0) snprintf(out, n, "(no output)");
}

/* ------------------------------------------------------------------ */
/* UDP monitoring                                                      */
/* ------------------------------------------------------------------ */
static void *monitor_thread(void *arg)
{
    session_t *s = arg;
    int u = socket(AF_INET, SOCK_DGRAM, 0);
    if (u < 0) return NULL;
    struct sockaddr_in dst = s->peer;
    dst.sin_port = htons((uint16_t)s->mon_port);

    while (!atomic_load(&s->mon_stop)) {
        char info[128], msg[192];
        get_sysinfo(info, sizeof info);
        snprintf(msg, sizeof msg, "%s SID:%s", info, SID);
        sendto(u, msg, strlen(msg), 0, (struct sockaddr *)&dst, sizeof dst);
        for (int i = 0; i < MONITOR_INTERVAL_MS / 100 &&
                        !atomic_load(&s->mon_stop); i++)
            usleep(100000);
    }
    close(u);
    return NULL;
}

static void stop_monitor(session_t *s)
{
    if (s->mon_active) {
        atomic_store(&s->mon_stop, 1);
        pthread_join(s->mon_thread, NULL);
        s->mon_active = 0;
        log_msg("%s MONITOR stream stopped", s->peer_str);
    }
}

/* ------------------------------------------------------------------ */
/* File transfer                                                       */
/* ------------------------------------------------------------------ */
/* Only simple names: letters, digits, '.', '_', '-', no leading dot.   */
static int valid_filename(const char *f)
{
    size_t l = strlen(f);
    if (l == 0 || l > 100 || f[0] == '.') return 0;
    for (; *f; f++) {
        unsigned char c = (unsigned char)*f;
        if (!(isalnum(c) || c == '.' || c == '_' || c == '-')) return 0;
    }
    return 1;
}

static int do_put(session_t *s, const char *fname, const char *sizestr)
{
    if (!fname || !sizestr || sizestr[0] == '-' || sizestr[0] == '\0') {
        reply(s, "ERR 006 BAD_REQUEST");
        return 0;
    }
    char *end;
    errno = 0;
    unsigned long long sz = strtoull(sizestr, &end, 10);
    if (*end != '\0' || errno != 0) {
        reply(s, "ERR 006 BAD_REQUEST");
        return 0;
    }

    if (sz > MAX_FILE_SIZE) {
        log_msg("%s PUT %s rejected: %llu bytes exceeds limit",
                s->peer_str, fname, sz);
        if (sz > DRAIN_LIMIT) { reply(s, "ERR 004 FILE_TOO_LARGE"); return 1; }
        if (drain(s, sz) < 0) return 1;
        reply(s, "ERR 004 FILE_TOO_LARGE");
        return 0;
    }
    if (!valid_filename(fname)) {
        if (drain(s, sz) < 0) return 1;       /* keep framing intact */
        reply(s, "ERR 003 BAD_FILENAME");
        return 0;
    }

    char final_path[256], tmp_path[300];
    snprintf(final_path, sizeof final_path, "%s/%s", STORE_DIR, fname);
    snprintf(tmp_path, sizeof tmp_path, "%s/.%s.part.%lu",
             STORE_DIR, fname, (unsigned long)pthread_self());
    FILE *f = fopen(tmp_path, "wb");
    if (!f) {
        if (drain(s, sz) < 0) return 1;
        reply(s, "ERR 011 STORAGE_ERROR");
        return 0;
    }

    unsigned long long remaining = sz;
    char buf[8192];
    while (remaining > 0) {
        size_t want = remaining > sizeof buf ? sizeof buf : (size_t)remaining;
        ssize_t k = read_some(s, buf, want);
        if (k <= 0) {                         /* client vanished mid-upload */
            fclose(f);
            unlink(tmp_path);
            log_msg("%s PUT %s aborted (connection lost, %llu bytes missing)",
                    s->peer_str, fname, remaining);
            return 1;
        }
        fwrite(buf, 1, (size_t)k, f);
        remaining -= (unsigned long long)k;
    }
    fclose(f);
    if (rename(tmp_path, final_path) != 0) {
        unlink(tmp_path);
        reply(s, "ERR 011 STORAGE_ERROR");
        return 0;
    }
    log_msg("%s PUT %s %llu bytes stored at %s", s->peer_str, fname, sz,
            final_path);
    return reply(s, "OK FILE_RECEIVED %s", fname) < 0;
}

static int do_get(session_t *s, const char *fname)
{
    if (!fname || !valid_filename(fname)) {
        reply(s, "ERR 005 FILE_NOT_FOUND");
        return 0;
    }
    char path[256];
    snprintf(path, sizeof path, "%s/%s", STORE_DIR, fname);
    struct stat st;
    FILE *f = fopen(path, "rb");
    if (!f || fstat(fileno(f), &st) != 0 || !S_ISREG(st.st_mode)) {
        if (f) fclose(f);
        log_msg("%s GET %s: not found", s->peer_str, fname);
        reply(s, "ERR 005 FILE_NOT_FOUND");
        return 0;
    }
    unsigned long long size = (unsigned long long)st.st_size;
    if (reply(s, "OK FILE_SEND %s %llu", fname, size) < 0) {
        fclose(f);
        return 1;
    }
    char buf[8192];
    unsigned long long remaining = size;
    while (remaining > 0) {
        size_t want = remaining > sizeof buf ? sizeof buf : (size_t)remaining;
        size_t k = fread(buf, 1, want, f);
        if (k == 0 || send_all(s->fd, buf, k) < 0) {
            fclose(f);
            log_msg("%s GET %s aborted", s->peer_str, fname);
            return 1;                 /* promised bytes can't be honoured */
        }
        remaining -= k;
    }
    fclose(f);
    log_msg("%s GET %s %llu bytes sent", s->peer_str, fname, size);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Command dispatcher: returns 1 if the connection should close        */
/* ------------------------------------------------------------------ */
static int handle_line(session_t *s, char *line)
{
    char copy[LINE_MAX_LEN];
    strncpy(copy, line, sizeof copy - 1);
    copy[sizeof copy - 1] = '\0';
    char *sp = NULL;
    char *cmd = strtok_r(copy, " ", &sp);
    if (!cmd) return 0;
    char *a1 = strtok_r(NULL, " ", &sp);
    char *a2 = strtok_r(NULL, " ", &sp);

    /* Never log the token itself. */
    if (strcmp(cmd, "AUTH") == 0) log_msg("%s CMD AUTH <token>", s->peer_str);
    else log_msg("%s CMD %s", s->peer_str, line);

    if (strcmp(cmd, "AUTH") == 0) {
        if (a1 && strcmp(a1, AUTH_TOKEN) == 0) {
            s->authed = 1;
            log_msg("%s authentication OK", s->peer_str);
            return reply(s, "OK AUTHENTICATED") < 0;
        }
        log_msg("%s authentication FAILED", s->peer_str);
        return reply(s, "ERR 001 AUTH_FAILED") < 0;
    }

    if (!s->authed) {
        return reply(s, "ERR 008 NOT_AUTHENTICATED") < 0;
    }

    if (strcmp(cmd, "SYSINFO") == 0) {
        char info[128];
        get_sysinfo(info, sizeof info);
        return reply(s, "OK %s", info) < 0;
    }
    if (strcmp(cmd, "LISTPROC") == 0) {
        char procs[OUT_MAX];
        get_procs(procs, sizeof procs);
        return reply(s, "OK PROCS %s", procs) < 0;
    }
    if (strcmp(cmd, "EXEC") == 0) {
        const char *real = a1 ? whitelist_lookup(a1) : NULL;
        if (!real) {
            log_msg("%s EXEC '%s' rejected (not whitelisted)", s->peer_str,
                    a1 ? a1 : "");
            return reply(s, "ERR 002 COMMAND_NOT_ALLOWED") < 0;
        }
        char out[1024];
        run_fixed(real, out, sizeof out);
        return reply(s, "OK EXEC_RESULT %s", out) < 0;
    }
    if (strcmp(cmd, "PUT") == 0) return do_put(s, a1, a2);
    if (strcmp(cmd, "GET") == 0) return do_get(s, a1);

    if (strcmp(cmd, "MONITOR") == 0) {
        if (a1 && strcmp(a1, "START") == 0) {
            char *e;
            long port = a2 ? strtol(a2, &e, 10) : 0;
            if (!a2 || *e != '\0' || port < 1 || port > 65535)
                return reply(s, "ERR 006 BAD_REQUEST") < 0;
            if (s->mon_active)
                return reply(s, "ERR 007 MONITOR_ALREADY_RUNNING") < 0;
            s->mon_port = (int)port;
            atomic_store(&s->mon_stop, 0);
            if (pthread_create(&s->mon_thread, NULL, monitor_thread, s) != 0)
                return reply(s, "ERR 012 INTERNAL_ERROR") < 0;
            s->mon_active = 1;
            log_msg("%s MONITOR stream started -> udp port %ld (every %d ms)",
                    s->peer_str, port, MONITOR_INTERVAL_MS);
            return reply(s, "OK MONITOR_STARTED") < 0;
        }
        if (a1 && strcmp(a1, "STOP") == 0) {
            stop_monitor(s);
            return reply(s, "OK MONITOR_STOPPED") < 0;
        }
        return reply(s, "ERR 006 BAD_REQUEST") < 0;
    }
    if (strcmp(cmd, "QUIT") == 0) {
        stop_monitor(s);
        reply(s, "OK BYE");
        s->quit_clean = 1;
        return 1;
    }
    return reply(s, "ERR 009 UNKNOWN_COMMAND") < 0;
}

/* ------------------------------------------------------------------ */
/* One thread per client                                               */
/* ------------------------------------------------------------------ */
static void *client_thread(void *arg)
{
    session_t *s = arg;
    char line[LINE_MAX_LEN];
    log_msg("%s connected", s->peer_str);

    for (;;) {
        int n = read_line(s, line, sizeof line);
        if (n == -2) { reply(s, "ERR 010 LINE_TOO_LONG"); break; }
        if (n < 0) break;               /* EOF / reset: ungraceful disconnect */
        if (n == 0) continue;
        if (handle_line(s, line)) break;
    }
    stop_monitor(s);
    close(s->fd);
    log_msg("%s disconnected (%s)", s->peer_str,
            s->quit_clean ? "QUIT" : "connection lost / closed");
    free(s);
    return NULL;
}

static void on_signal(int sig) { (void)sig; g_running = 0; }

int main(void)
{
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;           /* no SA_RESTART: accept() wakes */
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    srand((unsigned)time(NULL));

    g_log = fopen(LOG_FILE, "a");
    if (!g_log) { perror("open log"); return 1; }
    mkdir(STORE_ROOT, 0755);
    mkdir(STORE_DIR, 0755);

    int ls = socket(AF_INET, SOCK_STREAM, 0);
    if (ls < 0) { perror("socket"); return 1; }
    int yes = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(LISTEN_PORT);
    if (bind(ls, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("bind"); return 1;
    }
    if (listen(ls, 16) < 0) { perror("listen"); return 1; }

    log_msg("RemoteOps Agent (%s) listening on TCP port %d, SID:%s, "
            "storage %s", REG_NO, LISTEN_PORT, SID, STORE_DIR);

    while (g_running) {
        struct sockaddr_in cli;
        socklen_t cl = sizeof cli;
        int fd = accept(ls, (struct sockaddr *)&cli, &cl);
        if (fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }
        session_t *s = calloc(1, sizeof *s);
        if (!s) { close(fd); continue; }
        s->fd = fd;
        s->peer = cli;
        snprintf(s->peer_str, sizeof s->peer_str, "%s:%d",
                 inet_ntoa(cli.sin_addr), ntohs(cli.sin_port));
        atomic_init(&s->mon_stop, 0);

        pthread_t th;
        pthread_attr_t at;
        pthread_attr_init(&at);
        pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&th, &at, client_thread, s) != 0) {
            log_msg("thread creation failed for %s", s->peer_str);
            close(fd);
            free(s);
        }
        pthread_attr_destroy(&at);
    }
    log_msg("Agent shutting down");
    close(ls);
    fclose(g_log);
    return 0;
}
