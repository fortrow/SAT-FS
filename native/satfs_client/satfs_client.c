#define _GNU_SOURCE
#include "satfs_client.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

#define SATFS_BUF_SIZE 65536

typedef struct {
    int fd;
    int in_use;
    char read_buf[SATFS_BUF_SIZE];
    size_t buf_pos;
    size_t buf_len;
} satfs_conn_t;

struct satfs_client {
    int is_unix;
    char *target_path;
    char *target_host;
    int target_port;
    char *auth_username;
    char *auth_password;
    pthread_mutex_t pool_lock;
    satfs_conn_t conns[SATFS_POOL_SIZE];
};

static int write_exact(int fd, const void *buf, size_t count) {
    const char *p = (const char *)buf;
    while (count > 0) {
        ssize_t n = write(fd, p, count);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -errno;
        }
        if (n == 0) return -EPIPE;
        p += n;
        count -= (size_t)n;
    }
    return 0;
}

static int conn_fill_buffer(satfs_conn_t *conn) {
    conn->buf_pos = 0;
    ssize_t n;
    do {
        n = read(conn->fd, conn->read_buf, SATFS_BUF_SIZE);
    } while (n < 0 && errno == EINTR);

    if (n < 0) return -errno;
    if (n == 0) return -EPIPE;
    conn->buf_len = (size_t)n;
    return 0;
}

static int conn_read_line(satfs_conn_t *conn, char *out_buf, size_t max_len) {
    size_t written = 0;
    while (written + 1 < max_len) {
        if (conn->buf_pos >= conn->buf_len) {
            int ret = conn_fill_buffer(conn);
            if (ret != 0) return ret;
        }
        char ch = conn->read_buf[conn->buf_pos++];
        if (ch == '\n') {
            out_buf[written] = '\0';
            return 0;
        }
        out_buf[written++] = ch;
    }
    out_buf[written] = '\0';
    return -EOVERFLOW;
}

static int conn_read_exact(satfs_conn_t *conn, void *dest, size_t count) {
    char *d = (char *)dest;
    while (count > 0) {
        if (conn->buf_pos < conn->buf_len) {
            size_t avail = conn->buf_len - conn->buf_pos;
            size_t take = avail < count ? avail : count;
            memcpy(d, conn->read_buf + conn->buf_pos, take);
            conn->buf_pos += take;
            d += take;
            count -= take;
        } else {
            if (count >= SATFS_BUF_SIZE) {
                // Large read: bypass internal buffer directly into dest
                ssize_t n;
                do {
                    n = read(conn->fd, d, count);
                } while (n < 0 && errno == EINTR);
                if (n < 0) return -errno;
                if (n == 0) return -EPIPE;
                d += n;
                count -= (size_t)n;
            } else {
                int ret = conn_fill_buffer(conn);
                if (ret != 0) return ret;
            }
        }
    }
    return 0;
}

static int open_connection(struct satfs_client *client, satfs_conn_t *conn) {
    int sfd = -1;
    if (client->is_unix) {
        sfd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (sfd < 0) return -errno;

        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, client->target_path, sizeof(addr.sun_path) - 1);

        if (connect(sfd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            int err = errno;
            close(sfd);
            return -err;
        }
    } else {
        sfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sfd < 0) return -errno;

        int one = 1;
        setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t)client->target_port);
        if (inet_pton(AF_INET, client->target_host, &addr.sin_addr) <= 0) {
            close(sfd);
            return -EINVAL;
        }

        if (connect(sfd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            int err = errno;
            close(sfd);
            return -err;
        }
    }

    conn->fd = sfd;
    conn->buf_pos = 0;
    conn->buf_len = 0;

    // Authenticate if credentials are provided
    if (client->auth_username && client->auth_password) {
        char auth_line[1024];
        int len = snprintf(auth_line, sizeof(auth_line), "AUTH\t%s\t%s\n",
                           client->auth_username, client->auth_password);
        if (len < 0 || (size_t)len >= sizeof(auth_line)) {
            close(sfd);
            return -EINVAL;
        }
        int wr = write_exact(sfd, auth_line, (size_t)len);
        if (wr != 0) {
            close(sfd);
            return wr;
        }
        char line[512];
        int rd = conn_read_line(conn, line, sizeof(line));
        if (rd != 0 || strcmp(line, "READY") != 0) {
            close(sfd);
            return -EACCES;
        }
    }

    return 0;
}

static satfs_conn_t *acquire_conn(struct satfs_client *client) {
    pthread_mutex_lock(&client->pool_lock);
    for (int i = 0; i < SATFS_POOL_SIZE; i++) {
        if (!client->conns[i].in_use) {
            client->conns[i].in_use = 1;
            pthread_mutex_unlock(&client->pool_lock);

            if (client->conns[i].fd < 0) {
                int res = open_connection(client, &client->conns[i]);
                if (res != 0) {
                    pthread_mutex_lock(&client->pool_lock);
                    client->conns[i].in_use = 0;
                    pthread_mutex_unlock(&client->pool_lock);
                    return NULL;
                }
            }
            return &client->conns[i];
        }
    }
    pthread_mutex_unlock(&client->pool_lock);
    return NULL;
}

static void release_conn(struct satfs_client *client, satfs_conn_t *conn, int error) {
    if (conn == NULL) return;
    if (error) {
        if (conn->fd >= 0) {
            close(conn->fd);
            conn->fd = -1;
        }
        conn->buf_pos = 0;
        conn->buf_len = 0;
    }
    pthread_mutex_lock(&client->pool_lock);
    conn->in_use = 0;
    pthread_mutex_unlock(&client->pool_lock);
}

satfs_client_t *satfs_client_create_unix(const char *socket_path) {
    struct satfs_client *c = (struct satfs_client *)calloc(1, sizeof(*c));
    if (!c) return NULL;

    c->is_unix = 1;
    c->target_path = strdup(socket_path ? socket_path : SATFS_DEFAULT_UNIX_SOCKET);
    pthread_mutex_init(&c->pool_lock, NULL);
    for (int i = 0; i < SATFS_POOL_SIZE; i++) {
        c->conns[i].fd = -1;
    }
    return c;
}

satfs_client_t *satfs_client_create_tcp(const char *host, int port) {
    struct satfs_client *c = (struct satfs_client *)calloc(1, sizeof(*c));
    if (!c) return NULL;

    c->is_unix = 0;
    c->target_host = strdup(host ? host : "127.0.0.1");
    c->target_port = port;
    pthread_mutex_init(&c->pool_lock, NULL);
    for (int i = 0; i < SATFS_POOL_SIZE; i++) {
        c->conns[i].fd = -1;
    }
    return c;
}

int satfs_client_authenticate(satfs_client_t *client, const char *username, const char *password) {
    if (!client) return -EINVAL;
    pthread_mutex_lock(&client->pool_lock);
    free(client->auth_username);
    free(client->auth_password);
    client->auth_username = username ? strdup(username) : NULL;
    client->auth_password = password ? strdup(password) : NULL;

    // Reset pool connections to re-authenticate
    for (int i = 0; i < SATFS_POOL_SIZE; i++) {
        if (client->conns[i].fd >= 0 && !client->conns[i].in_use) {
            close(client->conns[i].fd);
            client->conns[i].fd = -1;
        }
    }
    pthread_mutex_unlock(&client->pool_lock);
    return 0;
}

int satfs_client_stat(satfs_client_t *client, const char *path, satfs_stat_t *stat_out) {
    if (!client || !path || !stat_out) return -EINVAL;
    satfs_conn_t *conn = acquire_conn(client);
    if (!conn) return -EIO;

    char req[SATFS_MAX_PATH + 16];
    int len = snprintf(req, sizeof(req), "STAT\t%s\n", path);
    if (len < 0 || (size_t)len >= sizeof(req)) {
        release_conn(client, conn, 0);
        return -ENAMETOOLONG;
    }

    int res = write_exact(conn->fd, req, (size_t)len);
    if (res != 0) {
        release_conn(client, conn, 1);
        return res;
    }

    char line[512];
    res = conn_read_line(conn, line, sizeof(line));
    if (res != 0) {
        release_conn(client, conn, 1);
        return res;
    }

    if (strcmp(line, "NOT_FOUND") == 0) {
        release_conn(client, conn, 0);
        return -ENOENT;
    }

    char *saveptr = NULL;
    char *token = strtok_r(line, "\t", &saveptr);
    if (!token || strcmp(token, "OK") != 0) {
        release_conn(client, conn, 0);
        return -EIO;
    }

    char *type = strtok_r(NULL, "\t", &saveptr);
    char *size_str = strtok_r(NULL, "\t", &saveptr);
    char *mtime_str = strtok_r(NULL, "\t", &saveptr);

    if (!type || !size_str || !mtime_str) {
        release_conn(client, conn, 0);
        return -EIO;
    }

    stat_out->is_directory = (strcmp(type, "DIRECTORY") == 0);
    stat_out->size = (uint64_t)strtoull(size_str, NULL, 10);
    stat_out->mtime_ms = (int64_t)strtoll(mtime_str, NULL, 10);

    release_conn(client, conn, 0);
    return 0;
}

int satfs_client_list(satfs_client_t *client, const char *path, satfs_entry_callback_t cb, void *user_data) {
    if (!client || !path || !cb) return -EINVAL;
    satfs_conn_t *conn = acquire_conn(client);
    if (!conn) return -EIO;

    char req[SATFS_MAX_PATH + 16];
    int len = snprintf(req, sizeof(req), "LIST\t%s\n", path);
    if (len < 0 || (size_t)len >= sizeof(req)) {
        release_conn(client, conn, 0);
        return -ENAMETOOLONG;
    }

    int res = write_exact(conn->fd, req, (size_t)len);
    if (res != 0) {
        release_conn(client, conn, 1);
        return res;
    }

    char line[1024];
    res = conn_read_line(conn, line, sizeof(line));
    if (res != 0) {
        release_conn(client, conn, 1);
        return res;
    }

    if (strncmp(line, "OK", 2) != 0) {
        release_conn(client, conn, 0);
        return -ENOENT;
    }

    while (1) {
        res = conn_read_line(conn, line, sizeof(line));
        if (res != 0) {
            release_conn(client, conn, 1);
            return res;
        }
        if (strcmp(line, "END") == 0) break;

        char *saveptr = NULL;
        char *token = strtok_r(line, "\t", &saveptr);
        if (!token || strcmp(token, "ENTRY") != 0) continue;

        char *type = strtok_r(NULL, "\t", &saveptr);
        char *size_str = strtok_r(NULL, "\t", &saveptr);
        char *mtime_str = strtok_r(NULL, "\t", &saveptr);
        char *name = strtok_r(NULL, "\t", &saveptr);

        if (type && size_str && mtime_str && name) {
            satfs_entry_t entry;
            entry.is_directory = (strcmp(type, "DIRECTORY") == 0);
            entry.size = (uint64_t)strtoull(size_str, NULL, 10);
            entry.mtime_ms = (int64_t)strtoll(mtime_str, NULL, 10);
            strncpy(entry.name, name, sizeof(entry.name) - 1);
            entry.name[sizeof(entry.name) - 1] = '\0';
            cb(&entry, user_data);
        }
    }

    release_conn(client, conn, 0);
    return 0;
}

int satfs_client_read_range(
    satfs_client_t *client,
    const char *path,
    uint64_t offset,
    size_t length,
    void *dest,
    size_t *out_read
) {
    if (!client || !path || !dest || !out_read) return -EINVAL;
    *out_read = 0;
    if (length == 0) return 0;

    satfs_conn_t *conn = acquire_conn(client);
    if (!conn) return -EIO;

    char req[SATFS_MAX_PATH + 64];
    int len = snprintf(req, sizeof(req), "READ\t%s\t%llu\t%zu\n",
                       path, (unsigned long long)offset, length);
    if (len < 0 || (size_t)len >= sizeof(req)) {
        release_conn(client, conn, 0);
        return -EINVAL;
    }

    int res = write_exact(conn->fd, req, (size_t)len);
    if (res != 0) {
        release_conn(client, conn, 1);
        return res;
    }

    char line[512];
    res = conn_read_line(conn, line, sizeof(line));
    if (res != 0) {
        release_conn(client, conn, 1);
        return res;
    }

    if (strncmp(line, "DATA\t", 5) != 0) {
        release_conn(client, conn, 0);
        return -EIO;
    }

    size_t data_len = (size_t)strtoull(line + 5, NULL, 10);
    if (data_len > length) {
        release_conn(client, conn, 1);
        return -EPROTO;
    }

    res = conn_read_exact(conn, dest, data_len);
    if (res != 0) {
        release_conn(client, conn, 1);
        return res;
    }

    char newline;
    res = conn_read_exact(conn, &newline, 1);
    if (res != 0 || newline != '\n') {
        release_conn(client, conn, 1);
        return -EPROTO;
    }

    res = conn_read_line(conn, line, sizeof(line));
    if (res != 0 || strcmp(line, "END") != 0) {
        release_conn(client, conn, 1);
        return -EPROTO;
    }

    *out_read = data_len;
    release_conn(client, conn, 0);
    return 0;
}

int satfs_client_write_chunk(
    satfs_client_t *client,
    const char *path,
    uint64_t offset,
    const void *data,
    size_t length
) {
    if (!client || !path || !data) return -EINVAL;
    satfs_conn_t *conn = acquire_conn(client);
    if (!conn) return -EIO;

    char req[SATFS_MAX_PATH + 64];
    int len = snprintf(req, sizeof(req), "CHUNK\t%s\t%llu\t%zu\n",
                       path, (unsigned long long)offset, length);
    if (len < 0 || (size_t)len >= sizeof(req)) {
        release_conn(client, conn, 0);
        return -EINVAL;
    }

    int res = write_exact(conn->fd, req, (size_t)len);
    if (res == 0) {
        res = write_exact(conn->fd, data, length);
    }
    if (res == 0) {
        res = write_exact(conn->fd, "\n", 1);
    }
    if (res != 0) {
        release_conn(client, conn, 1);
        return res;
    }

    char line[512];
    res = conn_read_line(conn, line, sizeof(line));
    if (res != 0 || strcmp(line, "OK") != 0) {
        release_conn(client, conn, 1);
        return -EIO;
    }

    release_conn(client, conn, 0);
    return 0;
}

int satfs_client_commit_write(satfs_client_t *client, const char *path, uint64_t size) {
    if (!client || !path) return -EINVAL;
    satfs_conn_t *conn = acquire_conn(client);
    if (!conn) return -EIO;

    char req[SATFS_MAX_PATH + 64];
    int len = snprintf(req, sizeof(req), "COMMIT\t%s\t%llu\n", path, (unsigned long long)size);
    if (len < 0 || (size_t)len >= sizeof(req)) {
        release_conn(client, conn, 0);
        return -EINVAL;
    }

    int res = write_exact(conn->fd, req, (size_t)len);
    if (res != 0) {
        release_conn(client, conn, 1);
        return res;
    }

    char line[512];
    res = conn_read_line(conn, line, sizeof(line));
    if (res != 0 || strcmp(line, "OK") != 0) {
        release_conn(client, conn, 1);
        return -EIO;
    }

    release_conn(client, conn, 0);
    return 0;
}

int satfs_client_abort_write(satfs_client_t *client, const char *path) {
    if (!client || !path) return -EINVAL;
    satfs_conn_t *conn = acquire_conn(client);
    if (!conn) return -EIO;

    char req[SATFS_MAX_PATH + 32];
    int len = snprintf(req, sizeof(req), "ABORT\t%s\n", path);
    if (len < 0 || (size_t)len >= sizeof(req)) {
        release_conn(client, conn, 0);
        return -EINVAL;
    }

    int res = write_exact(conn->fd, req, (size_t)len);
    if (res != 0) {
        release_conn(client, conn, 1);
        return res;
    }

    char line[512];
    conn_read_line(conn, line, sizeof(line));
    release_conn(client, conn, 0);
    return 0;
}

int satfs_client_delete(satfs_client_t *client, const char *path, int is_directory) {
    if (!client || !path) return -EINVAL;
    satfs_conn_t *conn = acquire_conn(client);
    if (!conn) return -EIO;

    char req[SATFS_MAX_PATH + 32];
    int len = snprintf(req, sizeof(req), "DELETE\t%s\t%d\n", path, is_directory ? 1 : 0);
    if (len < 0 || (size_t)len >= sizeof(req)) {
        release_conn(client, conn, 0);
        return -EINVAL;
    }

    int res = write_exact(conn->fd, req, (size_t)len);
    if (res != 0) {
        release_conn(client, conn, 1);
        return res;
    }

    char line[512];
    res = conn_read_line(conn, line, sizeof(line));
    if (res != 0 || strcmp(line, "OK") != 0) {
        release_conn(client, conn, 1);
        return -EIO;
    }

    release_conn(client, conn, 0);
    return 0;
}

int satfs_client_rename(satfs_client_t *client, const char *old_path, const char *new_path) {
    if (!client || !old_path || !new_path) return -EINVAL;
    satfs_conn_t *conn = acquire_conn(client);
    if (!conn) return -EIO;

    char req[SATFS_MAX_PATH * 2 + 32];
    int len = snprintf(req, sizeof(req), "RENAME\t%s\t%s\n", old_path, new_path);
    if (len < 0 || (size_t)len >= sizeof(req)) {
        release_conn(client, conn, 0);
        return -EINVAL;
    }

    int res = write_exact(conn->fd, req, (size_t)len);
    if (res != 0) {
        release_conn(client, conn, 1);
        return res;
    }

    char line[512];
    res = conn_read_line(conn, line, sizeof(line));
    if (res != 0 || strcmp(line, "OK") != 0) {
        release_conn(client, conn, 1);
        return -EIO;
    }

    release_conn(client, conn, 0);
    return 0;
}

void satfs_client_close(satfs_client_t *client) {
    if (!client) return;
    pthread_mutex_lock(&client->pool_lock);
    for (int i = 0; i < SATFS_POOL_SIZE; i++) {
        if (client->conns[i].fd >= 0) {
            close(client->conns[i].fd);
            client->conns[i].fd = -1;
        }
    }
    pthread_mutex_unlock(&client->pool_lock);
    pthread_mutex_destroy(&client->pool_lock);
    free(client->target_path);
    free(client->target_host);
    free(client->auth_username);
    free(client->auth_password);
    free(client);
}
