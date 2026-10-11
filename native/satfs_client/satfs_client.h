#ifndef SATFS_CLIENT_H
#define SATFS_CLIENT_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SATFS_CLIENT_VERSION_MAJOR 0
#define SATFS_CLIENT_VERSION_MINOR 1
#define SATFS_DEFAULT_UNIX_SOCKET "/tmp/satfs.sock"

#define SATFS_MAX_PATH 4096
#define SATFS_POOL_SIZE 16

/* File metadata */
typedef struct {
    int is_directory;
    uint64_t size;
    int64_t mtime_ms;
} satfs_stat_t;

/* Directory entry */
typedef struct {
    int is_directory;
    uint64_t size;
    int64_t mtime_ms;
    char name[256];
} satfs_entry_t;

typedef int (*satfs_entry_callback_t)(const satfs_entry_t *entry, void *user_data);

/* Opaque client instance */
typedef struct satfs_client satfs_client_t;

/**
 * Connect to a SAT-FS daemon via Unix Domain Socket.
 * If socket_path is NULL, SATFS_DEFAULT_UNIX_SOCKET is used.
 */
satfs_client_t *satfs_client_create_unix(const char *socket_path);

/**
 * Connect to a SAT-FS daemon via TCP.
 */
satfs_client_t *satfs_client_create_tcp(const char *host, int port);

/**
 * Authenticate with the SAT-FS daemon.
 */
int satfs_client_authenticate(satfs_client_t *client, const char *username, const char *password);

/**
 * Stat a file or directory.
 */
int satfs_client_stat(satfs_client_t *client, const char *path, satfs_stat_t *stat_out);

/**
 * List directory entries.
 */
int satfs_client_list(satfs_client_t *client, const char *path, satfs_entry_callback_t cb, void *user_data);

/**
 * Read a byte range directly into the destination buffer on-demand.
 * No whole-file hydration or temporary staging files required.
 */
int satfs_client_read_range(
    satfs_client_t *client,
    const char *path,
    uint64_t offset,
    size_t length,
    void *dest,
    size_t *out_read
);

/**
 * Stream a write chunk directly to the remote store.
 */
int satfs_client_write_chunk(
    satfs_client_t *client,
    const char *path,
    uint64_t offset,
    const void *data,
    size_t length
);

/**
 * Commit a completed file write.
 */
int satfs_client_commit_write(satfs_client_t *client, const char *path, uint64_t size);

/**
 * Abort an in-flight write stream.
 */
int satfs_client_abort_write(satfs_client_t *client, const char *path);

/**
 * Delete a file or directory.
 */
int satfs_client_delete(satfs_client_t *client, const char *path, int is_directory);

/**
 * Rename a path.
 */
int satfs_client_rename(satfs_client_t *client, const char *old_path, const char *new_path);

/**
 * Close and release client resources.
 */
void satfs_client_close(satfs_client_t *client);

#ifdef __cplusplus
}
#endif

#endif /* SATFS_CLIENT_H */
