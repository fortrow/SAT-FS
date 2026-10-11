#define FUSE_USE_VERSION 29

#ifndef CXNFS_FUSE_PROGRAM_NAME
#define CXNFS_FUSE_PROGRAM_NAME "cxnfs-fuse-t"
#endif

#ifndef CXNFS_FUSE_LOG_PREFIX
#define CXNFS_FUSE_LOG_PREFIX CXNFS_FUSE_PROGRAM_NAME ": "
#endif

#ifndef CXNFS_FUSE_HEADER
#define CXNFS_FUSE_HEADER <fuse/fuse.h>
#endif

#ifndef CXNFS_FUSE_REQUIRE_ROOT
#define CXNFS_FUSE_REQUIRE_ROOT 1
#endif

#ifndef CXNFS_FUSE_REQUIRE_ROOT_MESSAGE
#define CXNFS_FUSE_REQUIRE_ROOT_MESSAGE "macOS FUSE-T mount requires superuser privileges; rerun with sudo"
#endif

#ifndef CXNFS_FUSE_TEMP_TEMPLATE
#define CXNFS_FUSE_TEMP_TEMPLATE "/tmp/cxnfs-fuse-t-write-XXXXXX"
#endif

#ifndef CXNFS_FUSE_STAGING_DIR_ENV
#define CXNFS_FUSE_STAGING_DIR_ENV "CXNFS_FUSE_STAGING_DIR"
#endif

#ifndef CXNFS_FUSE_FALLBACK_STAGING_DIR_ENV
#define CXNFS_FUSE_FALLBACK_STAGING_DIR_ENV "CXNFS_FUSE_FALLBACK_STAGING_DIR"
#endif

#ifndef CXNFS_FUSE_COMMIT_BUFFER_BYTES_ENV
#define CXNFS_FUSE_COMMIT_BUFFER_BYTES_ENV "CXNFS_FUSE_COMMIT_BUFFER_BYTES"
#endif

#ifndef CXNFS_FUSE_DEFAULT_COMMIT_BUFFER_BYTES
#define CXNFS_FUSE_DEFAULT_COMMIT_BUFFER_BYTES (16UL * 1024UL * 1024UL)
#endif

#ifndef CXNFS_FUSE_MIN_COMMIT_BUFFER_BYTES
#define CXNFS_FUSE_MIN_COMMIT_BUFFER_BYTES (1024UL * 1024UL)
#endif

#ifndef CXNFS_FUSE_MAX_COMMIT_BUFFER_BYTES
#define CXNFS_FUSE_MAX_COMMIT_BUFFER_BYTES (64UL * 1024UL * 1024UL)
#endif

#ifndef CXNFS_FUSE_BRIDGE_MAX_CHUNK_BYTES
#define CXNFS_FUSE_BRIDGE_MAX_CHUNK_BYTES (16UL * 1024UL * 1024UL)
#endif

#ifndef CXNFS_FUSE_MOUNT_OPTIONS
#define CXNFS_FUSE_MOUNT_OPTIONS "nolocalcaches,noattrcache,novncache,entry_timeout=0,attr_timeout=0,negative_timeout=0"
#endif

#ifndef CXNFS_FUSE_FALLBACK_MOUNT_OPTIONS
#define CXNFS_FUSE_FALLBACK_MOUNT_OPTIONS ((const char *) 0)
#endif

#include <errno.h>
#include <fcntl.h>
#include CXNFS_FUSE_HEADER
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/xattr.h>
#include <time.h>
#include <unistd.h>
#include "../satfs_client/satfs_client.c"

struct cxnfs_state {
    char *handoff_path;
    char *remote_root;
    char *mount_point;
    char *staging_root;
    char *fallback_staging_root;
    uid_t owner_uid;
    gid_t owner_gid;
    satfs_client_t *satfs_client;
    int bridge_in;
    int bridge_out;
    pid_t bridge_pid;
    pthread_mutex_t lock;
    char **local_dirs;
    size_t local_dir_count;
    size_t local_dir_capacity;
    struct local_file_entry *local_files;
    size_t local_file_count;
    size_t local_file_capacity;
};

struct cxnfs_stat {
    int directory;
    long long size;
    long long mtime_ms;
};

struct local_file_entry {
    char *path;
    long long size;
    long long mtime_ms;
};

struct cxnfs_file_handle {
    char *remote;
    char *local_path;
    int writable;
    int dirty;
    int hydrated;
    int append;
    int stream_enabled;
    int stream_started;
    int stream_disabled;
    int fd;
    char *temp_path;
    char *temp_dir;
    char *temp_cleanup_stop;
    int using_fallback_staging;
    long long remote_size;
    long long remote_mtime_ms;
    long long stream_next_offset;
};

static int write_all(int fd, const void *data, size_t size) {
    const char *cursor = (const char *) data;
    while (size > 0) {
        ssize_t written = write(fd, cursor, size);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -errno;
        }
        cursor += written;
        size -= (size_t) written;
    }
    return 0;
}

static char *read_line_fd(int fd) {
    size_t capacity = 256;
    size_t length = 0;
    char *line = malloc(capacity);
    if (line == NULL) {
        return NULL;
    }
    for (;;) {
        char ch;
        ssize_t read_count = read(fd, &ch, 1);
        if (read_count < 0) {
            if (errno == EINTR) {
                continue;
            }
            free(line);
            return NULL;
        }
        if (read_count == 0) {
            if (length == 0) {
                free(line);
                return NULL;
            }
            break;
        }
        if (ch == '\n') {
            break;
        }
        if (length + 1 >= capacity) {
            capacity *= 2;
            char *grown = realloc(line, capacity);
            if (grown == NULL) {
                free(line);
                return NULL;
            }
            line = grown;
        }
        line[length++] = ch;
    }
    line[length] = '\0';
    return line;
}

static int read_exact_fd(int fd, char *buffer, size_t size) {
    size_t offset = 0;
    while (offset < size) {
        ssize_t read_count = read(fd, buffer + offset, size - offset);
        if (read_count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -errno;
        }
        if (read_count == 0) {
            return -EIO;
        }
        offset += (size_t) read_count;
    }
    return 0;
}

static void strip_newline(char *line) {
    size_t length = strlen(line);
    while (length > 0 && (line[length - 1] == '\n' || line[length - 1] == '\r')) {
        line[--length] = '\0';
    }
}

static char *value_after(char *line, const char *prefix) {
    size_t prefix_length = strlen(prefix);
    if (strncmp(line, prefix, prefix_length) == 0) {
        return line + prefix_length;
    }
    return NULL;
}

static int read_handoff(struct cxnfs_state *state, const char *path) {
    FILE *file = fopen(path, "r");
    if (file == NULL) {
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "failed to read handoff %s: %s\n", path, strerror(errno));
        return -errno;
    }
    char *line = NULL;
    size_t capacity = 0;
    while (getline(&line, &capacity, file) >= 0) {
        strip_newline(line);
        char *remote_root = value_after(line, "remote_root=");
        char *mount_point = value_after(line, "mount_point=");
        if (remote_root != NULL) {
            free(state->remote_root);
            state->remote_root = strdup(remote_root);
        } else if (mount_point != NULL) {
            free(state->mount_point);
            state->mount_point = strdup(mount_point);
        }
    }
    free(line);
    fclose(file);
    if (state->remote_root == NULL || state->remote_root[0] == '\0') {
        state->remote_root = strdup("/");
    }
    if (state->mount_point == NULL || state->mount_point[0] == '\0') {
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "handoff is missing mount_point\n");
        return -EINVAL;
    }
    return 0;
}

static int contains_protocol_delimiter(const char *value) {
    return strchr(value, '\t') != NULL || strchr(value, '\n') != NULL || strchr(value, '\r') != NULL;
}

static int is_appledouble_name(const char *name) {
    return name != NULL && strncmp(name, "._", 2) == 0;
}

static int is_appledouble_path(const char *path) {
    const char *slash = strrchr(path, '/');
    const char *name = slash == NULL ? path : slash + 1;
    return is_appledouble_name(name);
}

static int ensure_mount_point(const char *mount_point) {
    struct stat stat_result;
    if (stat(mount_point, &stat_result) == 0) {
        if (!S_ISDIR(stat_result.st_mode)) {
            fprintf(stderr, "fuse: bad mount point `%s': Not a directory\n", mount_point);
            return -ENOTDIR;
        }
        return 0;
    }
    if (errno != ENOENT) {
        fprintf(stderr, "fuse: bad mount point `%s': %s\n", mount_point, strerror(errno));
        return -errno;
    }

    char *copy = strdup(mount_point);
    if (copy == NULL) {
        return -ENOMEM;
    }
    size_t length = strlen(copy);
    while (length > 1 && copy[length - 1] == '/') {
        copy[--length] = '\0';
    }

    const char *parent = ".";
    char *separator = strrchr(copy, '/');
    if (separator != NULL) {
        if (separator == copy) {
            separator[1] = '\0';
        } else {
            *separator = '\0';
        }
        parent = copy;
    }

    if (stat(parent, &stat_result) != 0) {
        fprintf(stderr, "fuse: bad mount point `%s': %s\n", mount_point, strerror(errno));
        free(copy);
        return -errno;
    }
    if (!S_ISDIR(stat_result.st_mode)) {
        fprintf(stderr, "fuse: bad mount point `%s': Not a directory\n", mount_point);
        free(copy);
        return -ENOTDIR;
    }
    free(copy);

    if (mkdir(mount_point, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "fuse: bad mount point `%s': %s\n", mount_point, strerror(errno));
        return -errno;
    }
    return 0;
}

static char *prompt_username(void) {
    FILE *tty = fopen("/dev/tty", "r+");
    if (tty == NULL) {
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "interactive terminal is required for CXNFS credentials\n");
        return NULL;
    }
    fprintf(tty, "CXNFS username: ");
    fflush(tty);
    char buffer[256];
    if (fgets(buffer, sizeof(buffer), tty) == NULL) {
        fclose(tty);
        return NULL;
    }
    fclose(tty);
    strip_newline(buffer);
    if (buffer[0] == '\0' || contains_protocol_delimiter(buffer)) {
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "invalid username\n");
        return NULL;
    }
    return strdup(buffer);
}

static char *prompt_password(void) {
    char *password = getpass("CXNFS password: ");
    if (password == NULL || password[0] == '\0' || contains_protocol_delimiter(password)) {
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "invalid password\n");
        return NULL;
    }
    return strdup(password);
}

static char *remote_path_for(struct cxnfs_state *state, const char *path) {
    if (strchr(path, '\t') != NULL) {
        return NULL;
    }
    if (strcmp(path, "/") == 0) {
        return strdup(state->remote_root);
    }
    if (strcmp(state->remote_root, "/") == 0) {
        return strdup(path);
    }
    size_t root_length = strlen(state->remote_root);
    size_t path_length = strlen(path);
    char *remote = malloc(root_length + path_length + 1);
    if (remote == NULL) {
        return NULL;
    }
    memcpy(remote, state->remote_root, root_length);
    memcpy(remote + root_length, path, path_length + 1);
    return remote;
}

static int launch_bridge(struct cxnfs_state *state, const char *username, const char *password) {
    const char *sock_env = getenv("SATFS_SOCKET");
    if (sock_env != NULL || access(SATFS_DEFAULT_UNIX_SOCKET, F_OK) == 0) {
        const char *socket_target = sock_env ? sock_env : SATFS_DEFAULT_UNIX_SOCKET;
        state->satfs_client = satfs_client_create_unix(socket_target);
        if (state->satfs_client != NULL) {
            int auth_res = satfs_client_authenticate(state->satfs_client, username, password);
            if (auth_res == 0) {
                return 0; // connected directly via native Unix domain socket client
            }
            satfs_client_close(state->satfs_client);
            state->satfs_client = NULL;
        }
    }

    int in_pipe[2];
    int out_pipe[2];
    if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) {
        perror(CXNFS_FUSE_LOG_PREFIX "pipe");
        return -errno;
    }
    pid_t pid = fork();
    if (pid < 0) {
        perror(CXNFS_FUSE_LOG_PREFIX "fork");
        return -errno;
    }
    if (pid == 0) {
        dup2(in_pipe[0], STDIN_FILENO);
        dup2(out_pipe[1], STDOUT_FILENO);
        close(in_pipe[0]);
        close(in_pipe[1]);
        close(out_pipe[0]);
        close(out_pipe[1]);
        execlp("sat-fs", "sat-fs", "fuse-bridge", state->handoff_path, "--credentials-stdin", (char *) NULL);
        execlp("cxnfs", "cxnfs", "fuse-bridge", state->handoff_path, "--credentials-stdin", (char *) NULL);
        perror(CXNFS_FUSE_LOG_PREFIX "exec sat-fs/cxnfs fuse-bridge");
        _exit(127);
    }
    close(in_pipe[0]);
    close(out_pipe[1]);
    state->bridge_in = in_pipe[1];
    state->bridge_out = out_pipe[0];
    state->bridge_pid = pid;

    size_t auth_length = strlen("AUTH\t") + strlen(username) + 1 + strlen(password) + 1;
    char *auth = malloc(auth_length + 1);
    if (auth == NULL) {
        return -ENOMEM;
    }
    snprintf(auth, auth_length + 1, "AUTH\t%s\t%s\n", username, password);
    int auth_result = write_all(state->bridge_in, auth, strlen(auth));
    memset(auth, 0, auth_length + 1);
    free(auth);
    if (auth_result != 0) {
        return auth_result;
    }

    for (;;) {
        char *ready = read_line_fd(state->bridge_out);
        if (ready == NULL) {
            fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "bridge did not become ready\n");
            return -EIO;
        }
        if (strcmp(ready, "READY") == 0) {
            free(ready);
            return 0;
        }
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "ignoring bridge startup output: %s\n", ready);
        free(ready);
    }
}

static int send_command(struct cxnfs_state *state, const char *command, const char *path) {
    int result = write_all(state->bridge_in, command, strlen(command));
    if (result != 0) {
        return result;
    }
    result = write_all(state->bridge_in, "\t", 1);
    if (result != 0) {
        return result;
    }
    result = write_all(state->bridge_in, path, strlen(path));
    if (result != 0) {
        return result;
    }
    return write_all(state->bridge_in, "\n", 1);
}

static int bridge_error_errno(const char *message) {
    if (message == NULL) {
        return -EIO;
    }
    if (strstr(message, "unknown") != NULL || strstr(message, "missing") != NULL
            || strstr(message, "not found") != NULL) {
        return -ENOENT;
    }
    if (strstr(message, "not authorized") != NULL || strstr(message, "permission") != NULL) {
        return -EACCES;
    }
    if (strstr(message, "already exists") != NULL) {
        return -EEXIST;
    }
    if (strstr(message, "not empty") != NULL) {
        return -ENOTEMPTY;
    }
    return -EIO;
}

static int read_bridge_ok_locked(struct cxnfs_state *state, const char *operation) {
    char *line = read_line_fd(state->bridge_out);
    if (line == NULL) {
        return -EIO;
    }
    if (strncmp(line, "OK", 2) == 0 && (line[2] == '\0' || line[2] == '\t')) {
        free(line);
        return 0;
    }
    if (strncmp(line, "ERR\t", 4) == 0) {
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "bridge %s failed: %s\n", operation, line + 4);
        int result = bridge_error_errno(line + 4);
        free(line);
        return result;
    }
    free(line);
    return -EIO;
}

static int local_dir_index_locked(struct cxnfs_state *state, const char *path) {
    for (size_t index = 0; index < state->local_dir_count; index++) {
        if (strcmp(state->local_dirs[index], path) == 0) {
            return (int) index;
        }
    }
    return -1;
}

static int local_dir_add_locked(struct cxnfs_state *state, const char *path) {
    if (strcmp(path, "/") == 0 || local_dir_index_locked(state, path) >= 0) {
        return 0;
    }
    if (state->local_dir_count == state->local_dir_capacity) {
        size_t capacity = state->local_dir_capacity == 0 ? 8 : state->local_dir_capacity * 2;
        char **dirs = realloc(state->local_dirs, capacity * sizeof(*dirs));
        if (dirs == NULL) {
            return -ENOMEM;
        }
        state->local_dirs = dirs;
        state->local_dir_capacity = capacity;
    }
    state->local_dirs[state->local_dir_count] = strdup(path);
    if (state->local_dirs[state->local_dir_count] == NULL) {
        return -ENOMEM;
    }
    state->local_dir_count++;
    return 0;
}

static void local_dir_remove_index_locked(struct cxnfs_state *state, size_t index) {
    free(state->local_dirs[index]);
    for (size_t cursor = index + 1; cursor < state->local_dir_count; cursor++) {
        state->local_dirs[cursor - 1] = state->local_dirs[cursor];
    }
    state->local_dir_count--;
}

static int local_dir_remove_locked(struct cxnfs_state *state, const char *path) {
    int index = local_dir_index_locked(state, path);
    if (index < 0) {
        return -ENOENT;
    }
    local_dir_remove_index_locked(state, (size_t) index);
    return 0;
}

static int local_dir_exists_locked(struct cxnfs_state *state, const char *path) {
    return local_dir_index_locked(state, path) >= 0;
}

static int local_dir_parent_name(const char *path, const char *parent, const char **name) {
    if (strcmp(path, "/") == 0) {
        return 0;
    }
    const char *slash = strrchr(path, '/');
    if (slash == NULL) {
        return 0;
    }
    const char *entry_name = slash + 1;
    if (entry_name[0] == '\0') {
        return 0;
    }
    if (slash == path) {
        if (strcmp(parent, "/") != 0) {
            return 0;
        }
        *name = entry_name;
        return 1;
    }
    size_t parent_length = (size_t) (slash - path);
    if (strlen(parent) != parent_length || strncmp(path, parent, parent_length) != 0) {
        return 0;
    }
    *name = entry_name;
    return 1;
}

static int local_dir_has_child_locked(struct cxnfs_state *state, const char *path) {
    size_t path_length = strlen(path);
    for (size_t index = 0; index < state->local_dir_count; index++) {
        const char *candidate = state->local_dirs[index];
        if (strcmp(path, "/") == 0) {
            if (candidate[0] == '/' && strchr(candidate + 1, '/') == NULL) {
                return 1;
            }
        } else if (strncmp(candidate, path, path_length) == 0 && candidate[path_length] == '/') {
            return 1;
        }
    }
    return 0;
}

static void local_dir_remove_ancestors_locked(struct cxnfs_state *state, const char *path) {
    char *copy = strdup(path);
    if (copy == NULL) {
        return;
    }
    char *slash = strrchr(copy, '/');
    while (slash != NULL && slash != copy) {
        *slash = '\0';
        int index = local_dir_index_locked(state, copy);
        if (index >= 0) {
            local_dir_remove_index_locked(state, (size_t) index);
        }
        slash = strrchr(copy, '/');
    }
    if (slash == copy && copy[1] != '\0') {
        copy[1] = '\0';
    }
    free(copy);
}

static int local_file_index_locked(struct cxnfs_state *state, const char *path) {
    for (size_t index = 0; index < state->local_file_count; index++) {
        if (strcmp(state->local_files[index].path, path) == 0) {
            return (int) index;
        }
    }
    return -1;
}

static int local_file_put_locked(struct cxnfs_state *state, const char *path, long long size) {
    if (strcmp(path, "/") == 0) {
        return 0;
    }
    int existing = local_file_index_locked(state, path);
    long long now_ms = (long long) time(NULL) * 1000;
    if (existing >= 0) {
        state->local_files[existing].size = size;
        state->local_files[existing].mtime_ms = now_ms;
        return 0;
    }
    if (state->local_file_count == state->local_file_capacity) {
        size_t capacity = state->local_file_capacity == 0 ? 8 : state->local_file_capacity * 2;
        struct local_file_entry *files = realloc(state->local_files, capacity * sizeof(*files));
        if (files == NULL) {
            return -ENOMEM;
        }
        state->local_files = files;
        state->local_file_capacity = capacity;
    }
    state->local_files[state->local_file_count].path = strdup(path);
    if (state->local_files[state->local_file_count].path == NULL) {
        return -ENOMEM;
    }
    state->local_files[state->local_file_count].size = size;
    state->local_files[state->local_file_count].mtime_ms = now_ms;
    state->local_file_count++;
    return 0;
}

static void local_file_remove_index_locked(struct cxnfs_state *state, size_t index) {
    free(state->local_files[index].path);
    for (size_t cursor = index + 1; cursor < state->local_file_count; cursor++) {
        state->local_files[cursor - 1] = state->local_files[cursor];
    }
    state->local_file_count--;
}

static int local_file_remove_locked(struct cxnfs_state *state, const char *path) {
    int index = local_file_index_locked(state, path);
    if (index < 0) {
        return -ENOENT;
    }
    local_file_remove_index_locked(state, (size_t) index);
    return 0;
}

static int local_file_stat_locked(struct cxnfs_state *state, const char *path, struct cxnfs_stat *metadata) {
    int index = local_file_index_locked(state, path);
    if (index < 0) {
        return -ENOENT;
    }
    metadata->directory = 0;
    metadata->size = state->local_files[index].size;
    metadata->mtime_ms = state->local_files[index].mtime_ms;
    return 0;
}

static int local_file_rename_locked(struct cxnfs_state *state, const char *from, const char *to) {
    int index = local_file_index_locked(state, from);
    if (index < 0) {
        return -ENOENT;
    }
    char *copy = strdup(to);
    if (copy == NULL) {
        return -ENOMEM;
    }
    free(state->local_files[index].path);
    state->local_files[index].path = copy;
    state->local_files[index].mtime_ms = (long long) time(NULL) * 1000;
    return 0;
}

static int parse_stat_line(char *line, struct cxnfs_stat *stat_result) {
    char *save = NULL;
    char *token = strtok_r(line, "\t", &save);
    if (token == NULL || strcmp(token, "OK") != 0) {
        return -EIO;
    }
    char *type = strtok_r(NULL, "\t", &save);
    char *size = strtok_r(NULL, "\t", &save);
    char *mtime = strtok_r(NULL, "\t", &save);
    if (type == NULL || size == NULL || mtime == NULL) {
        return -EIO;
    }
    stat_result->directory = strcmp(type, "DIRECTORY") == 0;
    stat_result->size = atoll(size);
    stat_result->mtime_ms = atoll(mtime);
    return 0;
}

static unsigned long parse_unsigned_env(const char *name, unsigned long fallback) {
    const char *value = getenv(name);
    if (value == NULL || value[0] == '\0') {
        return fallback;
    }
    char *end = NULL;
    unsigned long parsed = strtoul(value, &end, 10);
    return end != value && *end == '\0' ? parsed : fallback;
}

static size_t commit_buffer_bytes(void) {
    unsigned long value = parse_unsigned_env(
            CXNFS_FUSE_COMMIT_BUFFER_BYTES_ENV,
            CXNFS_FUSE_DEFAULT_COMMIT_BUFFER_BYTES
    );
    if (value < CXNFS_FUSE_MIN_COMMIT_BUFFER_BYTES) {
        value = CXNFS_FUSE_MIN_COMMIT_BUFFER_BYTES;
    }
    if (value > CXNFS_FUSE_MAX_COMMIT_BUFFER_BYTES) {
        value = CXNFS_FUSE_MAX_COMMIT_BUFFER_BYTES;
    }
    return (size_t) value;
}

static uid_t mount_owner_uid(void) {
#ifdef CXNFS_FUSE_LINUX
    if (geteuid() == 0) {
        return (uid_t) parse_unsigned_env("SUDO_UID", (unsigned long) getuid());
    }
#endif
    return getuid();
}

static gid_t mount_owner_gid(void) {
#ifdef CXNFS_FUSE_LINUX
    if (geteuid() == 0) {
        return (gid_t) parse_unsigned_env("SUDO_GID", (unsigned long) getgid());
    }
#endif
    return getgid();
}

static char *strdup_nonempty_env(const char *name) {
    const char *value = getenv(name);
    if (value == NULL || value[0] == '\0') {
        return NULL;
    }
    return strdup(value);
}

static int ensure_directory_exists(const char *path) {
    struct stat existing;
    if (mkdir(path, 0700) == 0) {
        return 0;
    }
    if (errno != EEXIST) {
        return -errno;
    }
    if (stat(path, &existing) != 0) {
        return -errno;
    }
    return S_ISDIR(existing.st_mode) ? 0 : -ENOTDIR;
}

static int ensure_directory_recursive(const char *path) {
    if (path == NULL || path[0] == '\0') {
        return -EINVAL;
    }
    char *copy = strdup(path);
    if (copy == NULL) {
        return -ENOMEM;
    }
    size_t length = strlen(copy);
    while (length > 1 && copy[length - 1] == '/') {
        copy[--length] = '\0';
    }
    for (char *cursor = copy + 1; *cursor != '\0'; cursor++) {
        if (*cursor != '/') {
            continue;
        }
        *cursor = '\0';
        int result = ensure_directory_exists(copy);
        *cursor = '/';
        if (result != 0) {
            free(copy);
            return result;
        }
    }
    int result = ensure_directory_exists(copy);
    free(copy);
    return result;
}

static char *path_join(const char *left, const char *right) {
    if (right == NULL) {
        return NULL;
    }
    if (left == NULL || left[0] == '\0') {
        return strdup(right);
    }
    while (*right == '/') {
        right++;
    }
    size_t left_length = strlen(left);
    while (left_length > 1 && left[left_length - 1] == '/') {
        left_length--;
    }
    size_t right_length = strlen(right);
    int add_separator = left_length > 0 && !(left_length == 1 && left[0] == '/') && right_length > 0;
    char *joined = malloc(left_length + (add_separator ? 1U : 0U) + right_length + 1U);
    if (joined == NULL) {
        return NULL;
    }
    size_t offset = 0;
    memcpy(joined + offset, left, left_length);
    offset += left_length;
    if (add_separator) {
        joined[offset++] = '/';
    }
    memcpy(joined + offset, right, right_length);
    offset += right_length;
    joined[offset] = '\0';
    return joined;
}

static char *parent_path_copy(const char *path) {
    if (path == NULL || path[0] == '\0') {
        return strdup(".");
    }
    char *copy = strdup(path);
    if (copy == NULL) {
        return NULL;
    }
    size_t length = strlen(copy);
    while (length > 1 && copy[length - 1] == '/') {
        copy[--length] = '\0';
    }
    char *separator = strrchr(copy, '/');
    if (separator == NULL) {
        free(copy);
        return strdup(".");
    }
    if (separator == copy) {
        separator[1] = '\0';
        return copy;
    }
    *separator = '\0';
    return copy;
}

static char *last_path_component_copy(const char *path, const char *fallback) {
    if (path == NULL || path[0] == '\0') {
        return strdup(fallback);
    }
    const char *end = path + strlen(path);
    while (end > path + 1 && end[-1] == '/') {
        end--;
    }
    const char *start = end;
    while (start > path && start[-1] != '/') {
        start--;
    }
    size_t length = (size_t) (end - start);
    if (length == 0 || (length == 1 && start[0] == '.') || (length == 2 && start[0] == '.' && start[1] == '.')) {
        return strdup(fallback);
    }
    char *component = malloc(length + 1U);
    if (component == NULL) {
        return NULL;
    }
    memcpy(component, start, length);
    component[length] = '\0';
    return component;
}

static int safe_relative_path(const char *path, char **out) {
    const char *relative = path;
    while (relative != NULL && *relative == '/') {
        relative++;
    }
    if (relative == NULL || *relative == '\0') {
        *out = strdup("__root__");
        return *out == NULL ? -ENOMEM : 0;
    }
    char *copy = strdup(relative);
    if (copy == NULL) {
        return -ENOMEM;
    }
    char *cursor = copy;
    while (*cursor != '\0') {
        char *next = strchr(cursor, '/');
        if (next != NULL) {
            *next = '\0';
        }
        if (cursor[0] == '\0'
                || strcmp(cursor, ".") == 0
                || strcmp(cursor, "..") == 0) {
            free(copy);
            return -EINVAL;
        }
        if (next == NULL) {
            break;
        }
        *next = '/';
        cursor = next + 1;
    }
    *out = copy;
    return 0;
}

static char *staging_base_dir(const char *root) {
    return path_join(root, ".tmp");
}

static char *default_staging_root_for_mount(const char *mount_point) {
    return parent_path_copy(mount_point);
}

static char *default_fallback_staging_root(void) {
    return strdup("/tmp");
}

static int initialize_staging_roots(struct cxnfs_state *state) {
    state->staging_root = strdup_nonempty_env(CXNFS_FUSE_STAGING_DIR_ENV);
    if (state->staging_root == NULL) {
        state->staging_root = default_staging_root_for_mount(state->mount_point);
    }
    state->fallback_staging_root = strdup_nonempty_env(CXNFS_FUSE_FALLBACK_STAGING_DIR_ENV);
    if (state->fallback_staging_root == NULL) {
        state->fallback_staging_root = default_fallback_staging_root();
    }
    return state->staging_root == NULL || state->fallback_staging_root == NULL ? -ENOMEM : 0;
}

static int staging_roots_match(struct cxnfs_state *state) {
    return state->staging_root != NULL
            && state->fallback_staging_root != NULL
            && strcmp(state->staging_root, state->fallback_staging_root) == 0;
}

static void prune_empty_directories_until(const char *start, const char *stop) {
    if (start == NULL || stop == NULL || strcmp(start, stop) == 0) {
        return;
    }
    char *current = strdup(start);
    if (current == NULL) {
        return;
    }
    while (strcmp(current, stop) != 0 && strcmp(current, "/") != 0 && strcmp(current, ".") != 0) {
        if (rmdir(current) != 0) {
            break;
        }
        char *parent = parent_path_copy(current);
        if (parent == NULL || strcmp(parent, current) == 0) {
            free(parent);
            break;
        }
        free(current);
        current = parent;
    }
    free(current);
}

static void cleanup_temp_artifact(const char *temp_path, const char *temp_dir, const char *cleanup_stop) {
    if (temp_path != NULL) {
        unlink(temp_path);
    }
    prune_empty_directories_until(temp_dir, cleanup_stop);
}

static int create_temp_file_for_path(
        struct cxnfs_state *state,
        const char *local_path,
        int use_fallback,
        int *fd_out,
        char **temp_path_out,
        char **temp_dir_out,
        char **cleanup_stop_out
) {
    const char *root = use_fallback ? state->fallback_staging_root : state->staging_root;
    if (root == NULL || root[0] == '\0') {
        return -EINVAL;
    }
    char *base = staging_base_dir(root);
    char *mount_name = last_path_component_copy(state->mount_point, "mount");
    char *mount_dir = NULL;
    char *relative = NULL;
    char *temp_dir = NULL;
    char *template = NULL;
    if (base == NULL || mount_name == NULL) {
        free(base);
        free(mount_name);
        return -ENOMEM;
    }
    int result = safe_relative_path(local_path, &relative);
    if (result != 0) {
        free(base);
        free(mount_name);
        return result;
    }
    mount_dir = path_join(base, mount_name);
    if (mount_dir != NULL) {
        temp_dir = path_join(mount_dir, relative);
    }
    if (temp_dir != NULL) {
        template = path_join(temp_dir, "cxnfs-write-XXXXXX");
    }
    free(mount_name);
    free(mount_dir);
    free(relative);
    if (temp_dir == NULL || template == NULL) {
        free(base);
        free(temp_dir);
        free(template);
        return -ENOMEM;
    }
    result = ensure_directory_recursive(temp_dir);
    if (result != 0) {
        free(base);
        free(temp_dir);
        free(template);
        return result;
    }
    int fd = mkstemp(template);
    if (fd < 0) {
        result = -errno;
        free(base);
        free(temp_dir);
        free(template);
        return result;
    }
    *fd_out = fd;
    *temp_path_out = template;
    *temp_dir_out = temp_dir;
    *cleanup_stop_out = base;
    return 0;
}

static int create_temp_file_for_handle(
        struct cxnfs_state *state,
        struct cxnfs_file_handle *handle,
        int use_fallback
) {
    return create_temp_file_for_path(
            state,
            handle->local_path,
            use_fallback,
            &handle->fd,
            &handle->temp_path,
            &handle->temp_dir,
            &handle->temp_cleanup_stop
    );
}

static int create_initial_temp_file(struct cxnfs_state *state, struct cxnfs_file_handle *handle) {
    int result = create_temp_file_for_handle(state, handle, 0);
    if (result == 0) {
        handle->using_fallback_staging = 0;
        return 0;
    }
    if (result != -ENOSPC || staging_roots_match(state)) {
        return result;
    }
    int fallback_result = create_temp_file_for_handle(state, handle, 1);
    if (fallback_result == 0) {
        handle->using_fallback_staging = 1;
        return 0;
    }
    return fallback_result;
}

static int copy_fd_contents(int source_fd, int target_fd) {
    off_t source_offset = lseek(source_fd, 0, SEEK_CUR);
    if (source_offset < 0) {
        return -errno;
    }
    if (lseek(source_fd, 0, SEEK_SET) < 0) {
        return -errno;
    }
    if (ftruncate(target_fd, 0) != 0) {
        return -errno;
    }
    if (lseek(target_fd, 0, SEEK_SET) < 0) {
        return -errno;
    }
    char *buffer = malloc(1024 * 1024);
    if (buffer == NULL) {
        return -ENOMEM;
    }
    int result = 0;
    for (;;) {
        ssize_t read_count = read(source_fd, buffer, 1024 * 1024);
        if (read_count < 0) {
            if (errno == EINTR) {
                continue;
            }
            result = -errno;
            break;
        }
        if (read_count == 0) {
            break;
        }
        result = write_all(target_fd, buffer, (size_t) read_count);
        if (result != 0) {
            break;
        }
    }
    free(buffer);
    if (result == 0 && lseek(target_fd, source_offset, SEEK_SET) < 0) {
        result = -errno;
    }
    if (lseek(source_fd, source_offset, SEEK_SET) < 0 && result == 0) {
        result = -errno;
    }
    return result;
}

static int migrate_handle_to_fallback(struct cxnfs_state *state, struct cxnfs_file_handle *handle) {
    if (handle == NULL || handle->using_fallback_staging || staging_roots_match(state)) {
        return -ENOSPC;
    }
    int new_fd = -1;
    char *new_path = NULL;
    char *new_dir = NULL;
    char *new_cleanup_stop = NULL;
    int result = create_temp_file_for_path(
            state,
            handle->local_path,
            1,
            &new_fd,
            &new_path,
            &new_dir,
            &new_cleanup_stop
    );
    if (result != 0) {
        return result;
    }
    result = copy_fd_contents(handle->fd, new_fd);
    if (result != 0) {
        close(new_fd);
        cleanup_temp_artifact(new_path, new_dir, new_cleanup_stop);
        free(new_path);
        free(new_dir);
        free(new_cleanup_stop);
        return result;
    }
    close(handle->fd);
    cleanup_temp_artifact(handle->temp_path, handle->temp_dir, handle->temp_cleanup_stop);
    free(handle->temp_path);
    free(handle->temp_dir);
    free(handle->temp_cleanup_stop);
    handle->fd = new_fd;
    handle->temp_path = new_path;
    handle->temp_dir = new_dir;
    handle->temp_cleanup_stop = new_cleanup_stop;
    handle->using_fallback_staging = 1;
    return 0;
}

static int staging_available_bytes(const char *root, unsigned long long *available) {
    struct statvfs stats;
    if (root == NULL || statvfs(root, &stats) != 0) {
        return -errno;
    }
    *available = (unsigned long long) stats.f_bavail * (unsigned long long) stats.f_frsize;
    return 0;
}

static int ensure_handle_capacity(
        struct cxnfs_state *state,
        struct cxnfs_file_handle *handle,
        unsigned long long target_size
) {
    if (handle == NULL || handle->using_fallback_staging || staging_roots_match(state)) {
        return 0;
    }
    struct stat temp_stat;
    if (fstat(handle->fd, &temp_stat) != 0) {
        return -errno;
    }
    if (target_size <= (unsigned long long) temp_stat.st_size) {
        return 0;
    }
    unsigned long long available = 0;
    int result = staging_available_bytes(state->staging_root, &available);
    if (result != 0) {
        return 0;
    }
    unsigned long long additional = target_size - (unsigned long long) temp_stat.st_size;
    if (available >= additional) {
        return 0;
    }
    return migrate_handle_to_fallback(state, handle);
}

static void fill_stat(struct cxnfs_state *state, struct stat *stbuf, const struct cxnfs_stat *metadata) {
    memset(stbuf, 0, sizeof(*stbuf));
    stbuf->st_uid = state->owner_uid;
    stbuf->st_gid = state->owner_gid;
    stbuf->st_size = metadata->directory ? 0 : metadata->size;
    stbuf->st_nlink = metadata->directory ? 2 : 1;
    stbuf->st_mode = metadata->directory ? (S_IFDIR | 0755) : (S_IFREG | 0644);
    stbuf->st_mtime = (time_t) (metadata->mtime_ms / 1000);
}

static int bridge_stat(struct cxnfs_state *state, const char *remote, struct cxnfs_stat *metadata) {
    if (state != NULL && state->satfs_client != NULL) {
        satfs_stat_t s;
        int ret = satfs_client_stat(state->satfs_client, remote, &s);
        if (ret == 0) {
            metadata->directory = s.is_directory;
            metadata->size = (long long) s.size;
            metadata->mtime_ms = s.mtime_ms;
            return 0;
        }
        return ret;
    }
    pthread_mutex_lock(&state->lock);
    int result = send_command(state, "STAT", remote);
    if (result != 0) {
        pthread_mutex_unlock(&state->lock);
        return result;
    }
    char *line = read_line_fd(state->bridge_out);
    if (line == NULL) {
        pthread_mutex_unlock(&state->lock);
        return -EIO;
    }
    if (strcmp(line, "NOT_FOUND") == 0) {
        free(line);
        pthread_mutex_unlock(&state->lock);
        return -ENOENT;
    }
    if (strncmp(line, "ERR\t", 4) == 0) {
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "bridge stat failed: %s\n", line + 4);
        free(line);
        pthread_mutex_unlock(&state->lock);
        return -EIO;
    }
    result = parse_stat_line(line, metadata);
    free(line);
    pthread_mutex_unlock(&state->lock);
    return result;
}

static int bridge_read_range(
        struct cxnfs_state *state,
        const char *remote,
        long long offset,
        size_t size,
        char *buf,
        size_t *bytes_read
) {
    if (state != NULL && state->satfs_client != NULL) {
        return satfs_client_read_range(state->satfs_client, remote, (uint64_t) offset, size, buf, bytes_read);
    }
    *bytes_read = 0;
    pthread_mutex_lock(&state->lock);
    char header[1024];
    int header_length = snprintf(header, sizeof(header), "READ\t%s\t%lld\t%zu\n", remote, offset, size);
    if (header_length < 0 || (size_t) header_length >= sizeof(header)) {
        pthread_mutex_unlock(&state->lock);
        return -EINVAL;
    }
    int result = write_all(state->bridge_in, header, (size_t) header_length);
    if (result != 0) {
        pthread_mutex_unlock(&state->lock);
        return result;
    }
    char *line = read_line_fd(state->bridge_out);
    if (line == NULL) {
        pthread_mutex_unlock(&state->lock);
        return -EIO;
    }
    if (strncmp(line, "ERR\t", 4) == 0) {
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "bridge read failed: %s\n", line + 4);
        result = bridge_error_errno(line + 4);
        free(line);
        pthread_mutex_unlock(&state->lock);
        return result;
    }
    if (strncmp(line, "DATA\t", 5) != 0) {
        free(line);
        pthread_mutex_unlock(&state->lock);
        return -EIO;
    }
    long long data_length = atoll(line + 5);
    free(line);
    if (data_length < 0 || (size_t) data_length > size) {
        pthread_mutex_unlock(&state->lock);
        return -EIO;
    }
    result = read_exact_fd(state->bridge_out, buf, (size_t) data_length);
    if (result != 0) {
        pthread_mutex_unlock(&state->lock);
        return result;
    }
    char newline;
    result = read_exact_fd(state->bridge_out, &newline, 1);
    if (result != 0 || newline != '\n') {
        pthread_mutex_unlock(&state->lock);
        return -EIO;
    }
    line = read_line_fd(state->bridge_out);
    if (line == NULL || strcmp(line, "END") != 0) {
        free(line);
        pthread_mutex_unlock(&state->lock);
        return -EIO;
    }
    free(line);
    *bytes_read = (size_t) data_length;
    pthread_mutex_unlock(&state->lock);
    return 0;
}

static int bridge_read_to_fd(struct cxnfs_state *state, const char *remote, int fd, long long size) {
    const size_t chunk_size = 1024 * 1024;
    char *buffer = malloc(chunk_size);
    if (buffer == NULL) {
        return -ENOMEM;
    }
    long long offset = 0;
    while (offset < size) {
        size_t requested = (size_t) ((size - offset) < (long long) chunk_size ? (size - offset) : (long long) chunk_size);
        size_t read_count = 0;
        int result = bridge_read_range(state, remote, offset, requested, buffer, &read_count);
        if (result != 0) {
            free(buffer);
            return result;
        }
        if (read_count == 0) {
            break;
        }
        ssize_t written = pwrite(fd, buffer, read_count, (off_t) offset);
        if (written < 0 || (size_t) written != read_count) {
            free(buffer);
            return written < 0 ? -errno : -EIO;
        }
        offset += (long long) read_count;
    }
    free(buffer);
    return 0;
}

static int bridge_write_fd(
        struct cxnfs_state *state,
        const char *remote,
        const char *local_path,
        int fd
) {
    off_t size = lseek(fd, 0, SEEK_END);
    if (size < 0) {
        return -errno;
    }
    if (lseek(fd, 0, SEEK_SET) < 0) {
        return -errno;
    }

    pthread_mutex_lock(&state->lock);
    char header[1024];
    int header_length = snprintf(header, sizeof(header), "WRITE\t%s\t%lld\n", remote, (long long) size);
    if (header_length < 0 || (size_t) header_length >= sizeof(header)) {
        pthread_mutex_unlock(&state->lock);
        return -EINVAL;
    }
    int result = write_all(state->bridge_in, header, (size_t) header_length);
    if (result != 0) {
        pthread_mutex_unlock(&state->lock);
        return result;
    }

    size_t buffer_size = commit_buffer_bytes();
    char *buffer = malloc(buffer_size);
    if (buffer == NULL) {
        pthread_mutex_unlock(&state->lock);
        return -ENOMEM;
    }
    while (1) {
        ssize_t read_count = read(fd, buffer, buffer_size);
        if (read_count < 0) {
            if (errno == EINTR) {
                continue;
            }
            free(buffer);
            pthread_mutex_unlock(&state->lock);
            return -errno;
        }
        if (read_count == 0) {
            break;
        }
        result = write_all(state->bridge_in, buffer, (size_t) read_count);
        if (result != 0) {
            free(buffer);
            pthread_mutex_unlock(&state->lock);
            return result;
        }
    }
    free(buffer);
    result = write_all(state->bridge_in, "\nEND\n", 5);
    if (result == 0) {
        result = read_bridge_ok_locked(state, "write");
    }
    if (result == 0 && local_path != NULL) {
        local_dir_remove_ancestors_locked(state, local_path);
        local_file_remove_locked(state, local_path);
    }
    pthread_mutex_unlock(&state->lock);
    return result;
}

static int bridge_write_chunk(
        struct cxnfs_state *state,
        const char *remote,
        long long offset,
        const char *data,
        size_t size
) {
    if (size == 0) {
        return 0;
    }
    if (state != NULL && state->satfs_client != NULL) {
        return satfs_client_write_chunk(state->satfs_client, remote, (uint64_t) offset, data, size);
    }
    while (size > CXNFS_FUSE_BRIDGE_MAX_CHUNK_BYTES) {
        int result = bridge_write_chunk(
                state,
                remote,
                offset,
                data,
                (size_t) CXNFS_FUSE_BRIDGE_MAX_CHUNK_BYTES
        );
        if (result != 0) {
            return result;
        }
        offset += (long long) CXNFS_FUSE_BRIDGE_MAX_CHUNK_BYTES;
        data += CXNFS_FUSE_BRIDGE_MAX_CHUNK_BYTES;
        size -= CXNFS_FUSE_BRIDGE_MAX_CHUNK_BYTES;
    }
    pthread_mutex_lock(&state->lock);
    char header[1024];
    int header_length = snprintf(header, sizeof(header), "WRITE_CHUNK\t%s\t%lld\t%zu\n", remote, offset, size);
    if (header_length < 0 || (size_t) header_length >= sizeof(header)) {
        pthread_mutex_unlock(&state->lock);
        return -EINVAL;
    }
    int result = write_all(state->bridge_in, header, (size_t) header_length);
    if (result == 0) {
        result = write_all(state->bridge_in, data, size);
    }
    if (result == 0) {
        result = read_bridge_ok_locked(state, "write chunk");
    }
    pthread_mutex_unlock(&state->lock);
    return result;
}

static int bridge_write_commit(
        struct cxnfs_state *state,
        const char *remote,
        const char *local_path,
        long long size
) {
    if (state != NULL && state->satfs_client != NULL) {
        int r = satfs_client_commit_write(state->satfs_client, remote, (uint64_t) size);
        if (r == 0 && local_path != NULL) {
            pthread_mutex_lock(&state->lock);
            local_dir_remove_ancestors_locked(state, local_path);
            local_file_remove_locked(state, local_path);
            pthread_mutex_unlock(&state->lock);
        }
        return r;
    }
    pthread_mutex_lock(&state->lock);
    char header[1024];
    int header_length = snprintf(header, sizeof(header), "WRITE_COMMIT\t%s\t%lld\n", remote, size);
    if (header_length < 0 || (size_t) header_length >= sizeof(header)) {
        pthread_mutex_unlock(&state->lock);
        return -EINVAL;
    }
    int result = write_all(state->bridge_in, header, (size_t) header_length);
    if (result == 0) {
        result = read_bridge_ok_locked(state, "write commit");
    }
    if (result == 0 && local_path != NULL) {
        local_dir_remove_ancestors_locked(state, local_path);
        local_file_remove_locked(state, local_path);
    }
    pthread_mutex_unlock(&state->lock);
    return result;
}

static int bridge_write_abort(struct cxnfs_state *state, const char *remote) {
    if (state != NULL && state->satfs_client != NULL) {
        return satfs_client_abort_write(state->satfs_client, remote);
    }
    pthread_mutex_lock(&state->lock);
    char header[1024];
    int header_length = snprintf(header, sizeof(header), "WRITE_ABORT\t%s\n", remote);
    if (header_length < 0 || (size_t) header_length >= sizeof(header)) {
        pthread_mutex_unlock(&state->lock);
        return -EINVAL;
    }
    int result = write_all(state->bridge_in, header, (size_t) header_length);
    if (result == 0) {
        result = read_bridge_ok_locked(state, "write abort");
    }
    pthread_mutex_unlock(&state->lock);
    return result;
}

static int bridge_delete(struct cxnfs_state *state, const char *remote, int recursive) {
    if (state != NULL && state->satfs_client != NULL) {
        return satfs_client_delete(state->satfs_client, remote, recursive);
    }
    pthread_mutex_lock(&state->lock);
    char header[1024];
    int header_length = snprintf(header, sizeof(header), "DELETE\t%s\t%s\n", remote, recursive ? "true" : "false");
    if (header_length < 0 || (size_t) header_length >= sizeof(header)) {
        pthread_mutex_unlock(&state->lock);
        return -EINVAL;
    }
    int result = write_all(state->bridge_in, header, (size_t) header_length);
    if (result == 0) {
        result = read_bridge_ok_locked(state, "delete");
    }
    pthread_mutex_unlock(&state->lock);
    return result;
}

static int bridge_rename(struct cxnfs_state *state, const char *source, const char *target) {
    pthread_mutex_lock(&state->lock);
    char header[2048];
    int header_length = snprintf(header, sizeof(header), "RENAME\t%s\t%s\n", source, target);
    if (header_length < 0 || (size_t) header_length >= sizeof(header)) {
        pthread_mutex_unlock(&state->lock);
        return -EINVAL;
    }
    int result = write_all(state->bridge_in, header, (size_t) header_length);
    if (result == 0) {
        result = read_bridge_ok_locked(state, "rename");
    }
    pthread_mutex_unlock(&state->lock);
    return result;
}

static int cxnfs_getattr(const char *path, struct stat *stbuf) {
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    if (is_appledouble_path(path)) {
        pthread_mutex_lock(&state->lock);
        struct cxnfs_stat local_file_metadata;
        int local_file = local_file_stat_locked(state, path, &local_file_metadata);
        pthread_mutex_unlock(&state->lock);
        if (local_file == 0) {
            fill_stat(state, stbuf, &local_file_metadata);
            return 0;
        }
        return -ENOENT;
    }
    char *remote = remote_path_for(state, path);
    if (remote == NULL) {
        return -EINVAL;
    }
    struct cxnfs_stat metadata;
    int result = bridge_stat(state, remote, &metadata);
    free(remote);
    if (result != 0) {
        if (result == -ENOENT) {
            pthread_mutex_lock(&state->lock);
            struct cxnfs_stat local_file_metadata;
            int local_file = local_file_stat_locked(state, path, &local_file_metadata);
            int local_directory = local_dir_exists_locked(state, path);
            pthread_mutex_unlock(&state->lock);
            if (local_file == 0) {
                fill_stat(state, stbuf, &local_file_metadata);
                return 0;
            }
            if (local_directory) {
                struct cxnfs_stat local_metadata = {
                        .directory = 1,
                        .size = 0,
                        .mtime_ms = (long long) time(NULL) * 1000
                };
                fill_stat(state, stbuf, &local_metadata);
                return 0;
            }
        }
        return result;
    }
    fill_stat(state, stbuf, &metadata);
    return 0;
}

static int cxnfs_readdir(
        const char *path,
        void *buf,
        fuse_fill_dir_t filler,
        off_t offset,
        struct fuse_file_info *fi
) {
    (void) offset;
    (void) fi;
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    char *remote = remote_path_for(state, path);
    if (remote == NULL) {
        return -EINVAL;
    }

    pthread_mutex_lock(&state->lock);
    int result = send_command(state, "LIST", remote);
    free(remote);
    if (result != 0) {
        pthread_mutex_unlock(&state->lock);
        return result;
    }
    char *line = read_line_fd(state->bridge_out);
    if (line == NULL) {
        pthread_mutex_unlock(&state->lock);
        return -EIO;
    }
    if (strncmp(line, "ERR\t", 4) == 0) {
        int local_directory = local_dir_exists_locked(state, path);
        if (!local_directory) {
            fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "bridge list failed: %s\n", line + 4);
        }
        free(line);
        if (local_directory) {
            filler(buf, ".", NULL, 0);
            filler(buf, "..", NULL, 0);
            struct cxnfs_stat local_dir_metadata = {
                    .directory = 1,
                    .size = 0,
                    .mtime_ms = (long long) time(NULL) * 1000
            };
            struct stat local_dir_stat;
            fill_stat(state, &local_dir_stat, &local_dir_metadata);
            for (size_t index = 0; index < state->local_dir_count; index++) {
                const char *name = NULL;
                if (local_dir_parent_name(state->local_dirs[index], path, &name)) {
                    filler(buf, name, &local_dir_stat, 0);
                }
            }
            for (size_t index = 0; index < state->local_file_count; index++) {
                if (is_appledouble_path(state->local_files[index].path)) {
                    continue;
                }
                const char *name = NULL;
                if (local_dir_parent_name(state->local_files[index].path, path, &name)) {
                    struct cxnfs_stat local_file_metadata = {
                            .directory = 0,
                            .size = state->local_files[index].size,
                            .mtime_ms = state->local_files[index].mtime_ms
                    };
                    struct stat local_file_stat;
                    fill_stat(state, &local_file_stat, &local_file_metadata);
                    filler(buf, name, &local_file_stat, 0);
                }
            }
            pthread_mutex_unlock(&state->lock);
            return 0;
        }
        pthread_mutex_unlock(&state->lock);
        return -EIO;
    }
    if (strncmp(line, "OK\t", 3) != 0) {
        free(line);
        pthread_mutex_unlock(&state->lock);
        return -EIO;
    }
    free(line);

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    struct cxnfs_stat local_dir_metadata = {
            .directory = 1,
            .size = 0,
            .mtime_ms = (long long) time(NULL) * 1000
    };
    struct stat local_dir_stat;
    fill_stat(state, &local_dir_stat, &local_dir_metadata);
    for (size_t index = 0; index < state->local_dir_count; index++) {
        const char *name = NULL;
        if (local_dir_parent_name(state->local_dirs[index], path, &name)) {
            filler(buf, name, &local_dir_stat, 0);
        }
    }
    for (size_t index = 0; index < state->local_file_count; index++) {
        if (is_appledouble_path(state->local_files[index].path)) {
            continue;
        }
        const char *name = NULL;
        if (local_dir_parent_name(state->local_files[index].path, path, &name)) {
            struct cxnfs_stat local_file_metadata = {
                    .directory = 0,
                    .size = state->local_files[index].size,
                    .mtime_ms = state->local_files[index].mtime_ms
            };
            struct stat local_file_stat;
            fill_stat(state, &local_file_stat, &local_file_metadata);
            filler(buf, name, &local_file_stat, 0);
        }
    }
    while ((line = read_line_fd(state->bridge_out)) != NULL) {
        if (strcmp(line, "END") == 0) {
            free(line);
            pthread_mutex_unlock(&state->lock);
            return 0;
        }
        if (strncmp(line, "ENTRY\t", 6) != 0) {
            free(line);
            pthread_mutex_unlock(&state->lock);
            return -EIO;
        }
        char *save = NULL;
        strtok_r(line, "\t", &save);
        char *type = strtok_r(NULL, "\t", &save);
        char *size = strtok_r(NULL, "\t", &save);
        char *mtime = strtok_r(NULL, "\t", &save);
        char *name = strtok_r(NULL, "\t", &save);
        if (type == NULL || size == NULL || mtime == NULL || name == NULL) {
            free(line);
            pthread_mutex_unlock(&state->lock);
            return -EIO;
        }
        if (is_appledouble_name(name)) {
            free(line);
            continue;
        }
        struct cxnfs_stat metadata = {
                .directory = strcmp(type, "DIRECTORY") == 0,
                .size = atoll(size),
                .mtime_ms = atoll(mtime)
        };
        struct stat entry_stat;
        fill_stat(state, &entry_stat, &metadata);
        if (filler(buf, name, &entry_stat, 0) != 0) {
            free(line);
            pthread_mutex_unlock(&state->lock);
            return 0;
        }
        free(line);
    }
    pthread_mutex_unlock(&state->lock);
    return -EIO;
}

static void destroy_file_handle(struct cxnfs_file_handle *handle) {
    if (handle == NULL) {
        return;
    }
    if (handle->fd >= 0) {
        close(handle->fd);
    }
    if (handle->temp_path != NULL) {
        cleanup_temp_artifact(handle->temp_path, handle->temp_dir, handle->temp_cleanup_stop);
    }
    free(handle->remote);
    free(handle->local_path);
    free(handle->temp_path);
    free(handle->temp_dir);
    free(handle->temp_cleanup_stop);
    free(handle);
}

static int ensure_handle_hydrated(struct cxnfs_state *state, struct cxnfs_file_handle *handle) {
    if (handle == NULL) {
        return -EBADF;
    }
    if (handle->hydrated) {
        return 0;
    }
    if (ftruncate(handle->fd, 0) != 0) {
        return -errno;
    }
    if (is_appledouble_path(handle->local_path)) {
        if (handle->remote_size > 0 && ftruncate(handle->fd, (off_t) handle->remote_size) != 0) {
            return -errno;
        }
        handle->hydrated = 1;
        if (handle->append && lseek(handle->fd, 0, SEEK_END) < 0) {
            return -errno;
        }
        return 0;
    }
    if (handle->remote_size > 0) {
        int result = bridge_read_to_fd(state, handle->remote, handle->fd, handle->remote_size);
        if (result == -ENOSPC) {
            int migrate_result = migrate_handle_to_fallback(state, handle);
            if (migrate_result == 0) {
                if (ftruncate(handle->fd, 0) != 0) {
                    return -errno;
                }
                result = bridge_read_to_fd(state, handle->remote, handle->fd, handle->remote_size);
            }
        }
        if (result != 0) {
            return result;
        }
    }
    handle->hydrated = 1;
    if (handle->append && lseek(handle->fd, 0, SEEK_END) < 0) {
        return -errno;
    }
    return 0;
}

static int should_stream_write(const char *path, int flags, int create_empty) {
    return !is_appledouble_path(path)
            && ((flags & O_APPEND) == 0)
            && (create_empty || ((flags & O_TRUNC) != 0));
}

static void disable_streaming_write(
        struct cxnfs_state *state,
        struct cxnfs_file_handle *handle,
        int send_abort
) {
    if (handle == NULL || !handle->stream_enabled) {
        return;
    }
    if (send_abort && handle->stream_started) {
        int abort_result = bridge_write_abort(state, handle->remote);
        if (abort_result != 0) {
            fprintf(stderr,
                    CXNFS_FUSE_LOG_PREFIX "bridge write abort failed for %s: %s\n",
                    handle->remote,
                    strerror(-abort_result));
        }
    }
    handle->stream_enabled = 0;
    handle->stream_started = 0;
    handle->stream_disabled = 1;
}

static int create_write_handle(
        struct cxnfs_state *state,
        const char *path,
        char *remote,
        int flags,
        int create_empty,
        struct cxnfs_file_handle **out
) {
    struct cxnfs_file_handle *handle = calloc(1, sizeof(*handle));
    if (handle == NULL) {
        free(remote);
        return -ENOMEM;
    }
    handle->fd = -1;
    handle->remote = remote;
    handle->local_path = strdup(path);
    handle->writable = 1;
    handle->dirty = create_empty || ((flags & O_TRUNC) != 0);
    handle->hydrated = create_empty || ((flags & O_TRUNC) != 0);
    handle->append = (flags & O_APPEND) != 0;
    handle->stream_enabled = should_stream_write(path, flags, create_empty);
    handle->remote_size = 0;
    handle->remote_mtime_ms = 0;
    handle->stream_next_offset = 0;
    if (handle->local_path == NULL) {
        destroy_file_handle(handle);
        return -ENOMEM;
    }

    int temp_result = create_initial_temp_file(state, handle);
    if (temp_result != 0) {
        destroy_file_handle(handle);
        return temp_result;
    }

    if (is_appledouble_path(path)) {
        struct cxnfs_stat local_metadata;
        pthread_mutex_lock(&state->lock);
        int local_file = local_file_stat_locked(state, path, &local_metadata);
        pthread_mutex_unlock(&state->lock);
        if (local_file == 0) {
            handle->remote_size = local_metadata.size;
            handle->remote_mtime_ms = local_metadata.mtime_ms;
        }
    } else if (!create_empty && (flags & O_TRUNC) == 0) {
        struct cxnfs_stat metadata;
        int result = bridge_stat(state, handle->remote, &metadata);
        if (result != 0) {
            destroy_file_handle(handle);
            return result;
        }
        if (metadata.directory) {
            destroy_file_handle(handle);
            return -EISDIR;
        }
        handle->remote_size = metadata.size;
        handle->remote_mtime_ms = metadata.mtime_ms;
    }

    if (handle->append) {
        int result = ensure_handle_hydrated(state, handle);
        if (result != 0) {
            destroy_file_handle(handle);
            return result;
        }
    }
    *out = handle;
    return 0;
}

static int commit_file_handle(struct cxnfs_state *state, struct cxnfs_file_handle *handle) {
    if (handle == NULL || !handle->writable || !handle->dirty) {
        return 0;
    }
    if (is_appledouble_path(handle->local_path)) {
        off_t size = lseek(handle->fd, 0, SEEK_END);
        if (size < 0) {
            return -errno;
        }
        pthread_mutex_lock(&state->lock);
        int result = local_file_put_locked(state, handle->local_path, (long long) size);
        pthread_mutex_unlock(&state->lock);
        if (result == 0) {
            handle->dirty = 0;
        }
        return result;
    }
    off_t size = lseek(handle->fd, 0, SEEK_END);
    if (size < 0) {
        return -errno;
    }
    int result;
    if (handle->stream_enabled
            && handle->stream_started
            && (long long) size == handle->stream_next_offset) {
        result = bridge_write_commit(state, handle->remote, handle->local_path, (long long) size);
        if (result == 0) {
            handle->dirty = 0;
            handle->stream_enabled = 0;
            handle->stream_started = 0;
            handle->stream_disabled = 1;
            return 0;
        }
        fprintf(stderr,
                CXNFS_FUSE_LOG_PREFIX "streaming commit failed for %s, falling back to local staged upload: %s\n",
                handle->remote,
                strerror(-result));
        handle->stream_enabled = 0;
        handle->stream_started = 0;
        handle->stream_disabled = 1;
    }
    result = bridge_write_fd(state, handle->remote, handle->local_path, handle->fd);
    if (result == 0) {
        handle->dirty = 0;
    }
    return result;
}

static int cxnfs_open(const char *path, struct fuse_file_info *fi) {
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    char *remote = remote_path_for(state, path);
    if (remote == NULL) {
        return -EINVAL;
    }
    int access_mode = fi->flags & O_ACCMODE;
    if (access_mode == O_WRONLY || access_mode == O_RDWR) {
        struct cxnfs_file_handle *handle = NULL;
        int result = create_write_handle(state, path, remote, fi->flags, 0, &handle);
        if (result != 0) {
            return result;
        }
        fi->fh = (uint64_t) (uintptr_t) handle;
        fi->direct_io = 1;
        return 0;
    }
    struct cxnfs_stat metadata;
    int result = bridge_stat(state, remote, &metadata);
    free(remote);
    if (result != 0) {
        return result;
    }
    if (metadata.directory) {
        return -EISDIR;
    }
    return 0;
}

static int cxnfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    (void) mode;
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    char *remote = remote_path_for(state, path);
    if (remote == NULL) {
        return -EINVAL;
    }
    struct cxnfs_file_handle *handle = NULL;
    int result = create_write_handle(state, path, remote, fi->flags, 1, &handle);
    if (result != 0) {
        return result;
    }
    fi->fh = (uint64_t) (uintptr_t) handle;
    fi->direct_io = 1;
    pthread_mutex_lock(&state->lock);
    result = local_file_put_locked(state, path, 0);
    pthread_mutex_unlock(&state->lock);
    if (result != 0) {
        destroy_file_handle(handle);
        fi->fh = 0;
        return result;
    }
    return 0;
}

static int cxnfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    if (fi != NULL && fi->fh != 0) {
        struct cxnfs_file_handle *handle = (struct cxnfs_file_handle *) (uintptr_t) fi->fh;
        if (is_appledouble_path(handle->local_path) && !handle->hydrated && !handle->dirty) {
            if (offset < 0 || (long long) offset >= handle->remote_size) {
                return 0;
            }
            size_t available = (size_t) (handle->remote_size - (long long) offset);
            size_t returned = size < available ? size : available;
            memset(buf, 0, returned);
            return (int) returned;
        }
        // Demand-driven read: do not stage or hydrate whole file to /tmp on read
        if (!handle->dirty) {
            struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
            size_t data_length = 0;
            int result = bridge_read_range(state, handle->remote, (long long) offset, size, buf, &data_length);
            if (result != 0) {
                return result;
            }
            return (int) data_length;
        }
        ssize_t read_count = pread(handle->fd, buf, size, offset);
        return read_count < 0 ? -errno : (int) read_count;
    }
    if (is_appledouble_path(path)) {
        struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
        pthread_mutex_lock(&state->lock);
        struct cxnfs_stat local_file_metadata;
        int local_file = local_file_stat_locked(state, path, &local_file_metadata);
        pthread_mutex_unlock(&state->lock);
        if (local_file != 0) {
            return -ENOENT;
        }
        if (offset < 0 || (long long) offset >= local_file_metadata.size) {
            return 0;
        }
        size_t available = (size_t) (local_file_metadata.size - (long long) offset);
        size_t returned = size < available ? size : available;
        memset(buf, 0, returned);
        return (int) returned;
    }
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    char *remote = remote_path_for(state, path);
    if (remote == NULL) {
        return -EINVAL;
    }
    size_t data_length = 0;
    int result = bridge_read_range(state, remote, (long long) offset, size, buf, &data_length);
    free(remote);
    if (result != 0) {
        return result;
    }
    return (int) data_length;
}

static int cxnfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    (void) path;
    if (fi == NULL || fi->fh == 0) {
        return -EBADF;
    }
    struct cxnfs_file_handle *handle = (struct cxnfs_file_handle *) (uintptr_t) fi->fh;
    if (!handle->writable) {
        return -EACCES;
    }
    if (!handle->hydrated && !handle->stream_enabled) {
        struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
        int result = ensure_handle_hydrated(state, handle);
        if (result != 0) {
            return result;
        }
    }
    if (handle->append) {
        off_t end = lseek(handle->fd, 0, SEEK_END);
        if (end < 0) {
            return -errno;
        }
        offset = end;
    }
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    if (handle->stream_enabled && (long long) offset != handle->stream_next_offset) {
        disable_streaming_write(state, handle, 1);
    }
    if (offset >= 0 && size > 0) {
        unsigned long long target_size = (unsigned long long) offset + (unsigned long long) size;
        int capacity_result = ensure_handle_capacity(state, handle, target_size);
        if (capacity_result != 0) {
            return capacity_result;
        }
    }
    ssize_t written = pwrite(handle->fd, buf, size, offset);
    if (written < 0 && errno == ENOSPC) {
        struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
        int migrate_result = migrate_handle_to_fallback(state, handle);
        if (migrate_result == 0) {
            written = pwrite(handle->fd, buf, size, offset);
        } else {
            return migrate_result;
        }
    }
    if (written < 0) {
        return -errno;
    }
    if (handle->stream_enabled && written > 0) {
        int stream_result = bridge_write_chunk(
                state,
                handle->remote,
                (long long) offset,
                buf,
                (size_t) written
        );
        if (stream_result != 0) {
            fprintf(stderr,
                    CXNFS_FUSE_LOG_PREFIX "streaming write disabled for %s, falling back to local staged upload: %s\n",
                    handle->remote,
                    strerror(-stream_result));
            handle->stream_enabled = 0;
            handle->stream_started = 0;
            handle->stream_disabled = 1;
            return (int) written;
        }
        handle->stream_started = 1;
        handle->stream_next_offset += (long long) written;
    }
    handle->dirty = 1;
    struct stat temp_stat;
    if (fstat(handle->fd, &temp_stat) == 0) {
        pthread_mutex_lock(&state->lock);
        local_file_put_locked(state, handle->local_path, (long long) temp_stat.st_size);
        pthread_mutex_unlock(&state->lock);
    }
    return (int) written;
}

#if defined(CXNFS_FUSE_LINUX) && defined(CXNFS_FUSE_HAS_COPY_FILE_RANGE)
static ssize_t cxnfs_copy_file_range(
        const char *path_in,
        struct fuse_file_info *fi_in,
        off_t offset_in,
        const char *path_out,
        struct fuse_file_info *fi_out,
        off_t offset_out,
        size_t size,
        int flags
) {
    (void) fi_in;
    if (flags != 0) {
        return -EINVAL;
    }
    if (offset_in < 0 || offset_out < 0) {
        return -EINVAL;
    }
    if (fi_out == NULL || fi_out->fh == 0) {
        return -EBADF;
    }
    if (size == 0) {
        return 0;
    }

    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    char *remote = remote_path_for(state, path_in);
    if (remote == NULL) {
        return -EINVAL;
    }
    const size_t chunk_size = 4 * 1024 * 1024;
    char *buffer = malloc(chunk_size);
    if (buffer == NULL) {
        free(remote);
        return -ENOMEM;
    }

    size_t copied = 0;
    int result = 0;
    while (copied < size) {
        size_t requested = size - copied;
        if (requested > chunk_size) {
            requested = chunk_size;
        }
        size_t read_count = 0;
        result = bridge_read_range(
                state,
                remote,
                (long long) offset_in + (long long) copied,
                requested,
                buffer,
                &read_count
        );
        if (result != 0 || read_count == 0) {
            break;
        }
        int written = cxnfs_write(
                path_out,
                buffer,
                read_count,
                offset_out + (off_t) copied,
                fi_out
        );
        if (written < 0) {
            result = written;
            break;
        }
        if (written == 0) {
            break;
        }
        copied += (size_t) written;
        if ((size_t) written < read_count) {
            break;
        }
    }

    free(buffer);
    free(remote);
    if (copied > 0) {
        return (ssize_t) copied;
    }
    return result;
}
#endif

static int cxnfs_flush(const char *path, struct fuse_file_info *fi) {
    (void) path;
    if (fi == NULL || fi->fh == 0) {
        return 0;
    }
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    struct cxnfs_file_handle *handle = (struct cxnfs_file_handle *) (uintptr_t) fi->fh;
    return commit_file_handle(state, handle);
}

static int cxnfs_release(const char *path, struct fuse_file_info *fi) {
    (void) path;
    if (fi == NULL || fi->fh == 0) {
        return 0;
    }
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    struct cxnfs_file_handle *handle = (struct cxnfs_file_handle *) (uintptr_t) fi->fh;
    int result = commit_file_handle(state, handle);
    if (result != 0) {
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "failed to commit %s: %s\n", handle->remote, strerror(-result));
    }
    destroy_file_handle(handle);
    fi->fh = 0;
    return result;
}

static int cxnfs_ftruncate(const char *path, off_t size, struct fuse_file_info *fi) {
    (void) path;
    if (size < 0) {
        return -EINVAL;
    }
    if (fi == NULL || fi->fh == 0) {
        return -EBADF;
    }
    struct cxnfs_file_handle *handle = (struct cxnfs_file_handle *) (uintptr_t) fi->fh;
    if (!handle->hydrated) {
        if (handle->remote_size == size) {
            return 0;
        }
        if (size != 0) {
            struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
            int result = ensure_handle_hydrated(state, handle);
            if (result != 0) {
                return result;
            }
        } else {
            handle->hydrated = 1;
        }
    }
    struct stat temp_stat;
    if (fstat(handle->fd, &temp_stat) != 0) {
        return -errno;
    }
    if (temp_stat.st_size == size) {
        return 0;
    }
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    if (handle->stream_enabled && (long long) size != handle->stream_next_offset) {
        disable_streaming_write(state, handle, 1);
    }
    if (size > temp_stat.st_size) {
        int capacity_result = ensure_handle_capacity(state, handle, (unsigned long long) size);
        if (capacity_result != 0) {
            return capacity_result;
        }
    }
    if (ftruncate(handle->fd, size) != 0) {
        if (errno == ENOSPC) {
            int migrate_result = migrate_handle_to_fallback(state, handle);
            if (migrate_result == 0 && ftruncate(handle->fd, size) == 0) {
                goto truncated;
            }
            return migrate_result == 0 ? -errno : migrate_result;
        }
        return -errno;
    }
truncated:
    handle->dirty = 1;
    pthread_mutex_lock(&state->lock);
    local_file_put_locked(state, handle->local_path, (long long) size);
    pthread_mutex_unlock(&state->lock);
    return 0;
}

static int cxnfs_fgetattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi) {
    (void) path;
    if (fi == NULL || fi->fh == 0) {
        return cxnfs_getattr(path, stbuf);
    }
    struct cxnfs_file_handle *handle = (struct cxnfs_file_handle *) (uintptr_t) fi->fh;
    if (!handle->hydrated && !handle->dirty) {
        struct cxnfs_stat metadata = {
                .directory = 0,
                .size = handle->remote_size,
                .mtime_ms = handle->remote_mtime_ms
        };
        struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
        fill_stat(state, stbuf, &metadata);
        return 0;
    }
    if (!handle->hydrated) {
        struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
        int result = ensure_handle_hydrated(state, handle);
        if (result != 0) {
            return result;
        }
    }
    struct stat temp_stat;
    if (fstat(handle->fd, &temp_stat) != 0) {
        return -errno;
    }
    memset(stbuf, 0, sizeof(*stbuf));
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    stbuf->st_uid = state->owner_uid;
    stbuf->st_gid = state->owner_gid;
    stbuf->st_size = temp_stat.st_size;
    stbuf->st_nlink = 1;
    stbuf->st_mode = S_IFREG | 0644;
    stbuf->st_mtime = temp_stat.st_mtime;
    return 0;
}

static int cxnfs_truncate(const char *path, off_t size) {
    if (size < 0) {
        return -EINVAL;
    }
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    if (is_appledouble_path(path)) {
        pthread_mutex_lock(&state->lock);
        int result = local_file_put_locked(state, path, (long long) size);
        pthread_mutex_unlock(&state->lock);
        return result;
    }
    char *remote = remote_path_for(state, path);
    if (remote == NULL) {
        return -EINVAL;
    }
    struct cxnfs_stat metadata;
    int stat_result = bridge_stat(state, remote, &metadata);
    if (stat_result == 0) {
        if (metadata.directory) {
            free(remote);
            return -EISDIR;
        }
        if (metadata.size == size) {
            free(remote);
            return 0;
        }
    }
    struct cxnfs_file_handle *handle = NULL;
    int result = create_write_handle(state, path, remote, 0, 0, &handle);
    if (result != 0) {
        return result;
    }
    if (size != 0) {
        result = ensure_handle_hydrated(state, handle);
    }
    if (result == 0) {
        if (size > 0) {
            result = ensure_handle_capacity(state, handle, (unsigned long long) size);
        }
    }
    if (result == 0) {
        if (ftruncate(handle->fd, size) != 0) {
            if (errno == ENOSPC) {
                result = migrate_handle_to_fallback(state, handle);
                if (result == 0 && ftruncate(handle->fd, size) != 0) {
                    result = -errno;
                }
            } else {
                result = -errno;
            }
        }
    }
    if (result == 0) {
        handle->dirty = 1;
        result = commit_file_handle(state, handle);
    }
    destroy_file_handle(handle);
    return result;
}

static int cxnfs_access(const char *path, int mask) {
    (void) mask;
    struct stat stbuf;
    return cxnfs_getattr(path, &stbuf);
}

static int cxnfs_mkdir(const char *path, mode_t mode) {
    (void) mode;
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    pthread_mutex_lock(&state->lock);
    int result = local_dir_add_locked(state, path);
    pthread_mutex_unlock(&state->lock);
    return result;
}

static int cxnfs_unlink(const char *path) {
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    if (is_appledouble_path(path)) {
        pthread_mutex_lock(&state->lock);
        local_file_remove_locked(state, path);
        pthread_mutex_unlock(&state->lock);
        return 0;
    }
    char *remote = remote_path_for(state, path);
    if (remote == NULL) {
        return -EINVAL;
    }
    int result = bridge_delete(state, remote, 0);
    free(remote);
    if (result == -ENOENT) {
        pthread_mutex_lock(&state->lock);
        result = local_file_remove_locked(state, path);
        pthread_mutex_unlock(&state->lock);
    }
    return result;
}

static int cxnfs_rmdir(const char *path) {
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    pthread_mutex_lock(&state->lock);
    int local_directory = local_dir_exists_locked(state, path);
    int has_local_child = local_dir_has_child_locked(state, path);
    pthread_mutex_unlock(&state->lock);
    if (local_directory && has_local_child) {
        return -ENOTEMPTY;
    }

    char *remote = remote_path_for(state, path);
    if (remote == NULL) {
        return -EINVAL;
    }
    int result = bridge_delete(state, remote, 0);
    free(remote);
    if (result == -ENOENT && local_directory) {
        result = 0;
    }
    if (result == 0 && local_directory) {
        pthread_mutex_lock(&state->lock);
        local_dir_remove_locked(state, path);
        pthread_mutex_unlock(&state->lock);
    }
    return result;
}

static int cxnfs_rename(const char *from, const char *to) {
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    if (is_appledouble_path(from) || is_appledouble_path(to)) {
        pthread_mutex_lock(&state->lock);
        int result = local_file_rename_locked(state, from, to);
        if (result == -ENOENT && is_appledouble_path(to)) {
            result = local_file_put_locked(state, to, 0);
        }
        pthread_mutex_unlock(&state->lock);
        return result == -ENOENT ? 0 : result;
    }
    pthread_mutex_lock(&state->lock);
    int local_directory = local_dir_exists_locked(state, from);
    pthread_mutex_unlock(&state->lock);

    char *source = remote_path_for(state, from);
    char *target = remote_path_for(state, to);
    if (source == NULL || target == NULL) {
        free(source);
        free(target);
        return -EINVAL;
    }
    int result = bridge_rename(state, source, target);
    free(source);
    free(target);
    if (result == -ENOENT) {
        pthread_mutex_lock(&state->lock);
        result = local_file_rename_locked(state, from, to);
        pthread_mutex_unlock(&state->lock);
    }
    if (result == -ENOENT && local_directory) {
        result = 0;
    }
    if (result == 0 && local_directory) {
        pthread_mutex_lock(&state->lock);
        local_dir_remove_locked(state, from);
        local_dir_add_locked(state, to);
        pthread_mutex_unlock(&state->lock);
    }
    return result;
}

static int cxnfs_chmod(const char *path, mode_t mode) {
    (void) path;
    (void) mode;
    return 0;
}

static int cxnfs_chown(const char *path, uid_t uid, gid_t gid) {
    (void) path;
    (void) uid;
    (void) gid;
    return 0;
}

static int cxnfs_utimens(const char *path, const struct timespec tv[2]) {
    (void) path;
    (void) tv;
    return 0;
}

static int cxnfs_statfs(const char *path, struct statvfs *stbuf) {
    (void) path;
    struct cxnfs_state *state = (struct cxnfs_state *) fuse_get_context()->private_data;
    struct statvfs primary;
    struct statvfs fallback;
    int have_primary = state != NULL
            && state->staging_root != NULL
            && statvfs(state->staging_root, &primary) == 0;
    int have_fallback = state != NULL
            && state->fallback_staging_root != NULL
            && statvfs(state->fallback_staging_root, &fallback) == 0;
    if (!have_primary && !have_fallback) {
        if (statvfs("/tmp", stbuf) != 0) {
            return -errno;
        }
    } else if (have_primary && have_fallback) {
        unsigned long long primary_available = (unsigned long long) primary.f_bavail
                * (unsigned long long) primary.f_frsize;
        unsigned long long fallback_available = (unsigned long long) fallback.f_bavail
                * (unsigned long long) fallback.f_frsize;
        *stbuf = fallback_available > primary_available ? fallback : primary;
    } else {
        *stbuf = have_primary ? primary : fallback;
    }
    stbuf->f_namemax = 255;
    return 0;
}

static int cxnfs_getxattr(
        const char *path,
        const char *name,
        char *value,
        size_t size
#ifndef CXNFS_FUSE_LINUX
        , uint32_t position
#endif
) {
    (void) path;
    (void) name;
    (void) value;
    (void) size;
#ifndef CXNFS_FUSE_LINUX
    (void) position;
#endif
#ifdef ENOATTR
    return -ENOATTR;
#else
    return -ENODATA;
#endif
}

static int cxnfs_setxattr(
        const char *path,
        const char *name,
        const char *value,
        size_t size,
        int flags
#ifndef CXNFS_FUSE_LINUX
        , uint32_t position
#endif
) {
    (void) path;
    (void) name;
    (void) value;
    (void) size;
    (void) flags;
#ifndef CXNFS_FUSE_LINUX
    (void) position;
#endif
    return 0;
}

static int cxnfs_listxattr(const char *path, char *list, size_t size) {
    (void) path;
    (void) list;
    (void) size;
    return 0;
}

static int cxnfs_removexattr(const char *path, const char *name) {
    (void) path;
    (void) name;
    return 0;
}

static void destroy_state(void *private_data) {
    struct cxnfs_state *state = (struct cxnfs_state *) private_data;
    if (state == NULL) {
        return;
    }
    if (state->bridge_in >= 0) {
        write_all(state->bridge_in, "QUIT\n", 5);
        close(state->bridge_in);
    }
    if (state->bridge_out >= 0) {
        close(state->bridge_out);
    }
    if (state->bridge_pid > 0) {
        int status;
        waitpid(state->bridge_pid, &status, 0);
    }
    pthread_mutex_destroy(&state->lock);
    for (size_t index = 0; index < state->local_dir_count; index++) {
        free(state->local_dirs[index]);
    }
    free(state->local_dirs);
    for (size_t index = 0; index < state->local_file_count; index++) {
        free(state->local_files[index].path);
    }
    free(state->local_files);
    free(state->handoff_path);
    free(state->remote_root);
    free(state->mount_point);
    free(state->staging_root);
    free(state->fallback_staging_root);
    free(state);
}

static const struct fuse_operations cxnfs_operations = {
        .getattr = cxnfs_getattr,
        .access = cxnfs_access,
        .mkdir = cxnfs_mkdir,
        .unlink = cxnfs_unlink,
        .rmdir = cxnfs_rmdir,
        .rename = cxnfs_rename,
        .chmod = cxnfs_chmod,
        .chown = cxnfs_chown,
        .truncate = cxnfs_truncate,
        .utimens = cxnfs_utimens,
        .statfs = cxnfs_statfs,
        .setxattr = cxnfs_setxattr,
        .getxattr = cxnfs_getxattr,
        .listxattr = cxnfs_listxattr,
        .removexattr = cxnfs_removexattr,
        .readdir = cxnfs_readdir,
        .open = cxnfs_open,
        .create = cxnfs_create,
        .read = cxnfs_read,
        .write = cxnfs_write,
#if defined(CXNFS_FUSE_LINUX) && defined(CXNFS_FUSE_HAS_COPY_FILE_RANGE)
        .copy_file_range = cxnfs_copy_file_range,
#endif
        .flush = cxnfs_flush,
        .release = cxnfs_release,
        .ftruncate = cxnfs_ftruncate,
        .fgetattr = cxnfs_fgetattr,
        .destroy = destroy_state
};

static int run_fuse_main_with_options(
        const char *program_name,
        const char *mount_options,
        struct cxnfs_state *state
) {
    char *fuse_argv[] = {
            (char *) program_name,
            "-f",
            "-o",
            (char *) mount_options,
            state->mount_point,
            NULL
    };
    return fuse_main(5, fuse_argv, &cxnfs_operations, state);
}

static void usage(void) {
    fprintf(stderr, "Usage: " CXNFS_FUSE_PROGRAM_NAME " --handoff <cxnfs.mount.handoff.v1>\n");
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    const char *handoff = NULL;
    for (int index = 1; index < argc; index++) {
        if (strcmp(argv[index], "--handoff") == 0 && index + 1 < argc) {
            handoff = argv[++index];
        } else if (strcmp(argv[index], "--help") == 0 || strcmp(argv[index], "-h") == 0) {
            usage();
            return 0;
        } else {
            usage();
            return 64;
        }
    }
    if (handoff == NULL) {
        usage();
        return 64;
    }

    struct cxnfs_state *state = calloc(1, sizeof(*state));
    if (state == NULL) {
        return 1;
    }
    state->bridge_in = -1;
    state->bridge_out = -1;
    state->owner_uid = mount_owner_uid();
    state->owner_gid = mount_owner_gid();
    state->handoff_path = strdup(handoff);
    pthread_mutex_init(&state->lock, NULL);

    int result = read_handoff(state, handoff);
    if (result != 0) {
        destroy_state(state);
        return 1;
    }
#if CXNFS_FUSE_REQUIRE_ROOT
    if (geteuid() != 0) {
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX CXNFS_FUSE_REQUIRE_ROOT_MESSAGE "\n");
        destroy_state(state);
        return 1;
    }
#endif
    result = ensure_mount_point(state->mount_point);
    if (result != 0) {
        destroy_state(state);
        return 1;
    }
    result = initialize_staging_roots(state);
    if (result != 0) {
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "failed to initialize staging directories: %s\n", strerror(-result));
        destroy_state(state);
        return 1;
    }
    char *username = prompt_username();
    if (username == NULL) {
        destroy_state(state);
        return 1;
    }
    char *password = prompt_password();
    if (password == NULL) {
        memset(username, 0, strlen(username));
        free(username);
        destroy_state(state);
        return 1;
    }
    result = launch_bridge(state, username, password);
    memset(username, 0, strlen(username));
    memset(password, 0, strlen(password));
    free(username);
    free(password);
    if (result != 0) {
        destroy_state(state);
        return 1;
    }

    int fuse_result = run_fuse_main_with_options(argv[0], CXNFS_FUSE_MOUNT_OPTIONS, state);
    if (fuse_result != 0 && CXNFS_FUSE_FALLBACK_MOUNT_OPTIONS != NULL) {
        fprintf(stderr,
                CXNFS_FUSE_LOG_PREFIX "retrying FUSE mount without allow_other; enable user_allow_other in /etc/fuse.conf for cross-user access\n");
        fuse_result = run_fuse_main_with_options(argv[0], CXNFS_FUSE_FALLBACK_MOUNT_OPTIONS, state);
    }
    if (fuse_result != 0) {
        fprintf(stderr, CXNFS_FUSE_LOG_PREFIX "fuse_main exited with code %d\n", fuse_result);
    }
    return fuse_result;
}
