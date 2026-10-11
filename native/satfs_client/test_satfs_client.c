#include "satfs_client.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void) {
    printf("Testing satfs_client...\n");

    /* Test creation with custom Unix path */
    satfs_client_t *client_unix = satfs_client_create_unix("/tmp/test_satfs.sock");
    assert(client_unix != NULL);
    satfs_client_close(client_unix);

    /* Test creation with TCP host/port */
    satfs_client_t *client_tcp = satfs_client_create_tcp("127.0.0.1", 7799);
    assert(client_tcp != NULL);
    satfs_client_close(client_tcp);

    /* Test null parameter handling */
    assert(satfs_client_stat(NULL, "/test", NULL) < 0);
    assert(satfs_client_read_range(NULL, "/test", 0, 100, NULL, NULL) < 0);

    printf("satfs_client unit tests passed successfully!\n");
    return 0;
}
