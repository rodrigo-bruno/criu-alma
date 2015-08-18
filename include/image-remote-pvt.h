#ifndef IMAGE_REMOTE_INTERNAL_H
#define	IMAGE_REMOTE_INTERNAL_H

#include <pthread.h>
#include "list.h"

#define DEFAULT_LISTEN 50
#define PATHLEN 32
#define DUMP_FINISH "DUMP_FINISH"
#define PAGESIZE 4096
#define BUF_SIZE PAGESIZE

// TODO - this may be problematic because of double evaluation...
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

/*
 * This header is used by both the image-proxy and the image-cache.
 */

typedef struct rbuf {
    char buffer[BUF_SIZE];
    int nbytes; // How many bytes are in the buffer.
    struct list_head l;
} remote_buffer;

typedef struct rimg {
    char path[PATHLEN];
    int src_fd;
    int dst_fd;
    struct list_head l;
    pthread_t putter, getter;
    struct list_head buf_head;
    
} remote_image;

#endif	/* IMAGE_REMOTE_INTERNAL_H */

