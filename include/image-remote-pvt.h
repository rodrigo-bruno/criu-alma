#ifndef IMAGE_REMOTE_INTERNAL_H
#define	IMAGE_REMOTE_INTERNAL_H

#include <pthread.h>
#include "list.h"
#include "image-remote.h"

#define DEFAULT_LISTEN 50
#define PAGESIZE 4096
#define BUF_SIZE PAGESIZE

// GC compression means that we avoid transferring garbage data.
#define GC_COMPRESSION 1

/*
 * This header is used by both the image-proxy and the image-cache.
 */

// TODO - if we want to implement shared cache and proxy, we might need to clean
// image files from memory. Otherwise we will harvest lots of memory unnecessarily.

typedef struct rbuf {
    char buffer[BUF_SIZE];
    int nbytes; // How many bytes are in the buffer.
    struct list_head l;
#if GC_COMPRESSION
    char garbage;
#endif
} remote_buffer;

typedef struct rimg {
    char path[PATHLEN];
    char namespace[PATHLEN];
    int src_fd;
    int dst_fd;
    struct list_head l;
    struct list_head buf_head;
    
} remote_image;

int init_cache();
int init_proxy();

void join_workers();
remote_image* wait_for_image(int cli_fd, char* namespace, char* path);

void prepare_put_rimg();
void finalize_put_rimg(remote_image* rimg);

void* accept_get_image_connections(void* port);
void* accept_put_image_connections(void* port);

void* cache_remote_image(void* rimg);
void* proxy_remote_image(void* rimg);

int send_remote_image(int fd, char* path, struct list_head* rbuff_head);
int recv_remote_image(int fd, char* path, struct list_head* rbuff_head);

#if GC_COMPRESSION
int send_remote_pages(int fd, char* path, struct list_head* rbuff_head);
int recv_remote_pages(int fd, char* path, struct list_head* rbuff_head);
#endif

int prepare_server_socket(int port);
int prepare_client_socket(char* server, int port);

#if GC_COMPRESSION
void* get_proxied_image(void* rimg);
#endif

#endif	/* IMAGE_REMOTE_INTERNAL_H */

