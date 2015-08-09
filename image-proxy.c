#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/stat.h> 
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <utlist.h>

#include <google/protobuf-c/protobuf-c.h>

#include "image-remote.h"
#include "criu-log.h"
#include "asm/types.h"
#include "protobuf.h"
#include "protobuf/pagemap.pb-c.h"

// TODO - share defines in a single header.
#define DEFAULT_LISTEN 50
#define PATHLEN 32
#define DUMP_FINISH "DUMP_FINISH"
#define PAGESIZE 4096
#define BUF_SIZE PAGESIZE
// TODO - this may be problematic because of double evaluation...
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

// GC compression means that we avoid transferring garbage data.
#define GC_COMPRESSION 1

// TODO - use CRIU's list implementation
// TODO - check when both proxy and cache daemons die.

typedef struct rbuf {
    char buffer[BUF_SIZE];
    int nbytes; // How many bytes are in the buffer.
    struct rbuf *next, *prev;
} remote_buffer;

typedef struct rimg {
    char path[PATHLEN];
    int src_fd;
    int dst_fd;
    struct rimg *next, *prev;
    pthread_t worker;
    remote_buffer* buf_head;
    
} remote_image;

static remote_image *head = NULL;
static int sockfd = -1;
static sem_t semph;
static int server_port = DEFAULT_PUT_PORT;
static char* dst_host;
static int dst_port = DEFAULT_PUT_PORT;

#if GC_COMPRESSION

#define PB_PKOBJ_LOCAL_SIZE	1024*4 // Multiplied by 4 to assure no problems.

typedef struct rpagemap {
    PagemapEntry* pentry;
    struct rpagemap *next, *prev;
} remote_pagemap;

typedef struct rmem {
    char path[PATHLEN];
    remote_image *pages, *pagemap;
    PagemapHead* pheader;
    remote_pagemap* pagemap_list;
    struct rmem *next, *prev;  
} remote_mem;

static pthread_mutex_t pages_lock;
static remote_mem *rmem_head = NULL;

int path_cmp_rmem(remote_mem *a, remote_mem *b) {
    return strcmp(a->path, b->path);
} 

int init_gc_compression() {
    if (pthread_mutex_init(&pages_lock, NULL) != 0) {
        fprintf(stderr,"GC compression mutex init failed\n");
        return -1;
    }
    return 0;
}

int rbuff_read(remote_buffer** cbuf, int* cbytes, void* buf, size_t len) {
    // Easy way
    if(*cbytes + len < BUF_SIZE) {
        memcpy(buf, (*cbuf)->buffer + *cbytes, len);
        *cbytes += len;
        return len;
    } 
    // Hard way (this works if len <= BUF_SIZE)
    else {
        size_t n = BUF_SIZE - *cbytes;
        // Copy the rest from the current buffer
        memcpy(buf, (*cbuf)->buffer + *cbytes, n);
        // Next buffer
        *cbuf = (*cbuf)->next;        
        if(*cbuf == NULL) {
            return n;
        }
        // Copy more from the next buffer
        memcpy( buf + n, (*cbuf)->buffer, len - n);
        *cbytes = len - n;
        return len;
    }
}

int rbuff_write() {
    // TODO
    return -1;
}

int pb_unpack_object(
    remote_buffer** cbuf, int* cbytes, void* buf, int eof, int type, void** pobj) {
        int ret;
        u32 size;
        
        // Read object size
	ret = rbuff_read(cbuf, cbytes, &size, sizeof(size));
	if (ret == 0) {
                if(eof) {
                    return 0;
                } else {
                        pr_err("Unexpected EOF.\n");
                        return -1;
                }		
	} else if (ret < sizeof(size)) {
		pr_perror("Read %d bytes while %d expected.", ret, size);
		return -1;
	}
	if (size > PB_PKOBJ_LOCAL_SIZE) {
		pr_err("Stack buffer is not enough for PB header\n");
		return -1;
	}

        // Read object
	ret = rbuff_read(cbuf, cbytes, buf, size);
	if (ret < 0) {
		pr_perror("Can't read %d bytes.", size);
		return -1;
	} else if (ret != size) {
		pr_perror("Read %d bytes while %d expected.", ret, size);
		return -1;
	}

	*pobj = cr_pb_descs[type].unpack(NULL, size, buf);
	if (!*pobj) {
		pr_err("Failed unpacking object %p\n",*pobj);
		return -1;
	}
        
        return 1;
}

int pb_pack_object() {
    // TODO
    return -1;
}

int pb_unpack_pagemap(remote_image* rimg, remote_mem* rmem)
{
	u8 local[PB_PKOBJ_LOCAL_SIZE];
	void *buf = (void *)&local;
	int ret;
        void* pobj = NULL;
        
        // These two will be used as reading position for remote buffers.
        remote_buffer* cbuf = rimg->buf_head;
        int cbytes = 0;

        ret = pb_unpack_object(&cbuf, &cbytes, buf, 0, PB_PAGEMAP_HEAD, &pobj);
        if (ret == -1) {
            pr_perror("Error unpacking header from %s.", rimg->path);
            return -1;
        }
        rmem->pheader = (PagemapHead*) pobj;

        while (1) {
               ret = pb_unpack_object(&cbuf, &cbytes, buf, 1, PB_PAGEMAP, &pobj);
                if (ret == -1) {
                    pr_perror("Error unpacking header from %s.", rimg->path);
                    return -1;
                } else if (ret == 0) {
                    break;
                }
               remote_pagemap* rpagemap = malloc(sizeof(remote_pagemap));
               if (!rpagemap) {
                   pr_perror("Cannot allocate memory for remote_pagemap");
                    return -1;
               }
               rpagemap->pentry = (PagemapEntry*) pobj;
                DL_APPEND(rmem->pagemap_list, rpagemap);
        }
	return 1;
        // TODO - I need to free all objects returned by the PB unpacker!
}

int pb_pack_pagemap(remote_image* rimg, remote_mem* rmem) {
    // TODO
    return -1;
}

void compress_garbage(remote_mem* rmem) {
    // TODO - iterate through pagemap and whenever a mapping (or part of it) is
    // garbage, clean it, removing the corresponding page from pages 
    // remote_image).
}

void compress_garbage_if_ready(remote_image* rimg) {
    remote_mem *result, like, *rmem;
    char pages_path[PATHLEN];
    
    strncpy(like.path, pages_path, PATHLEN);
    pthread_mutex_lock(&pages_lock);
    DL_SEARCH(rmem_head, result, &like, path_cmp_rmem);
    
    rmem = result;
    if(!result) {
        rmem = malloc( sizeof(remote_mem));
        if(rmem == NULL) {
            fprintf(stderr,"Unable to allocate remote_mem structures\n");
        }
        else {
            DL_APPEND(rmem_head, rmem);
        }        
    }
    
    if(!strncmp(rimg->path, "pagemap-", 8)) {
        if (pb_unpack_pagemap(rimg, rmem) == -1) {
            pr_perror("Error unpacking pagemap %s", rimg->path);
        }
    }
    else {
        rmem->pages = rimg;
    }
    
    if(result) {
        compress_garbage(result);
        // TODO - pack new pagemap
        // TODO - call send on pagemap and pages
    }
    pthread_mutex_unlock(&pages_lock);
}

#endif

int recv_remote_image(remote_image* rimg) {
    int n;
    int src_fd = rimg->src_fd;
    remote_buffer* curr_buf = rimg->buf_head;
    
    while(1) {
        n = read(   src_fd, 
                    curr_buf->buffer + curr_buf->nbytes, 
                    BUF_SIZE - curr_buf->nbytes);
        if (n == 0) {
            close(src_fd);
            printf("Finished receiving %s. Forwarding...\n", rimg->path);
            break;
        }
        else if (n > 0) {
            curr_buf->nbytes += n;
            if(curr_buf->nbytes == BUF_SIZE) {
                remote_buffer* buf = malloc(sizeof (remote_buffer));
                if(buf == NULL) {
                    fprintf(stderr,"Unable to allocate remote_buffer structures\n");
                    return -1;
                }
                buf->nbytes = 0;
                DL_APPEND(rimg->buf_head, buf);
                curr_buf = buf;
            }
            
        }
        else {
            fprintf(stderr,"Read on %s socket failed\n", rimg->path);
            return -1;
        }
    }
    return 0;
}

int send_remote_image(remote_image* rimg) {
    int dst_fd = rimg->dst_fd;
    remote_buffer* curr_buf = rimg->buf_head;
    int n, curr_offset = 0;
    
    while(1) {
        n = write(
                    dst_fd, 
                    curr_buf->buffer + curr_offset, 
                    MIN(BUF_SIZE, curr_buf->nbytes) - curr_offset);
        if(n > -1) {
            curr_offset += n;
            if(curr_offset == BUF_SIZE) {
                curr_buf = curr_buf->next;
                curr_offset = 0;
            }
            else if(curr_offset == curr_buf->nbytes) {
                printf("Finished forwarding %s. Done.\n", rimg->path);
                close(dst_fd);
                break;
            }
        }
        else {
             fprintf(stderr,"Write on %s socket failed (n=%d)\n", rimg->path, n);
        }
    }
    return 0;
}

void* buffer_remote_image(void* ptr) {
    remote_image* rimg = (remote_image*) ptr;
    if (!recv_remote_image(rimg)) {
        return NULL;
    }
    
#if GC_COMPRESSION
    if(!strncmp(rimg->path, "pages-", 6) || !strncmp(rimg->path, "pagemap-", 8)) {
        compress_garbage_if_ready(rimg);
        return NULL;
    }
#endif
    
    send_remote_image(rimg);
    return NULL;
}

void* accept_remote_image_connections(void* null) {
    socklen_t clilen;
    int src_fd, dst_fd, n;
    struct sockaddr_in cli_addr, serv_addr;
    clilen = sizeof (cli_addr);
    struct hostent *restore_server;

    while (1) {
        src_fd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (src_fd < 0) {
            fprintf(stderr,"Unable to accept checkpoint image connection\n");
            continue;
        }

        remote_image* img = malloc(sizeof (remote_image));
        if (img == NULL) {
            fprintf(stderr,"Unable to allocate remote_image structures\n");
            return NULL;
        }
        
        remote_buffer* buf = malloc(sizeof (remote_buffer));
        if(buf == NULL) {
            fprintf(stderr,"Unable to allocate remote_buffer structures\n");
            return NULL;
        }

        n = read(src_fd, img->path, PATHLEN);
        if (n < 0) {
            fprintf(stderr,"Error reading from checkpoint remote image socket\n");
            continue;
        } else if (n == 0) {
            fprintf(stderr,"Remote checkpoint image socket closed before receiving path\n");
            continue;
        }
        
        
        dst_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (dst_fd < 0) {
            fprintf(stderr,"Unable to open recover image socket\n");
            return NULL;
        }

        restore_server = gethostbyname(dst_host);
        if (restore_server == NULL) {
            fprintf(stderr,"Unable to get host by name (%s)\n", dst_host);
            return NULL;
        }

        bzero((char *) &serv_addr, sizeof (serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy(  (char *) restore_server->h_addr,
                (char *) &serv_addr.sin_addr.s_addr,
                restore_server->h_length);
        serv_addr.sin_port = htons(dst_port);

        n = connect(dst_fd, (struct sockaddr *) &serv_addr, sizeof (serv_addr));
        if (n < 0) {
            fprintf(stderr,"Unable to connect to remote restore host %s: %s\n", dst_host, strerror(errno));
            return NULL;
        }

        if (write(dst_fd, img->path, PATHLEN) < 1) {
            fprintf(stderr,"Unable to send path to remote image connection\n");
            return NULL;
        }
        
        if (!strncmp(img->path, DUMP_FINISH, sizeof (DUMP_FINISH))) {
            printf("Dump side is finished!\n");
            free(img);
            free(buf);
            close(src_fd);
            sem_post(&semph);
            return NULL;
        }
       
        img->src_fd = src_fd;
        img->dst_fd = dst_fd;
        img->buf_head = NULL;
        buf->nbytes = 0;
        DL_APPEND(img->buf_head, buf);
        
        if (pthread_create( &img->worker, 
                            NULL, 
                            buffer_remote_image, 
                            (void*) img)) {
                fprintf(stderr,"Unable to create socket thread\n");
                return NULL;
        } 
        
        printf("Reveiced %s, from %d to %d\n", img->path, img->src_fd, img->dst_fd);
        DL_APPEND(head, img);
    }
}

int image_proxy(char* cache_host) {
    int sockopt = 1;
    struct sockaddr_in serv_addr;
    pthread_t sock_thr;
    
    dst_host = cache_host;
    printf ("Local Port %d, Remote Host %s:%d\n", server_port, dst_host, dst_port);
    
    if (sem_init(&semph, 0, 0) != 0) {
        fprintf(stderr, "Remote image connection semaphore init failed\n");
        return -1;
    }

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        fprintf(stderr, "Unable to open image socket\n");
        return -1;
    }

    bzero((char *) &serv_addr, sizeof (serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(server_port);

    if (setsockopt(
            sockfd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &sockopt,
            sizeof (sockopt)) == -1) {
        fprintf(stderr, "Unable to set SO_REUSEADDR\n");
        return -1;
    }

    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof (serv_addr)) < 0) {
        fprintf(stderr, "Unable to bind image socket\n");
        return -1;
    }

    if (listen(sockfd, DEFAULT_LISTEN)) {
        fprintf(stderr, "Unable to listen image socket\n");
        return -1;
    }

    if (pthread_create(
            &sock_thr, NULL, accept_remote_image_connections, NULL)) {
        fprintf(stderr, "Unable to create socket thread\n");
        return -1;

    }
    
    sem_wait(&semph);
    // TODO - why not to replace this semph with a pthread_join?
    
    remote_image *elt, *tmp;
    DL_FOREACH_SAFE(head,elt,tmp) {
        pthread_join(elt->worker, NULL);
        DL_DELETE(head,elt);
    }
    
    return 0;
}
