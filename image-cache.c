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
#include <time.h>

#include "image-remote.h"
#include "criu-log.h"

#define DEFAULT_LISTEN 50
#define PATHLEN 32
#define DUMP_FINISH "DUMP_FINISH"
#define PAGESIZE 4096
#define BUF_SIZE PAGESIZE
// TODO - this may be problematic because of double evaluation...
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

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
    pthread_t putter, getter;
    remote_buffer* buf_head;
    
} remote_image;

// TODO - fixme!
int path_cmp2(remote_image *a, remote_image *b) {
    return strcmp(a->path, b->path);
}

static remote_image *head = NULL;
static int get_fd = -1;
static int put_fd = -1;
static int finished = 0;
static pthread_mutex_t lock;
static sem_t semph;
static int putting = 0;

void* get_remote_image(void* ptr) {
    remote_image* rimg = (remote_image*) ptr;
    remote_buffer* curr_buf = rimg->buf_head;
    int n, curr_offset, nblocks;
    int dst_fd = rimg->dst_fd;
    
    nblocks = 1; // This is for debug only
    curr_offset = 0;
    while(1) {
        n = write(
                    dst_fd, 
                    curr_buf->buffer + curr_offset, 
                    MIN(BUF_SIZE, curr_buf->nbytes) - curr_offset);
        if(n > -1) {
            curr_offset += n;
            if(curr_offset == BUF_SIZE) {
                curr_buf = curr_buf->next;
                nblocks++;
                curr_offset = 0;
            }
            else if(curr_offset == curr_buf->nbytes) {
                pr_info("Finished forwarding %s (%d blocks, %d bytes on last block)\n", rimg->path, nblocks, curr_offset);
                close(dst_fd);
                return NULL;
            }
        }
        else {
             pr_perror("Write on %s socket failed (ret = %d)", rimg->path, n);
             return NULL;
        }
    }
}

void* put_remote_image(void* ptr) {
    remote_image* rimg = (remote_image*) ptr;
    remote_buffer* curr_buf = rimg->buf_head;
    int src_fd = rimg->src_fd;
    int n, nblocks;
    time_t t;
    
    pthread_mutex_lock(&lock);
    putting++;
    pthread_mutex_unlock(&lock);
    
    nblocks = 1;
    while(1) {
        n = read(   src_fd, 
                    curr_buf->buffer + curr_buf->nbytes, 
                    BUF_SIZE - curr_buf->nbytes);
        if (n == 0) {
            time(&t);
            // TODO - remove timestamp
            pr_info("Finished receiving %s (%d blocks, %d bytes on last block) %s\n", rimg->path, nblocks, rimg->buf_head->prev->nbytes, ctime(&t));
            close(src_fd);
            pthread_mutex_lock(&lock);
            DL_APPEND(head, rimg);
            putting--;
            pthread_mutex_unlock(&lock);
            sem_post(&semph);
            return NULL;
        }
        else if (n > 0) {
            curr_buf->nbytes += n;
            if(curr_buf->nbytes == BUF_SIZE) {
                remote_buffer* buf = malloc(sizeof (remote_buffer));
                if(buf == NULL) {
                    pr_perror("Unable to allocate remote_buffer structures");
                }
                buf->nbytes = 0;
                DL_APPEND(rimg->buf_head, buf);
                curr_buf = buf;
                nblocks++;
            }
            
        }
        else {
            pr_perror("Read on %s socket failed", rimg->path);
            return NULL;
        }
    }
    
}

remote_image* wait_for_image(int cli_fd, const char* path) {
    remote_image *result, like;
    
    strncpy(like.path, path, PATHLEN);
    while (1) {
        pthread_mutex_lock(&lock);
        DL_SEARCH(head, result, &like, path_cmp2);
        pthread_mutex_unlock(&lock);
        if (result != NULL) {
            if (write(cli_fd, path, PATHLEN) < 1) {
                pr_perror("Unable to send ack to get image connection");
                close(cli_fd);
                return NULL;
            }
            return result;
        }
        if (finished && !putting) {
            if (write(cli_fd, DUMP_FINISH, PATHLEN) < 1) {
                pr_perror("Unable to send nack to get image connection");
            }
            close(cli_fd);
            return NULL;
        }
        sem_wait(&semph);
    }
}

void* accept_get_image_connections(void* null) {
    socklen_t clilen;
    int n, cli_fd;
    struct sockaddr_in cli_addr;
    clilen = sizeof (cli_addr);
    char path_buf[PATHLEN];
    remote_image* img;

    while (1) {
        
        cli_fd = accept(get_fd, (struct sockaddr *) &cli_addr, &clilen);
        if (cli_fd < 0) {
            pr_perror("Unable to accept get image connection");
            continue;
        }
        
        n = read(cli_fd, path_buf, PATHLEN);
        if (n < 0) {
            pr_perror("Error reading from checkpoint remote image socket");
            continue;
        } else if (n == 0) {
            pr_perror("Remote checkpoint image socket closed before receiving path");
            continue;
        }
        
        pr_info("Received GET for %s.\n", path_buf);
        
        img = wait_for_image(cli_fd, path_buf);
        if(!img) {
            continue;
        }
        
        img->dst_fd = cli_fd;
        
        if (pthread_create( &img->getter, 
                            NULL, 
                            get_remote_image, 
                            (void*) img)) {
            pr_perror("Unable to create put thread");
            return NULL;
        } 
    }
}

void* accept_put_image_connections(void* null) {
    socklen_t clilen;
    int n, cli_fd;
    struct sockaddr_in cli_addr;
    clilen = sizeof (cli_addr);
    char path_buf[PATHLEN];
    time_t t;
    
    while (1) {
        
        cli_fd = accept(put_fd, (struct sockaddr *) &cli_addr, &clilen);
        if (cli_fd < 0) {
            pr_perror("Unable to accept put image connection");
            continue;
        }
        
        n = read(cli_fd, path_buf, PATHLEN);
        if (n < 0) {
            pr_perror("Error reading from checkpoint remote image socket");
            continue;
        } else if (n == 0) {
            pr_perror("Remote checkpoint image socket closed before receiving path");
            continue;
        }
        
        if (!strncmp(path_buf, DUMP_FINISH, sizeof (DUMP_FINISH))) {
            close(cli_fd);
            close(put_fd);
            finished = 1;
            sem_post(&semph);
            return NULL;
        }
        
        remote_image* img = malloc(sizeof (remote_image));
        if (img == NULL) {
            pr_perror("Unable to allocate remote_image structures");
            return NULL;
        }
        
        remote_buffer* buf = malloc(sizeof (remote_buffer));
        if(buf == NULL) {
            pr_perror("Unable to allocate remote_buffer structures");
            return NULL;
        }
        
        strncpy(img->path, path_buf, PATHLEN);
        img->src_fd = cli_fd;
        img->dst_fd = -1;
        img->buf_head = NULL;
        buf->nbytes = 0;
        DL_APPEND(img->buf_head, buf);
        
        if (pthread_create( &img->putter, 
                            NULL, 
                            put_remote_image, 
                            (void*) img)) {
            pr_perror("Unable to create put thread");
            return NULL;
        } 
        time(&t);
        pr_info("Reveiced PUT request for %s. %s\n", img->path, ctime(&t));
    }
}

int prepare_server_socket(int port) {
    struct sockaddr_in serv_addr;
    int sockopt = 1;
    
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        pr_perror("Unable to open image socket");
        return -1;
    }

    bzero((char *) &serv_addr, sizeof (serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);

    if (setsockopt(
            sockfd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &sockopt,
            sizeof (sockopt)) == -1) {
        pr_perror("Unable to set SO_REUSEADDR");
        return -1;
    }

    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof (serv_addr)) < 0) {
        pr_perror("Unable to bind image socket");
        return -1;
    }

    if (listen(sockfd, DEFAULT_LISTEN)) {
        pr_perror("Unable to listen image socket");
        return -1;
    }
    
    return sockfd;
}

int image_cache(unsigned short cache_port) {
    
    pthread_t get_thr, put_thr;
    int put_port, get_port;
    
    put_port = cache_port;
    get_port = DEFAULT_GET_PORT;
    pr_info("Put Port %d, Get Port %d\n", put_port, get_port);

    put_fd = prepare_server_socket(put_port);
    get_fd = prepare_server_socket(get_port);
    
    if (pthread_mutex_init(&lock, NULL) != 0) {
        pr_perror("Remote image connection mutex init failed");
        return -1;
    }

    if (sem_init(&semph, 0, 0) != 0) {
        pr_perror("Remote image connection semaphore init failed");
        return -1;
    }
    
    if (pthread_create(
            &put_thr, NULL, accept_put_image_connections, NULL)) {
        pr_perror("Unable to create put thread");
        return -1;
    }
    if (pthread_create(
            &get_thr, NULL, accept_get_image_connections, NULL)) {
        pr_perror("Unable to create get thread");
        return -1;
    }
    
    // TODO - wait for ctrl-c to close every thing;
    
    pthread_join(put_thr, NULL);
    pthread_join(get_thr, NULL);
    
    remote_image *elt, *tmp;
    DL_FOREACH_SAFE(head,elt,tmp) {
        pthread_join(elt->putter, NULL);
        pthread_join(elt->getter, NULL);
        DL_DELETE(head,elt);
    }
    
    return 0;
}
