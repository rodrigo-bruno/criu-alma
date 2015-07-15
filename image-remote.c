/* 
 * File:   image-remote.c
 * Author: underscore
 *
 * Created on July 7, 2015, 12:46 AM
 */

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include <pthread.h>
#include <semaphore.h>

#include "criu-log.h"
#include <utlist.h>

#define DEFAULT_PORT 9997
#define DEFAULT_HOST "localhost"
#define DEFAULT_LISTEN 50
#define PATHLEN 32
#define DUMP_FINISH "DUMP_FINISH"
#define PIPE_READ 0
#define PIPE_WRITE 1
#define PAGESIZE 4096
#define BUF_SIZE PAGESIZE*250

// TODO - this may be problematic because of double evaluation...
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

// http://pubs.opengroup.org/onlinepubs/9699919799/functions/fmemopen.html
// http://pubs.opengroup.org/onlinepubs/9699919799/functions/open_memstream.html

// TODO - maybe it would be a good idea to perform some cleanups at the end (in
// both sides).

typedef struct rbuf {
    char buffer[BUF_SIZE];
    int nbytes; // How many bytes are in the buffer.
    struct rbuf *next, *prev;
} remote_buffer;

typedef struct rimg {
    char path[PATHLEN];
    int sockfd;
    int pipe[2]; // pipe[0] is RDONLY, pipe[1] is WRONLY
    struct rimg *next, *prev;
    pthread_t worker;
    remote_buffer* buf_head;
    
} remote_image;

int path_cmp(remote_image *a, remote_image *b) {
    return strcmp(a->path, b->path);
}

int fd_cmp(remote_image *a, remote_image *b) {
    return  a->sockfd == b->sockfd || 
            a->pipe[PIPE_READ] == b->pipe[PIPE_READ] || 
            a->pipe[PIPE_WRITE] == b->pipe[PIPE_WRITE];
}

static remote_image *head = NULL;
static int sockfd = -1;
static pthread_mutex_t lock;
static sem_t semph;
static int finished = 0;

void check_remote_connections() {
    int error = 0;
    socklen_t len = sizeof (error);
    remote_image *s;

    pthread_mutex_lock(&lock);
    for(s = head; s != NULL; s = s->next) {
        pr_info("Path = %s FD = %d State %d\n", 
                s->path, 
                s->sockfd,
                getsockopt(s->sockfd, SOL_SOCKET, SO_ERROR, &error, &len));
    }
    pthread_mutex_unlock(&lock);
}

int is_remote_image(int fd) {
    remote_image *result, like;

    like.sockfd = fd;
    like.pipe[PIPE_READ] = fd;
    like.pipe[PIPE_WRITE] = fd;

    pthread_mutex_lock(&lock);
    DL_SEARCH(head, result, &like, fd_cmp);
    pthread_mutex_unlock(&lock);
    
    if (result != NULL) {
        return 1;
    }
    
    return 0;
}

void* buffer_remote_image(remote_image* rimg, int src_fd, int dst_fd) {
    int n;
    remote_buffer* curr_buf = rimg->buf_head;
    int curr_offset = 0;
    
    while(1) {
        n = read(   src_fd, 
                    curr_buf->buffer + curr_offset, 
                    BUF_SIZE - curr_offset);
        if (n == 0) {
            close(src_fd);
            break;
        }
        else if (n > 0) {
            curr_offset += n;
            curr_buf->nbytes += n;
            if(curr_offset == BUF_SIZE) {
                remote_buffer* buf = malloc(sizeof (remote_buffer));
                if(buf == NULL) {
                    pr_perror("Unable to allocate remote_buffer structures");
                }
                buf->nbytes = 0;
                DL_APPEND(rimg->buf_head, buf);
                curr_offset = 0;
                curr_buf = buf;
                // TODO - make sure the list is appending at the end!
            }
            
        }
        else {
            pr_perror("Read on %s socket failed", rimg->path);
            return NULL;
        }
    }
    
    curr_buf = rimg->buf_head;
    curr_offset = 0;
    while(1) {
        if(!curr_buf) {
            break;
            close(dst_fd);
        }
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
        }
        else {
             pr_perror("Write on %s socket failed (n=%d)", rimg->path, n);
        }
    }
    return NULL;
}

/* Dump side:
 *  read pipe, write to buffer
 *  when pipe is closed, send buffer to socket
 */
void* buffer_to_remote_image(void* ptr) {
    remote_image* rimg = (remote_image*) ptr;
    return buffer_remote_image(rimg, rimg->pipe[PIPE_READ], rimg->sockfd);;
    
}

/* Restore side 
 *  read socket, write to buffer
 *  when socket is closed, write to pipe
 */
void* buffer_from_remote_image(void* ptr){
    remote_image* rimg = (remote_image*) ptr;
    return buffer_remote_image(rimg, rimg->sockfd, rimg->pipe[PIPE_WRITE]);
}

void* accept_remote_image_connections(void* null) {
    socklen_t clilen;
    int imgsockfd, n;
    struct sockaddr_in cli_addr;
    clilen = sizeof (cli_addr);
    

    while (1) {
        imgsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (imgsockfd < 0) {
            pr_perror("Unable to accept image connection");
        }

        remote_image* img = malloc(sizeof (remote_image));
        if (img == NULL) {
            pr_perror("Unable to allocate remote_image structures");
        }
        
        remote_buffer* buf = malloc(sizeof (remote_buffer));
        if(buf == NULL) {
            pr_perror("Unable to allocate remote_buffer structures");
        }

        n = read(imgsockfd, img->path, PATHLEN);
        if (n < 0) {
            pr_perror("Error reading from remote image socket");
        } else if (n == 0) {
            pr_perror("Remote image socket closed before receiving path");
        }

        if (!strncmp(img->path, DUMP_FINISH, sizeof (DUMP_FINISH))) {
            pr_info("Dump side is finished!\n");
            free(img);
            free(buf);
            finished = 1;
            close(imgsockfd);
            return NULL;
        }

        if(pipe(img->pipe)) {
            pr_perror("Cannot create pipe from buffer remote image");
        }
        
        img->sockfd = imgsockfd;
        img->buf_head = NULL;
        buf->nbytes = 0;
        DL_APPEND(img->buf_head, buf);
        
        if (pthread_create( &img->worker, 
                            NULL, 
                            buffer_from_remote_image, 
                            (void*) img)) {
                pr_perror("Unable to create socket thread");
                return NULL;
        } 
        
        pr_info("Reveiced %s, fd = %d\n", img->path, img->sockfd);

        pthread_mutex_lock(&lock);
        DL_APPEND(head, img);
        pthread_mutex_unlock(&lock);
        sem_post(&semph);
        
        // <underscore> DEBUG
        check_remote_connections();
    }
}

int prepare_remote_image_connections() {
    int sockopt = 1;

    struct sockaddr_in serv_addr;
    pthread_t sock_thr;

    if (pthread_mutex_init(&lock, NULL) != 0) {
        pr_perror("Remote image connection mutex init failed");
        return -1;
    }

    if (sem_init(&semph, 0, 0) != 0) {
        pr_perror("Remote image connection semaphore init failed");
        return -1;
    }

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        pr_perror("Unable to open image socket");
        return -1;
    }

    bzero((char *) &serv_addr, sizeof (serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(DEFAULT_PORT);

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

    if (pthread_create(
            &sock_thr, NULL, accept_remote_image_connections, NULL)) {
        pr_perror("Unable to create socket thread");
        return -1;

    }
    return 0;

}

int get_remote_image_connection(char* path) {
    remote_image *result, like;

    strncpy(like.path, path, PATHLEN);

    while (1) {
        pthread_mutex_lock(&lock);
        DL_SEARCH(head, result, &like, path_cmp);
        pthread_mutex_unlock(&lock);
        if (result != NULL) {
            break;
        }

        if (finished) {
            return -1;
        }

        pr_perror("Remote image connection not found (%s). Waiting...", path);
        sem_wait(&semph);
    }

    return result->pipe[PIPE_READ];
}

int open_remote_image_connection(char* path) {
    int sockfd;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        pr_perror("Unable to open remote image socket");
        return -1;
    }

    server = gethostbyname(DEFAULT_HOST);
    if (server == NULL) {
        pr_perror("Unable to get host by name (%s)", DEFAULT_HOST);
        return -1;
    }

    bzero((char *) &serv_addr, sizeof (serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
            (char *) &serv_addr.sin_addr.s_addr,
            server->h_length);
    serv_addr.sin_port = htons(DEFAULT_PORT);

    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof (serv_addr)) < 0) {
        pr_perror("Unable to connect to remote restore host %s", DEFAULT_HOST);
        return -1;
    }
   
    if (write(sockfd, path, PATHLEN) < 1) {
        pr_perror("Unable to send path to remote image connection");
        return -1;
    }

    remote_image* img = malloc(sizeof (remote_image));
    if (img == NULL) {
        pr_perror("Unable to allocate remote_image structures");
    }

    remote_buffer* buf = malloc(sizeof (remote_buffer));
    if(buf == NULL) {
        pr_perror("Unable to allocate remote_buffer structures");
    }
    
    if(pipe(img->pipe)) {
        pr_perror("Cannot create pipe from buffer remote image");
    }

    strncpy(img->path, path, PATHLEN);
    img->sockfd = sockfd;
    img->buf_head = NULL;
    buf->nbytes = 0;
    DL_APPEND(img->buf_head, buf);

    if (pthread_create( &img->worker, 
                        NULL, 
                        buffer_to_remote_image, 
                        (void*) img)) {
            pr_perror("Unable to create socket thread");
            return -1;
    } 

    DL_APPEND(head, img);
    
    return img->pipe[PIPE_WRITE];
}

int finish_remote_dump() {
    pr_info("Dump side is calling finish\n");
    int fd = open_remote_image_connection(DUMP_FINISH);
    if (fd == -1) {
        pr_perror("Unable to open finish dump connection");
        return -1;
    }
    // <underscore> TODO - uncomment this.
    //close(fd);
    return 0;
}

