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
#include <semaphore.h>
#include <time.h>

#include "image-remote.h"
#include "image-remote-pvt.h"
#include "criu-log.h"


static LIST_HEAD(rimg_head);
static int get_fd = -1;
static int put_fd = -1;
static int finished = 0;
static pthread_mutex_t lock;
static sem_t semph;
static int putting = 0;

static remote_image* get_rimg_by_path(const char* path) {
    remote_image* rimg = NULL;
    list_for_each_entry(rimg, &rimg_head, l) {
        if(!strncmp(rimg->path, path, PATHLEN)) {
            return rimg;
        }
    }
    return NULL;
}

void* get_remote_image(void* ptr) {
    remote_image* rimg = (remote_image*) ptr;
    send_remote_image(rimg->dst_fd, rimg->path, &rimg->buf_head);
    return NULL;
}


void* put_remote_image(void* ptr) {
    remote_image* rimg = (remote_image*) ptr;
    
    pthread_mutex_lock(&lock);
    putting++;
    pthread_mutex_unlock(&lock);    
    
    recv_remote_image(rimg->src_fd, rimg->path, &rimg->buf_head);
    
    pthread_mutex_lock(&lock);
    list_add_tail(&(rimg->l), &rimg_head);
    putting--;
    pthread_mutex_unlock(&lock);
    sem_post(&semph);
    return NULL;
}

remote_image* wait_for_image(int cli_fd, const char* path) {
    remote_image *result;
    
    while (1) {
        pthread_mutex_lock(&lock);
        result = get_rimg_by_path(path);
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
        
        remote_image* rimg = malloc(sizeof (remote_image));
        if (rimg == NULL) {
            pr_perror("Unable to allocate remote_image structures");
            return NULL;
        }
        
        remote_buffer* buf = malloc(sizeof (remote_buffer));
        if(buf == NULL) {
            pr_perror("Unable to allocate remote_buffer structures");
            return NULL;
        }
        
        strncpy(rimg->path, path_buf, PATHLEN);
        rimg->src_fd = cli_fd;
        rimg->dst_fd = -1;
        INIT_LIST_HEAD(&(rimg->buf_head));
        buf->nbytes = 0;
        INIT_LIST_HEAD(&(rimg->buf_head));
        list_add_tail(&(buf->l), &(rimg->buf_head));
        
        if (pthread_create( &rimg->putter, 
                            NULL, 
                            put_remote_image, 
                            (void*) rimg)) {
            pr_perror("Unable to create put thread");
            return NULL;
        } 
        time(&t);
        pr_info("Reveiced PUT request for %s. %s\n", rimg->path, ctime(&t));
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
    
    remote_image* rimg = NULL;
    list_for_each_entry(rimg, &rimg_head, l) {
        pthread_join(rimg->putter, NULL);
        pthread_join(rimg->getter, NULL);
        // TODO - delete from list?
    }
    
    // TODO - clean memory?
    
    return 0;
}
