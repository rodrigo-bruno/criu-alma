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
#include "utlist.h"

#define DEFAULT_PORT 9997
#define DEFAULT_HOST "localhost"
#define DEFAULT_LISTEN 50
#define PATHLEN 32

typedef struct el {
    char path[PATHLEN];
    int sockfd;
    struct el *next, *prev;
} remote_image;

int path_cmp(remote_image *a, remote_image *b) {
    return strcmp(a->path,b->path);
}

static remote_image *head = NULL;
static int sockfd = -1;
static pthread_mutex_t lock;
static sem_t semph;

// TODO
// int close_remote_image_connection()

void* accept_remote_image_connections(void* null) {
    socklen_t clilen;
    int imgsockfd, n;
    struct sockaddr_in cli_addr;
    clilen = sizeof(cli_addr);

    while(1) {
        imgsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (imgsockfd < 0) {
            pr_perror("Unable to accept image connection");
        }
        
        remote_image* img = malloc(sizeof(remote_image));
        if(img == NULL) {
            pr_perror("Unable to allocate remote_image structures");
        }
        
        n = read(imgsockfd,img->path, PATHLEN);
        if (n < 0) {
            pr_perror("Error reading from remote image socket");
        }
        else if (n == 0) {
            pr_perror("Remote image socket closed before receiving path");
        }
        img->sockfd = imgsockfd;
        
        pthread_mutex_lock(&lock);
        DL_APPEND(head, img);
        pthread_mutex_unlock(&lock);
        sem_post(&semph);
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
    
    if(sem_init(&semph, 0, 0) != 0) {
        pr_perror("Remote image connection semaphore init failed");
        return -1;
    }
    
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        pr_perror("Unable to open image socket");
        return -1;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(DEFAULT_PORT);

    if (setsockopt(
            sockfd, 
            SOL_SOCKET, 
            SO_REUSEADDR, 
            &sockopt, 
            sizeof(sockopt)) == -1) {
        pr_perror("Unable to set SO_REUSEADDR");
        return -1;
    }

    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) { 
        pr_perror("Unable to bind image socket");
        return -1;
    }

    if(listen(sockfd,DEFAULT_LISTEN)) {
        pr_perror("Unable to listen image socket");
        return -1;
    }
    
    if(pthread_create(
            &sock_thr, NULL, accept_remote_image_connections, NULL)) {
        pr_perror("Unable to create socket thread");
        return -1;

    }
    return 0;
    
}

int get_remote_image_connection(char* path) {
    remote_image *result, like;
    int sockfd;
    
    strncpy(like.path, path, PATHLEN);
    
    while(1) {
        DL_SEARCH(head,result,&like,path_cmp);
        if(result != NULL) {
            break;
        }
        pr_perror("Remote image connection not found. Waiting...");
        sem_wait(&semph);
    }
    
    sockfd = result->sockfd;
    pthread_mutex_lock(&lock);
    DL_DELETE(head, result);
    pthread_mutex_unlock(&lock);
    free(result);
    
    return sockfd;
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
    
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, 
          (char *)&serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(DEFAULT_PORT);
    
    if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) {
        pr_perror("Unable to connect to remote restore host %s", DEFAULT_HOST);
        return -1;
    }
    
    if (write(sockfd, path, PATHLEN) < 1) {
        pr_perror("Unable to send path to remote image connection");
        return -1;
    }
    
    return sockfd;
}

