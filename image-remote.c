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
#include "list.h"

#include "image-remote.h"

#define DEFAULT_HOST "localhost"
#define PATHLEN 32
#define DUMP_FINISH "DUMP_FINISH"

typedef struct el {
    char path[PATHLEN];
    int sockfd;
    struct list_head l;
} remote_image;

static LIST_HEAD(rimg_head);

remote_image* get_rimg_by_fd(int fd) {
    remote_image* rimg = NULL;
    list_for_each_entry(rimg, &rimg_head, l) {
        if(rimg->sockfd == fd) {
            return rimg;
        }
    }
    return NULL;
}

void check_remote_connections() {
    int error = 0;
    socklen_t len = sizeof (error);
    remote_image *rimg;

    list_for_each_entry(rimg, &rimg_head, l) {
        pr_info("Path = %s FD = %d State %d\n", 
                rimg->path, 
                rimg->sockfd,
                getsockopt(rimg->sockfd, SOL_SOCKET, SO_ERROR, &error, &len));
    }
}

int is_remote_image(int fd) {
    remote_image *result = get_rimg_by_fd(fd);
    return result != NULL ? 1 : 0;
}

int setup_local_client_connection(int port) {
    int sockfd;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        pr_perror("Unable to open remote image socket to img cache");
        return -1;
    }

    server = gethostbyname(DEFAULT_HOST);
    if (server == NULL) {
        pr_perror("Unable to get host by name (%s)", DEFAULT_HOST);
        return -1;
    }

    bzero((char *) &serv_addr, sizeof (serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy(  (char *) server->h_addr,
            (char *) &serv_addr.sin_addr.s_addr,
            server->h_length);
    serv_addr.sin_port = htons(port);

    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof (serv_addr)) < 0) {
        pr_perror("Unable to connect to remote restore host %s", DEFAULT_HOST);
        return -1;
    }
    
    return sockfd;
}

int get_remote_image_connection(char* path) {
    int sockfd, n;
    char buf[PATHLEN];

    sockfd = setup_local_client_connection(DEFAULT_GET_PORT);
    if(sockfd < 0) {
        return -1;
    }

    if (write(sockfd, path, PATHLEN) < 1) {
        pr_perror("Unable to send path to remote image connection");
        return -1;
    }

    n = read(sockfd, buf, PATHLEN);
    if (n < 0) {
        pr_perror("Error reading from checkpoint remote image socket");
        return -1;
    } else if (n == 0) {
        pr_perror("Remote checkpoint image socket closed before receiving path");
        return -1;
    }
    
    if(!strncmp(buf, path, PATHLEN)) {
        pr_info("Image cache does have %s\n", path);
               
        remote_image* img = malloc(sizeof (remote_image));
        if (img == NULL) {
            pr_perror("Unable to allocate remote_image structures");
            return -1;
        }
        img->sockfd = sockfd;
        strncpy(img->path, path, PATHLEN);
        list_add_tail(&(img->l), &rimg_head);
        return sockfd;
    }
    else if(!strncmp(buf, DUMP_FINISH, PATHLEN)) {
        pr_info("Image cache does not have %s\n", path);
        close(sockfd);
        return -1;
    }
    else {
        pr_perror("Image cache returned erroneous name %s\n", path);
        close(sockfd);
        return -1;
    }
}

int open_remote_image_connection(char* path) {
    int sockfd;

    sockfd = setup_local_client_connection(DEFAULT_PUT_PORT);
    if(sockfd < 0) {
        return -1;
    }

    if (write(sockfd, path, PATHLEN) < 1) {
        pr_perror("Unable to send path to remote image connection");
        return -1;
    }

    return sockfd;
}

int finish_remote_dump() {
    pr_info("Dump side is calling finish\n");
    int fd = open_remote_image_connection(DUMP_FINISH);
    if (fd == -1) {
        pr_perror("Unable to open finish dump connection");
        return -1;
    }
    close(fd);
    return 0;
}

