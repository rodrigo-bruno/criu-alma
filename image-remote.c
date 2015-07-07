/* 
 * File:   image-remote.c
 * Author: underscore
 *
 * Created on July 7, 2015, 12:46 AM
 */

#include <unistd.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include <pthread.h>

#include "criu-log.h"

#define DEFAULT_PORT 9997
#define DEFAULT_HOST "localhost"
#define DEFAULT_LISTEN 50

// int get_remote_image_connection()
// int close_remote_image_connection()

static int sockfd;

void* accept_remote_image_connections(void* null) {
    socklen_t clilen;
    int imgsockfd;
    struct sockaddr_in cli_addr;
    clilen = sizeof(cli_addr);

    while(1) {
        imgsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (imgsockfd < 0) {
            pr_perror("Unable to accept image connection");
        }
        
        // TODO - read key
        // TODO - insert into hashmap
    }
}
    

int prepare_remote_image_connections() {
    int sockopt = 1;
    
    struct sockaddr_in serv_addr;
    pthread_t sock_thr;

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
    
    // TODO - send the path name
    
    return sockfd;
}

