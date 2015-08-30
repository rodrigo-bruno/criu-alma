/* 
 * File:   image-remote.h
 * Author: underscore
 *
 * Created on July 8, 2015, 1:06 AM
 */

#ifndef IMAGE_REMOTE_H
#define	IMAGE_REMOTE_H

#define DEFAULT_HOST "localhost"
#define PATHLEN 32
#define DUMP_FINISH "DUMP_FINISH"
#define PARENT_IMG "parent"
#define NULL_NAMESPACE "null"

// TODO - there should be only one GET and one PUT port.
#define PROXY_GET_PORT 9995
#define PROXY_PUT_PORT 9996
#define CACHE_PUT_PORT 9997 // can be overwritten by main
#define CACHE_GET_PORT 9998
#define PROXY_FWD_PORT CACHE_PUT_PORT
#define PROXY_FWD_HOST "localhost" // can be overwritten by main

// TODO - this may be problematic because of double evaluation...
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

/* Called by restore to get the fd correspondent to a particular path. This call
 * will block until the connection is received. */
extern int get_remote_image_connection(char* namespace, char* path);

/* Called by dump to create a socket connection to the restore side. The socket
 * fd is returned for further writing operations. */
extern int open_remote_image_connection(char* namespace, char* path );

/* Called by dump when everything is dumped. This function creates a new 
 * connection with a special control name. The recover side uses it to ack that
 * no more files are coming. */
extern int finish_remote_dump();

/* Starts an image proxy daemon (dump side). It receives image files through 
 * socket connections and forwards them to the image cache (restore side). */
extern int image_proxy(char* cache_host, unsigned short cache_port);

/* Starts an image cache daemon (restore side). It receives image files through
 * socket connections and caches them until they are requested by the restore
 * process. */
extern int image_cache(unsigned short cache_port);

// TODO - comment
int skip_remote_bytes(int fd, unsigned long len);
// TODO - check error handling for callers of these functions
void init_namespace(char* namespace, char* parent);
int get_current_namespace_fd();
char* get_namespace(int dfd);
int push_namespace();

int write_header(int fd, char* namespace, char* path);
int read_header(int fd, char* namespace, char* path);



#endif	/* IMAGE_REMOTE_H */