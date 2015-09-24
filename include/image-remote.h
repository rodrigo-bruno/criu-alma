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

#define LOCAL_DEVEL 1

#define GET_PORT 9998
#define PUT_PORT 9996

#define PROXY_GET_PORT LOCAL_DEVEL ? 9995 : GET_PORT
#define PROXY_PUT_PORT PUT_PORT

#define CACHE_GET_PORT GET_PORT
#define CACHE_PUT_PORT LOCAL_DEVEL ? 9997 : PUT_PORT  // can be overwritten by main

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

/* Reads (discards) 'len' bytes from fd. This is used to emulate the function
 * lseek, which is used to advance the file needle. */
int skip_remote_bytes(int fd, unsigned long len);

/* To support iterative migration (multiple pre-dumps before the final dump
 * and subsequent restore, the concept of namespace is introduced. Each image
 * is tagged with one namespace and we build a hierarchy of namespaces to 
 * represent the dependency between pagemaps. Currently, the images dir is 
 * used as namespace when the operation is marked as remote. */

/* Sets the current namesapce and parent namespace. */
void init_namespace(char* namespace, char* parent);

/* Returns an integer (virtual fd) representing the current namespace. */
int get_current_namespace_fd();

/* Returns a pointer to the char array containing the current namesapce. */
char* get_current_namespace();

/* Returns the namespace associated with the virtual fd (given as argument). */
char* get_namespace(int dfd);

/* Pushes the current namespace into the namespace hierarchy. The hierarchy is
 * read, modified, and written. */
int push_namespace();

/* Two functions used to read and write remote images' headers.*/
int write_header(int fd, char* namespace, char* path);
int read_header(int fd, char* namespace, char* path);



#endif	/* IMAGE_REMOTE_H */