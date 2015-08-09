/* 
 * File:   image-remote.h
 * Author: underscore
 *
 * Created on July 8, 2015, 1:06 AM
 */

#ifndef IMAGE_REMOTE_H
#define	IMAGE_REMOTE_H

/* Port number to be used for: i) receiving image files connections to be 
 * proxied; ii) storing image files in cache. */
#define DEFAULT_PUT_PORT 9995
/* Port number to be used for retrieving image files from cache. */
#define DEFAULT_GET_PORT 9997


/* Called by restore function. Launches a threads that accepts connections for
 * remote images. */
extern int prepare_remote_image_connections();

/* Called by restore to get the fd correspondent to a particular path. This call
 * will block until the connection is received. */
extern int get_remote_image_connection(char* path);

/* Called by dump to create a socket connection to the restore side. The socket
 * fd is returned for further writing operations. */
extern int open_remote_image_connection(char* path);

/* Called by dump when everything is dumped. This function creates a new 
 * connection with a special control name. The recover side uses it to ack that
 * no more files are comming. */
extern int finish_remote_dump();

/* Dumps to pr_pinfo the state of all remote image connections. Useful for 
 * debug. */
extern void check_remote_connections();

/* Checks if the given fd is registered as a remove image connection fd. */
extern int is_remote_image(int fd);

/* Starts an image proxy daemon (dump side). It receives image files through 
 * socket connections and forwards them to the image cache (restore side). */
extern int image_proxy(char* cache_host);

/* Starts an image cache daemon (restore side). It receives image files through
 * socket connections and caches them until they are requested by the restore
 * process. */
extern int image_cache();


#endif	/* IMAGE_REMOTE_H */

