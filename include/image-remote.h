/* 
 * File:   image-remote.h
 * Author: underscore
 *
 * Created on July 8, 2015, 1:06 AM
 */

#ifndef IMAGE_REMOTE_H
#define	IMAGE_REMOTE_H

/* Called by restore function. Launches a threads that accepts connections for
   remote images. */
extern int prepare_remote_image_connections();

/* Called by restore to get the fd correspondent to a particular path. This call
   will block until the connection is received. */
extern int get_remote_image_connection(char* path);

/* Called by dump to create a socket connection to the restore side. The socket
   fd is returned for further writing operations. */
extern int open_remote_image_connection(char* path);

/* Called by dump when everything is dumped. This function creates a new 
 * connection with a special control name. The recover side uses it to ack that
 *  no more files are comming. */
extern int finish_remote_dump();

#endif	/* IMAGE_REMOTE_H */

