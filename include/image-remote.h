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

#endif	/* IMAGE_REMOTE_H */

