#include "image-remote.h"
#include "image-remote-pvt.h"
#include "criu-log.h"

int image_cache(unsigned short cache_port) 
{    
        pthread_t get_thr, put_thr;
        int put_fd, get_fd;
        
        pr_info("Put Port %d, Get Port %d\n", cache_port, DEFAULT_GET_PORT);

        put_fd = prepare_server_socket(cache_port);
        get_fd = prepare_server_socket(DEFAULT_GET_PORT);

        if(init_sync_structures()) 
                return -1;

        // TODO - init functions that handle image receptions and sending
        
        if (pthread_create(
            &put_thr, NULL, accept_put_image_connections, (void*) &put_fd)) {
                pr_perror("Unable to create put thread");
                return -1;
        }
        if (pthread_create(
            &get_thr, NULL, accept_get_image_connections, (void*) &get_fd)) {
                pr_perror("Unable to create get thread");
                return -1;
        }

        // TODO - wait for ctrl-c to close every thing;

        pthread_join(put_thr, NULL);
        pthread_join(get_thr, NULL);

        /* TODO - What am I gonna do?
        remote_image* rimg = NULL;
        list_for_each_entry(rimg, &rimg_head, l) {
                pthread_join(rimg->putter, NULL);
                pthread_join(rimg->getter, NULL);
                // TODO - delete from list?
        }
        */
        // TODO - clean memory?

        return 0;
}