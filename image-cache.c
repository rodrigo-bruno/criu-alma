#include <unistd.h>

#include "image-remote.h"
#include "image-remote-pvt.h"
#include "criu-log.h"

void* cache_remote_image(void* ptr) 
{
        remote_image* rimg = (remote_image*) ptr;
        
        if (!strncmp(rimg->path, DUMP_FINISH, sizeof (DUMP_FINISH))) 
        {
                close(rimg->src_fd);
                return NULL;
        }
    
        prepare_put_rimg();
    
        recv_remote_image(rimg->src_fd, rimg->path, &rimg->buf_head);
        
        finalize_put_rimg(rimg);
        
        return NULL;
}

int image_cache(unsigned short cache_put_port) 
{    
        pthread_t get_thr, put_thr;
        int put_fd, get_fd;
        
        pr_info("Put Port %d, Get Port %d\n", cache_put_port, CACHE_GET_PORT);

        put_fd = prepare_server_socket(cache_put_port);
        get_fd = prepare_server_socket(CACHE_GET_PORT);

        if(init_cache()) 
                return -1;
       
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

        join_workers();
        
        // TODO - wait for ctrl-c to close every thing;
        pthread_join(put_thr, NULL);
        pthread_join(get_thr, NULL);
        // TODO - clean memory?

        return 0;
}