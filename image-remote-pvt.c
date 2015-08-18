#include <unistd.h>
#include <stdlib.h>

#include "image-remote-pvt.h"
#include "criu-log.h"

int recv_remote_image(int fd, char* path, struct list_head* rbuff_head) 
{
        remote_buffer* curr_buf = list_entry(rbuff_head->next, remote_buffer, l);
        int n, nblocks;
        time_t t;
       
        nblocks = 1;
        while(1) {
                n = read(fd, 
                         curr_buf->buffer + curr_buf->nbytes, 
                         BUF_SIZE - curr_buf->nbytes);
                if (n == 0) {
                        time(&t);
                        pr_info("Finished receiving %s (%d blocks, %d bytes on last block) %s", 
                                path, nblocks, curr_buf->nbytes, ctime(&t));
                        close(fd);
                        return nblocks*BUF_SIZE + curr_buf->nbytes;
                }
                else if (n > 0) {
                        curr_buf->nbytes += n;
                        if(curr_buf->nbytes == BUF_SIZE) {
                                remote_buffer* buf = malloc(sizeof(remote_buffer));
                                if(buf == NULL) {
                                        pr_perror("Unable to allocate remote_buffer structures");
                                        return -1;
                                }
                                buf->nbytes = 0;
                                list_add_tail(&(buf->l), rbuff_head);
                                curr_buf = buf;
                                nblocks++;
                        }            
                }
                else {
                        pr_perror("Read on %s socket failed", path);
                        return -1;
                }
        }
}

int send_remote_image(int fd, char* path, struct list_head* rbuff_head) 
{
        remote_buffer* curr_buf = list_entry(rbuff_head->next, remote_buffer, l);
        int n, curr_offset, nblocks;
    
        nblocks = 1;
        curr_offset = 0;
    
        while(1) {
                n = write(
                    fd, 
                    curr_buf->buffer + curr_offset, 
                    MIN(BUF_SIZE, curr_buf->nbytes) - curr_offset);
                if(n > -1) {
                        curr_offset += n;
                        if(curr_offset == BUF_SIZE) {
                                curr_buf = 
                                    list_entry(curr_buf->l.next, remote_buffer, l);
                                nblocks++;
                                curr_offset = 0;
                        }
                        else if(curr_offset == curr_buf->nbytes) {
                                pr_info("Finished forwarding %s (%d blocks, %d bytes on last block)\n", 
                                        path, nblocks, curr_offset);
                                close(fd);
                               return nblocks*BUF_SIZE + curr_buf->nbytes;
                        }
                }
                else {
                        pr_perror("Write on %s socket failed (n=%d)", path, n);
                        return -1;
                }
        }
}
