#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/stat.h> 
#include <fcntl.h>
#include <semaphore.h>

#include <google/protobuf-c/protobuf-c.h>

#include "image-remote.h"
#include "image-remote-pvt.h"
#include "criu-log.h"
#include "asm/types.h"
#include "protobuf.h"
#include "protobuf/pagemap.pb-c.h"
#include "image-desc.h"

static char* dst_host;
static unsigned short dst_port = CACHE_PUT_PORT;

#if GC_COMPRESSION

#define GC_SOCK_PORT 9991
#define PB_PKOBJ_LOCAL_SIZE	1024*4 // Multiplied by 4 to assure no problems.

typedef struct rpagemap {
        PagemapEntry* pentry;
        struct list_head l;
} remote_pagemap;

typedef struct rgarbage {
        uint64_t start, finish;
        struct list_head l;
} remote_garbage;

typedef struct rmem {
        char path[PATHLEN];
        remote_image *pages, *pagemap;
        PagemapHead* pheader;
        struct list_head pagemap_head;   
        u32 pagemap_magic_a;
        u32 pagemap_magic_b;
        struct list_head l;
        sem_t* pages_cached;
} remote_mem;

static pthread_mutex_t pages_lock;
static LIST_HEAD(rmem_head);
static LIST_HEAD(garbage_head);

// TODO - check if this needs namespace
static remote_mem* get_rmem_by_path(char* path) 
{
        remote_mem* rmem = NULL;
        list_for_each_entry(rmem, &rmem_head, l) {
                if(!strncmp(rmem->path, path, PATHLEN)) {
                        return rmem;
                }
        }
        return NULL;
}

// TODO - check if this needs namespace
static remote_mem* get_rmem_by_rimg(remote_image* rimg) 
{
        remote_mem* rmem = NULL;
        list_for_each_entry(rmem, &rmem_head, l) {
                if(!strncmp(rmem->pagemap->path, rimg->path, PATHLEN)) {
                        return rmem;
                }
        }
        return NULL;
}

int pb_unpack_object(int fd, int eof, int type, void** pobj) 
{
        u8 local[PB_PKOBJ_LOCAL_SIZE];
	void *buf = (void *)&local;
        int ret;
        int nbytes = 0;
        u32 size;
        
        // Read object size
	ret = read(fd, &size, sizeof(size));
	if (ret == 0) {
                if(eof) {
                        return ret;
                } else {
                        pr_perror("Unexpected EOF.");
                        return -1;
                }		
	} else if (ret < sizeof(size)) {
		pr_perror("Read %d bytes while %d expected.", ret, size);
		return -1;
	}
	if (size > PB_PKOBJ_LOCAL_SIZE) {
		pr_perror("Stack buffer is not enough for PB header (%u bytes)",
                           size);
		return -1;
	}
        nbytes = ret;

        // Read object
	ret = read(fd, buf, size);
	if (ret < 0) {
		pr_perror("Can't read %d bytes.", size);
		return -1;
	} else if (ret != size) {
		pr_perror("Read %d bytes while %d expected.", ret, size);
		return -1;
	}
        nbytes += ret;

	*pobj = cr_pb_descs[type].unpack(NULL, size, buf);
	if (!*pobj) {
		pr_perror("Failed unpacking object %p",*pobj);
		return -1;
	}
        
        return nbytes;
}

int pb_pack_object(int fd, int type, void* obj) 
{
        u8 local[PB_PKOBJ_LOCAL_SIZE];
        void *buf = (void *)&local;
        u32 size, packed;
        int ret = -1;
        int nbytes = 0;

        size = cr_pb_descs[type].getpksize(obj);
        if (size > PB_PKOBJ_LOCAL_SIZE) {
		pr_perror("Stack buffer is not enough for PB header (%u bytes)",
                           size);
		return -1;
        }

        packed = cr_pb_descs[type].pack(obj, buf);
        if (packed != size) {
                pr_perror("Failed packing PB object %p", obj);
                return -1;
        }
        ret = write(fd, &size, sizeof(size));
        if(ret != sizeof(size)) {
		pr_perror("Could not write %zu bytes (obj size)", sizeof(size));
		return -1;
        }
        nbytes = ret;
        
        ret = write(fd, buf, size);
        if (ret != size) {
		pr_perror("Could not write %u bytes (obj)", size);
		return -1;
        }
        
        nbytes += ret;
        return nbytes;
}

// NOTE: I assume the double magic way (check image.c img_check_magic).
int rimg_read_magic(int fd, remote_mem* rmem) 
{
        if (read(fd, &(rmem->pagemap_magic_a), sizeof(u32)) != sizeof(u32)) {
                pr_perror("Could not read magic.");
                return -1;
        }
        if (read(fd, &(rmem->pagemap_magic_b), sizeof(u32)) != sizeof(u32)) {
                pr_perror("Could not read magic.");
                return -1;
        }

        return sizeof(rmem->pagemap_magic_a) + sizeof(rmem->pagemap_magic_b);
}
// NOTE: I assume the double magic way (check image.c img_check_magic).
int rimg_write_magic(int fd, remote_mem* rmem) 
{
        if(write(fd, &(rmem->pagemap_magic_a), sizeof(u32)) != sizeof(u32)) {
                pr_perror("Could not write magic.");
                return -1;
        }
        if(write(fd, &(rmem->pagemap_magic_b), sizeof(u32)) != sizeof(u32)) {
                pr_perror("Could not write magic.");
                return -1;
        }
        return sizeof(rmem->pagemap_magic_a) + sizeof(rmem->pagemap_magic_b);
}

int unpack_pagemap(remote_image* rimg, remote_mem* rmem)
{
	int ret;
        void* pobj = NULL;
        int nbytes = 0;
               
        if(rimg_read_magic(rimg->src_fd, rmem) == -1) {
            pr_perror("Magic could not be verified for %s", rimg->path);
            return -1;
        }

        ret = pb_unpack_object(rimg->src_fd, 0, PB_PAGEMAP_HEAD, &pobj);
        if (ret < 0) {
            pr_perror("Error unpacking header from %s.", rimg->path);
            return -1;
        }
        nbytes = ret;
        rmem->pheader = (PagemapHead*) pobj;
        sprintf(rmem->path, "pages-%d.img", rmem->pheader->pages_id);

        while (1) {
               ret = pb_unpack_object(rimg->src_fd, 1, PB_PAGEMAP, &pobj);
                if (ret < 0) {
                        pr_perror("Error unpacking header from %s.", rimg->path);
                        return -1;
                } else if (ret == 0) {
                        close(rimg->src_fd);
                        pr_info("Unpacking done for %s.\n", rimg->path);
                        return nbytes;
                }
                nbytes += ret;
                remote_pagemap* rpagemap = malloc(sizeof(remote_pagemap));
                if (!rpagemap) {
                        pr_perror("Cannot allocate memory for remote_pagemap");
                        return -1;
                }
                rpagemap->pentry = (PagemapEntry*) pobj;
                pr_info("Pagemap entry -> pages = %u, vaddr = %p\n", 
                        rpagemap->pentry->nr_pages, 
                        decode_pointer(rpagemap->pentry->vaddr));
                list_add_tail(&(rpagemap->l), &(rmem->pagemap_head));
        }
}

int pack_pagemap(remote_image* rimg, remote_mem* rmem) 
{
    	int ret;
        remote_pagemap* rpmap = NULL;
        int nbytes = 0;
               
        if(rimg_write_magic(rimg->dst_fd, rmem) < 0) {
                pr_perror("Could not write magic for %s", rmem->path);
                return -1;
        }

        ret = pb_pack_object(rimg->dst_fd, PB_PAGEMAP_HEAD, rmem->pheader);
        if(ret < 0) {
                pr_perror("Error packing header from %s.", rmem->path);
                return -1;
        }
        nbytes = ret;
        
        list_for_each_entry(rpmap, &(rmem->pagemap_head), l) {
                ret = pb_pack_object(rimg->dst_fd, PB_PAGEMAP, rpmap->pentry);
                if(ret < 0) {
                        pr_perror("Error packing pagemap from %s.", rmem->path);
                        return -1;
                }
                nbytes += ret;
        }
        close(rimg->dst_fd);
        pr_info("Packing done for %s.\n", rimg->path);
        return nbytes;
}

// Assumption, no garbage space crosses a mapping space
// Assumption, garbage spaces are ordered
int compress_garbage(remote_mem* rmem) // TODO - put here garbage head
{
        remote_garbage* rgarbage = list_entry(garbage_head.next, remote_garbage, l);
        remote_pagemap* rpagemap = NULL;
        remote_buffer* rpage = list_entry(rmem->pages->buf_head.next, remote_buffer, l);
        uint64_t gstart, gend, pmstart, pmend, pstart, pend;
        int counter = 0;

        // TODO - put logging and check for unexpected scenarios.
        list_for_each_entry(rpagemap, &(rmem->pagemap_head), l) {
                gstart = rgarbage->start;
                gend = rgarbage->finish;
                pmstart = rpagemap->pentry->vaddr;
                pmend = pmstart + rpagemap->pentry->nr_pages * PAGESIZE;
                pstart = pmstart;
                pend = pstart + PAGESIZE;
                
                // DELETE? pr_info("pmstart = %p pmend = %p gstart = %p gend = %p\n", 
                // DELETE?         decode_pointer(pmstart), decode_pointer(pmend),
                // DELETE?         decode_pointer(gstart), decode_pointer(gend));
                
                // This pagemap does not contain garbage
                if(gstart > pmend) {
                        // DELETE? pr_info("Pagemap with no garbage.\n");
                        continue;
                }
                
                // Pagemaps might jump over garbage spaces.
                while(gend < pmstart) {
                        // DELETE? pr_info("Garbage pages are not mapped. Advancing\n");
                        if(list_is_last(&(rgarbage->l), &garbage_head)) {
                                // DELETE? pr_info("no more garbage spaces\n");
                                return 1;
                        }
                        rgarbage = list_entry(rgarbage->l.next, remote_garbage, l);
                        gstart = rgarbage->start;
                        gend = rgarbage->finish;
                        // DELETE? pr_info("pmstart = %p pmend = %p gstart = %p gend = %p\n", 
                        // DELETE?      decode_pointer(pmstart), decode_pointer(pmend),
                        // DELETE?      decode_pointer(gstart), decode_pointer(gend));
                }
                
                while(1) {
                        
                        // This means that we finished the pages of the current pagemap
                        if(pstart >= pmend) {
                                break;
                        }
                        // DELETE? pr_info("pstart = %p pend = %p gstart = %p gend = %p\n", 
                        // DELETE?     decode_pointer(pstart), decode_pointer(pend),
                        // DELETE?      decode_pointer(gstart), decode_pointer(gend));
                        // The page is garbage
                        if(pstart >= gstart && pend <= gend) {
                                rpage->garbage = 1;
                                pr_info("%d garbage pages found - pstart=%p pend=%p gstart=%p gend=%p\n", 
                                        ++counter,
                                        decode_pointer(pstart), decode_pointer(pend),
                                        decode_pointer(gstart), decode_pointer(gend));
                        }
                        // The current garbage space ends here
                        if(gend <= pend) {
                                // Return if all gc spaces were inspected.
                                if(list_is_last(&(rgarbage->l), &garbage_head)) {
                                        // DELETE? pr_info("no more garbage spaces\n");
                                        return 1;
                                }
                                rgarbage = list_entry(rgarbage->l.next, remote_garbage, l);
                                gstart = rgarbage->start;
                                gend = rgarbage->finish;
                                // DELETE? pr_info("advancing garbage space\n");
                        }
                        // Break cycle if this pagemap is finished.
                        if(pend >= pmend) { 
                               break;
                        }
                        
                        rpage = list_entry(rpage->l.next, remote_buffer, l);
                        pstart = pend;
                        pend += PAGESIZE;                        
                }
        }
        return 1;
}

// Get existing rmem or create one and return it.
remote_mem* get_rmem_for(char* path) 
{
        remote_mem *result;

        pthread_mutex_lock(&pages_lock);
        result = get_rmem_by_path(path);
        if(!result) {
                result = malloc(sizeof(remote_mem));
                if(result == NULL) {
                        pr_perror("Unable to allocate remote_mem structures");
                        pthread_mutex_unlock(&pages_lock);
                        return NULL;
                }
                INIT_LIST_HEAD(&(result->pagemap_head));
                strncpy(result->path, path, PATHLEN);
                result->pages_cached = malloc(sizeof(sem_t));
                if(!result->pages_cached) {
                        pr_perror("Unable to allocate sem_t structures");
                        pthread_mutex_unlock(&pages_lock);
                        return NULL;    
                }
                if (sem_init(result->pages_cached, 0, 0) != 0) {
                        pr_perror("Pages cached semaphore init failed");
                        pthread_mutex_unlock(&pages_lock);
                        return NULL;  
                }
                list_add_tail(&(result->l), &rmem_head);
        }
        pthread_mutex_unlock(&pages_lock);
        return result;
}

#endif

void* proxy_remote_image(void* ptr)
{
        remote_image* rimg = (remote_image*) ptr;
        rimg->dst_fd = prepare_client_socket(dst_host, dst_port);
        if (rimg->dst_fd < 0) {
                pr_perror("Unable to open recover image socket");
                return NULL;
        }

        if(write_header(rimg->dst_fd, rimg->namespace, rimg->path) < 0) {
                pr_perror("Error writing header for %s:%s", 
                        rimg->path, rimg->namespace);
                return NULL;
        }  

        prepare_put_rimg();
        
        if (!strncmp(rimg->path, DUMP_FINISH, sizeof(DUMP_FINISH))) 
        {
            close(rimg->dst_fd);
            finalize_put_rimg(rimg);
            return NULL;
        }
#if GC_COMPRESSION
        else if(!strncmp(rimg->path, "pages-", 6)) {
                remote_mem* rmem = get_rmem_for(rimg->path);
                rmem->pages = rimg;
                if (recv_remote_image(rimg->src_fd, rimg->path, &(rimg->buf_head)) < 0) {
                        return NULL;
                }
                finalize_put_rimg(rimg);
                sem_post(rmem->pages_cached);
                return NULL;
        }
        else if(!strncmp(rimg->path, "pagemap-", 8)) {
                remote_mem *rmem1, rmem2;
                INIT_LIST_HEAD(&(rmem2.pagemap_head));
                if (unpack_pagemap(rimg, &rmem2) == -1) {
                        pr_perror("Error unpacking pagemap %s", rimg->path);
                        return NULL;
                }
                finalize_put_rimg(rimg);
                rmem1 = get_rmem_for(rmem2.path);
                rmem1->pagemap = rimg;
                list_replace(&(rmem2.pagemap_head), &(rmem1->pagemap_head));
                rmem1->pheader = rmem2.pheader;
                rmem1->pagemap_magic_a = rmem2.pagemap_magic_a;
                rmem1->pagemap_magic_b = rmem2.pagemap_magic_b;


                sem_wait(rmem1->pages_cached);

                if( compress_garbage(rmem1) == -1) {
                        pr_perror("Compress garbage for %s failed.", rmem1->path);
                        return NULL;
                }
                pr_info("compressing done for %s.\n", rmem1->path);

                if(pack_pagemap(rimg, rmem1) == -1) {
                        pr_perror("Error packing pagemap %s", rimg->path);
                        return NULL;
                }

                // TODO - free memory (hein?)
                send_remote_pages(rmem1->pages->dst_fd, rmem1->pages->path, &(rmem1->pages->buf_head));
                return NULL;
        }
#endif
        if (recv_remote_image(rimg->src_fd, rimg->path, &(rimg->buf_head)) < 0) {
                return NULL;
        }
        finalize_put_rimg(rimg);
        send_remote_image(rimg->dst_fd, rimg->path, &(rimg->buf_head)); 
    return NULL;
}

#if GC_COMPRESSION
void* get_proxied_image(void* ptr) 
{
        remote_image* rimg = (remote_image*) ptr;

        if(!strncmp(rimg->path, "pagemap-", 8)) {
                remote_mem* rmem = get_rmem_by_rimg(rimg);
                if(!rmem) {
                        pr_perror("Could not find rmem for %s", rimg->path);
                        return NULL;
                }
                if(pack_pagemap(rimg, rmem) == -1) {
                        pr_perror("Error packing pagemap %s", rimg->path);
                        return NULL;
                }
        }
        else {
                send_remote_image(rimg->dst_fd, rimg->path, &rimg->buf_head);
        }
        return NULL;    
}

// NOTE: I am assuming that the garbage list is received in order.
int recv_garbage_list(int fd) 
{
        int n = 0;
        uint64_t start, finish;
        remote_garbage* rgarbage = NULL;

        while(1) {
                n = read(fd, &start, sizeof(uint64_t));
                if(!n) {
                        pr_info("Finished receiving garbage list.\n");  
                        close(fd);
                        break;
                }
                else if(n != sizeof(start)) {
                        pr_perror("Could not read vaddr from socket (garbage list)");
                        return -1;
                }

                n = read(fd, &finish, sizeof(uint64_t));
                if(n != sizeof(uint64_t)) {
                        pr_perror("Could not read # pages from socket (garbage list)");
                        return -1;
                }

                rgarbage = malloc(sizeof(remote_garbage));
                if(!rgarbage) {
                        pr_perror("Could not allocate remote garbage structure");
                        return -1;
                }
                rgarbage->start = start;
                rgarbage->finish = finish;
                pr_info("Received free region: start=%p finish=%p\n", 
                    decode_pointer(start), decode_pointer(finish));
                list_add_tail(&(rgarbage->l), &garbage_head);
        }
        return 1;
}

void* accept_free_regions(void* fd) 
{
        int gc_fd = *((int*) fd);
        int cli_fd;
        struct sockaddr_in cli_addr;
        socklen_t clilen = sizeof(cli_addr);

        while (1) {

                cli_fd = accept(gc_fd, (struct sockaddr *) &cli_addr, &clilen);
                if (cli_fd < 0) {
                        pr_perror("Unable to accept put image connection");
                        return NULL;
                }
                
                pr_info("Serving GC request\n");

                while(!list_empty(&garbage_head))
                        list_del(garbage_head.next);
                
                if (recv_garbage_list(cli_fd) < 0)
                        pr_perror("Error while receiving free regions");
        }
}

#endif

int image_proxy(char* fwd_host, unsigned short fwd_port) 
{
        pthread_t get_thr, put_thr;
        int put_fd, get_fd;
        
        dst_host = fwd_host;
        dst_port = fwd_port;
        
        pr_info("Proxy Get Port %d, Put Port %d, Destination Host %s:%hu\n", 
                PROXY_GET_PORT, PROXY_PUT_PORT, fwd_host, fwd_port);

        put_fd = prepare_server_socket(PROXY_PUT_PORT);
        get_fd = prepare_server_socket(PROXY_GET_PORT);
        if(init_proxy()) 
                return -1;

#if GC_COMPRESSION
        pthread_t gc_thr;
        int gc_fd  = prepare_server_socket(GC_SOCK_PORT);
        if (pthread_mutex_init(&pages_lock, NULL) != 0) {
                pr_perror("GC compression mutex init failedpr_perror");
                return -1;
        }
        if (pthread_create(
            &gc_thr, NULL, accept_free_regions, (void*) &gc_fd)) {
                pr_perror("Unable to create get thread");
                return -1;
        }
#endif

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
        
        // NOTE: these joins will never return...
        pthread_join(put_thr, NULL);
        pthread_join(get_thr, NULL);
        return 0;
}