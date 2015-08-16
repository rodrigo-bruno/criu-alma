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
#include <utlist.h>

#include <google/protobuf-c/protobuf-c.h>

#include "image-remote.h"
#include "image-remote-pvt.h"
#include "criu-log.h"
#include "asm/types.h"
#include "protobuf.h"
#include "protobuf/pagemap.pb-c.h"
#include "image-desc.h"

// GC compression means that we avoid transferring garbage data.
#define GC_COMPRESSION 1

// TODO - use CRIU's list implementation
// TODO - check when both proxy and cache daemons die.

static remote_image *head = NULL;
static int sockfd = -1;
static sem_t semph;
static unsigned short server_port = DEFAULT_PUT_PORT;
static char* dst_host;
static unsigned short dst_port = DEFAULT_PUT_PORT;

#if GC_COMPRESSION

#define PB_PKOBJ_LOCAL_SIZE	1024*4 // Multiplied by 4 to assure no problems.

typedef struct rpagemap {
    PagemapEntry* pentry;
    struct rpagemap *next, *prev;
} remote_pagemap;

typedef struct rgarbage {
    uint64_t vaddr;
    uint32_t nr_pages;
    struct rgarbage *next, *prev;
} remote_garbage;

typedef struct rmem {
    char path[PATHLEN];
    remote_image *pages, *pagemap;
    PagemapHead* pheader;
    remote_pagemap* pagemap_list;
    remote_garbage* garbage_list;
    
    u32 pagemap_magic_a;
    u32 pagemap_magic_b;
    struct rmem *next, *prev;
    sem_t pages_cached;
    sem_t garbage_cached;
} remote_mem;

static pthread_mutex_t pages_lock;
static remote_mem *rmem_head = NULL;

int recv_remote_image(remote_image* rimg);
int send_remote_image(remote_image* rimg);

int path_cmp_rmem(remote_mem *a, remote_mem *b) {
    return strcmp(a->path, b->path);
} 

int pb_unpack_object(int fd, int eof, int type, void** pobj) {
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

int pb_pack_object(int fd, int type, void* obj) {
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
int rimg_read_magic(int fd, remote_mem* rmem) {
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
int rimg_write_magic(int fd, remote_mem* rmem) {
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
                    pr_info("Unpacking done for %s.n", rimg->path);
                    return nbytes;
                }
                nbytes += ret;
                remote_pagemap* rpagemap = malloc(sizeof(remote_pagemap));
                if (!rpagemap) {
                    pr_perror("Cannot allocate memory for remote_pagemap");
                    return -1;
                }
                rpagemap->pentry = (PagemapEntry*) pobj;
                // DEBUG
                printf("pagemap entry -> pages = %u, vaddr = %p\n", rpagemap->pentry->nr_pages, decode_pointer(rpagemap->pentry->vaddr));
                DL_APPEND(rmem->pagemap_list, rpagemap);
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
        
        for(rpmap = rmem->pagemap_list; rpmap != NULL; rpmap = rpmap->next) {
                ret = pb_pack_object(rimg->dst_fd, PB_PAGEMAP, rpmap->pentry);
                if(ret < 0) {
                        pr_perror("Error packing pagemap from %s.", rmem->path);
                        return -1;
                }
                nbytes += ret;
        }
        close(rimg->dst_fd);
        pr_info("Packing done for %s.n", rimg->path);
        return nbytes;
}

// NOTE: I am assuming that the garbage list is received in order.
int recv_garbage_list(int fd, remote_mem* rmem) {
    int n = 0;
    uint64_t vaddr;
    uint32_t nr_pages;
    remote_garbage* rgarbage = NULL;
    
    while(1) {
        n = read(fd, &vaddr, sizeof(vaddr));
        if(!n) {
            pr_info("Finished receiving garbage list.\n");
            close(fd);
            break;
        }
        else if(n != sizeof(vaddr)) {
            pr_perror("Could not read vaddr from socket (garbage list)");
            return -1;
        }
        
        n = read(fd, &nr_pages, sizeof(nr_pages));
        if(n != sizeof(nr_pages)) {
            pr_perror("Could not read # pages from socket (garbage list)");
            return -1;
        }
        
        rgarbage = malloc(sizeof(remote_garbage));
        if(!rgarbage) {
            pr_perror("Could not allocate remote garbage structure");
            return -1;
        }
        rgarbage->vaddr = vaddr;
        rgarbage->nr_pages = nr_pages;
        DL_APPEND(rmem->garbage_list, rgarbage);
    }
    return 1;
}

int clean_pages(remote_buffer* rhead, remote_buffer** rpage, 
                uint64_t curr, uint64_t from, uint64_t to) {
    remote_buffer* aux = NULL;
    uint64_t i;
    
    // Advance to from position
    for(i = (from - curr) / PAGESIZE; i > 0; i--) {
        *rpage = (*rpage)->next;
        if(*rpage == NULL) {
            pr_perror("Pages ended while advancing...");
            return -1;
        }
    }
    
    // Start deleting garbage pages
    for(i = (to - from) / PAGESIZE; i > 0; i--) {
        aux = (*rpage)->next;
        DL_DELETE(rhead, *rpage);
        *rpage = aux;
        if(*rpage == NULL && i > 1) {
            pr_perror("Pages ended while deleting...");
            return -1;
        }
    }
    return 1;
}

remote_pagemap* alloc_pagemap() {
    remote_pagemap* new_pm = NULL;
    PagemapEntry* new_pe = NULL;
    
    new_pm = malloc(sizeof(remote_pagemap));
    if(!new_pm) {
        pr_perror("Could not allocate remote_pagemap structure");
        return NULL;
    }
    
    new_pe = malloc(sizeof(PagemapEntry));
    if(!new_pe) {
        pr_perror("Could not allocate PagemapEntry structure");
        return NULL;
    }
    new_pm->pentry = new_pe;
    return new_pm;
}

int compress_garbage(remote_mem* rmem) {
    remote_garbage* rgarbage = rmem->garbage_list;
    remote_pagemap* rpagemap = rmem->pagemap_list;
    remote_buffer* rpage = rmem->pages->buf_head;
    remote_buffer* rpage_head = rmem->pages->buf_head;
    uint32_t counter;
    uint64_t gstart, gend, pstart, pend;

    while(rgarbage != NULL) {
        gstart = rgarbage->vaddr;
        gend = gstart + rgarbage->nr_pages * PAGESIZE;
        
        // Advance until we hit the first garbage pages
        pstart = rpagemap->pentry->vaddr;
        pend = pstart + rpagemap->pentry->nr_pages * PAGESIZE;
        while(pend <= gstart) { 
            for(counter = 0; counter < rpagemap->pentry->nr_pages; counter++) {
                rpage = rpage->next;
                if(rpage == NULL) {
                    pr_perror("Page list ended while advancing page maps");
                    return -1;
                }
            }
            rpagemap = rpagemap->next;
            if(!rpagemap) {
                pr_perror("Page mappings ended before all garbage is processed (%s)"
                          , rmem->path);
            }
            
            pstart = rpagemap->pentry->vaddr;
            pend = pstart + rpagemap->pentry->nr_pages * PAGESIZE;
        }
                
        // Case 1
        if(pstart <= gstart && pend >= gend) {
            remote_pagemap* new = alloc_pagemap();
            rpagemap->pentry->nr_pages = (gstart - pstart) / PAGESIZE; 
            new->pentry->vaddr = gend;
            new->pentry->nr_pages = (pend - gend) / PAGESIZE;
            if(rpagemap->next == NULL) {
                DL_APPEND(rmem->pagemap_list, new);
            }
            else {
                DL_PREPEND_ELEM(rmem->pagemap_list, rpagemap, new);
            }
            if(clean_pages(rpage_head, &rpage, pstart, gstart, gend) == -1) {
                pr_perror("Could not clean pages in case 1 (%s)", rmem->path);
                return -1;
            }
        }
        // Case 2
        else if(pstart >= gstart && pend <= gend) {
            DL_DELETE(rmem->pagemap_list, rpagemap);
            if(clean_pages(rpage_head, &rpage, pstart, pstart, pend) == -1) {
                pr_perror("Could not clean pages in case 2 (%s)", rmem->path);
                return -1;
            }
        }
        // Case 3
        else if(pstart <= gstart && pend > gstart && pend <= gend) { 
            rpagemap->pentry->nr_pages = (gstart - pstart) / PAGESIZE;
            if(clean_pages(rpage_head, &rpage, pstart, gstart, pend) == -1) {
                pr_perror("Could not clean pages in case 3 (%s)", rmem->path);
                return -1;
            }            
        }
        // Case 4
        else if(pstart > gstart && pstart <= gend && pend >= gend) {
            rpagemap->pentry->vaddr = gend;
            if(clean_pages(rpage_head, &rpage, pstart, pstart, gend) == -1) {
                pr_perror("Could not clean pages in case 4 (%s)", rmem->path);
                return -1;
            }
        }
        // Unexpected case
        else {
            // TODO - report error, unexpected scenario. Print all addresses.
            pr_perror("Unexpected scenario for %s", rmem->path);
            return -1;
        }

        rgarbage = rgarbage->next;
    }
    
    return 1;
}

// Get existing rmem or create one and return it.
remote_mem* get_rmem_for(char* path) {
    remote_mem *result, like;
    strncpy(like.path, path, PATHLEN);
    
    pthread_mutex_lock(&pages_lock);
    DL_SEARCH(rmem_head, result, &like, path_cmp_rmem);
    if(!result) {
        result = malloc(sizeof(remote_mem));
        if(result == NULL) {
            pr_perror("Unable to allocate remote_mem structures");
            pthread_mutex_unlock(&pages_lock);
            return NULL;
        }
        else {
            result->pagemap_list = NULL;
            result->garbage_list = NULL;
            strncpy(result->path, path, PATHLEN);
            if (sem_init(&(result->pages_cached), 0, 0) != 0) {
                pr_perror("Pages cached semaphore init failed");
                pthread_mutex_unlock(&pages_lock);
                return NULL;  
            }
            if (sem_init(&(result->garbage_cached), 0, 0) != 0) {
                pr_perror("Gargabe cached semaphore init failed");
                pthread_mutex_unlock(&pages_lock);
                return NULL;  
            }
            DL_APPEND(rmem_head, result);
        }
    }
    pthread_mutex_unlock(&pages_lock);
    return result;
}

#endif

int recv_remote_image(remote_image* rimg) {
    int n;
    int src_fd = rimg->src_fd;
    remote_buffer* curr_buf = rimg->buf_head;
    
    while(1) {
        n = read(   src_fd, 
                    curr_buf->buffer + curr_buf->nbytes, 
                    BUF_SIZE - curr_buf->nbytes);
        if (n == 0) {
            close(src_fd);
            pr_info("Finished receiving %s.\n", rimg->path);
            break;
        }
        else if (n > 0) {
            curr_buf->nbytes += n;
            if(curr_buf->nbytes == BUF_SIZE) {
                remote_buffer* buf = malloc(sizeof (remote_buffer));
                if(buf == NULL) {
                    pr_perror("Unable to allocate remote_buffer structures");
                    return -1;
                }
                buf->nbytes = 0;
                DL_APPEND(rimg->buf_head, buf);
                curr_buf = buf;
            }
            
        }
        else {
            pr_perror("Read on %s socket failed", rimg->path);
            return -1;
        }
    }
    return 0;
}

int send_remote_image(remote_image* rimg) {
    int dst_fd = rimg->dst_fd;
    remote_buffer* curr_buf = rimg->buf_head;
    int n, curr_offset = 0;
    
    while(1) {
        n = write(
                    dst_fd, 
                    curr_buf->buffer + curr_offset, 
                    MIN(BUF_SIZE, curr_buf->nbytes) - curr_offset);
        if(n > -1) {
            curr_offset += n;
            if(curr_offset == BUF_SIZE) {
                curr_buf = curr_buf->next;
                curr_offset = 0;
            }
            else if(curr_offset == curr_buf->nbytes) {
                pr_info("Finished forwarding %s.\n", rimg->path);
                close(dst_fd);
                break;
            }
        }
        else {
             pr_perror("Write on %s socket failed (n=%d)", rimg->path, n);
        }
    }
    return 0;
}

void* buffer_remote_image(void* ptr) {
    remote_image* rimg = (remote_image*) ptr;

#if GC_COMPRESSION
    if(!strncmp(rimg->path, "pages-", 6)) {
        remote_mem* rmem = get_rmem_for(rimg->path);
        rmem->pages = rimg;
        if (recv_remote_image(rimg)) {
            return NULL;
        }
        sem_post(&(rmem->pages_cached));
        return NULL;
    }
    else if(!strncmp(rimg->path, "pagemap-", 8)) {
        remote_mem *rmem1, rmem2;
        if (unpack_pagemap(rimg, &rmem2) == -1) {
            pr_perror("Error unpacking pagemap %s", rimg->path);
            return NULL;
        }
        rmem1 = get_rmem_for(rmem2.path);
        rmem1->pagemap = rimg;
        rmem1->pagemap_list = rmem2.pagemap_list;
        rmem1->pheader = rmem2.pheader;
        rmem1->pagemap_magic_a = rmem2.pagemap_magic_a;
        rmem1->pagemap_magic_b = rmem2.pagemap_magic_b;
        
        
        sem_wait(&(rmem1->pages_cached));
        
        /*
        sem_wait(&(rmem1->garbage_cached));
        if( compress_garbage(rmem1) == -1) {
            pr_perror("Compress garbage for %s failed.", rmem1->path);
            return NULL;
        }
        pr_info("compressing done for %s.\n", rmem1->path);
        */
        if(pack_pagemap(rimg, rmem1) == -1) {
            pr_perror("Error packing pagemap %s", rimg->path);
            return NULL;
        }
        
        // TODO - free memory
        send_remote_image(rmem1->pages);
        return NULL;
    }
    else if(!strncmp(rimg->path, "garbage-", 8)) {
        remote_mem* rmem = NULL;
        char path[PATHLEN];
        int pid;
        
        // We do not need to send this file to the cache.
        close(rimg->dst_fd);
        
        sscanf(rimg->path, "garbage-%d", &pid);
        sprintf(path, "pages-%d", pid);
        rmem = get_rmem_for(path);
        
        recv_garbage_list(rimg->src_fd, rmem);
        sem_post(&(rmem->garbage_cached));
        return NULL;
    }
#endif
    
    if (recv_remote_image(rimg)) {
        return NULL;
    }
    send_remote_image(rimg);
    return NULL;
}

void* accept_remote_image_connections(void* null) {
    socklen_t clilen;
    int src_fd, dst_fd, n;
    struct sockaddr_in cli_addr, serv_addr;
    clilen = sizeof (cli_addr);
    struct hostent *restore_server;

    while (1) {
        src_fd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (src_fd < 0) {
            pr_perror("Unable to accept checkpoint image connection");
            continue;
        }

        remote_image* img = malloc(sizeof (remote_image));
        if (img == NULL) {
            pr_perror("Unable to allocate remote_image structures");
            return NULL;
        }
        
        remote_buffer* buf = malloc(sizeof (remote_buffer));
        if(buf == NULL) {
            pr_perror("Unable to allocate remote_buffer structures");
            return NULL;
        }

        n = read(src_fd, img->path, PATHLEN);
        if (n < 0) {
            pr_perror("Error reading from checkpoint remote image socket");
            continue;
        } else if (n == 0) {
            pr_perror("Remote checkpoint image socket closed before receiving path");
            continue;
        }
        
        
        dst_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (dst_fd < 0) {
            pr_perror("Unable to open recover image socket");
            return NULL;
        }

        restore_server = gethostbyname(dst_host);
        if (restore_server == NULL) {
            pr_perror("Unable to get host by name (%s)", dst_host);
            return NULL;
        }

        bzero((char *) &serv_addr, sizeof (serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy(  (char *) restore_server->h_addr,
                (char *) &serv_addr.sin_addr.s_addr,
                restore_server->h_length);
        serv_addr.sin_port = htons(dst_port);

        n = connect(dst_fd, (struct sockaddr *) &serv_addr, sizeof (serv_addr));
        if (n < 0) {
            pr_perror("Unable to connect to remote restore host %s: %s", dst_host, strerror(errno));
            return NULL;
        }

        if (write(dst_fd, img->path, PATHLEN) < 1) {
            pr_perror("Unable to send path to remote image connection");
            return NULL;
        }
        
        if (!strncmp(img->path, DUMP_FINISH, sizeof (DUMP_FINISH))) {
            pr_info("Dump side is finished!\n");
            free(img);
            free(buf);
            close(src_fd);
            sem_post(&semph);
            return NULL;
        }
       
        img->src_fd = src_fd;
        img->dst_fd = dst_fd;
        img->buf_head = NULL;
        buf->nbytes = 0;
        DL_APPEND(img->buf_head, buf);
        
        if (pthread_create( &img->putter, 
                            NULL, 
                            buffer_remote_image, 
                            (void*) img)) {
                pr_perror("Unable to create socket thread");
                return NULL;
        } 
        
        pr_info("Reveiced put request for %s.\n", img->path);
        DL_APPEND(head, img);
    }
}

int image_proxy(char* cache_host, unsigned short cache_port) {
    int sockopt = 1;
    struct sockaddr_in serv_addr;
    pthread_t sock_thr;
    
    dst_host = cache_host;
    dst_port = cache_port;
    pr_info("Proxy Port %d, Destination Host %s:%hu\n", server_port, dst_host, dst_port);
    
    if (sem_init(&semph, 0, 0) != 0) {
        pr_perror("Remote image connection semaphore init failed");
        return -1;
    }

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        pr_perror("Unable to open image socket");
        return -1;
    }

    bzero((char *) &serv_addr, sizeof (serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(server_port);

    if (setsockopt(
            sockfd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &sockopt,
            sizeof (sockopt)) == -1) {
        pr_perror("Unable to set SO_REUSEADDR");
        return -1;
    }

    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof (serv_addr)) < 0) {
        pr_perror("Unable to bind image socket");
        return -1;
    }

    if (listen(sockfd, DEFAULT_LISTEN)) {
        pr_perror("Unable to listen image socket");
        return -1;
    }
    
#if GC_COMPRESSION
    if (pthread_mutex_init(&pages_lock, NULL) != 0) {
        pr_perror("GC compression mutex init failedpr_perror");
        return -1;
    }
#endif

    if (pthread_create(
            &sock_thr, NULL, accept_remote_image_connections, NULL)) {
        pr_perror("Unable to create socket thread");
        return -1;

    }
    
    sem_wait(&semph);
    // TODO - why not to replace this semph with a pthread_join?
    
    remote_image *elt, *tmp;
    DL_FOREACH_SAFE(head,elt,tmp) {
        pthread_join(elt->putter, NULL);
        DL_DELETE(head,elt);
    }
    
    return 0;
}
