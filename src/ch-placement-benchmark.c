/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/time.h>

#include "oid-gen.h"
#include "ch-placement.h"
#include "comb.h"

struct options
{
    unsigned int num_servers;
    unsigned int num_objs;
    unsigned int replication;
    char* placement;
    unsigned int virt_factor;
    char* comb_name;
};

struct comb_stats {
    unsigned long count;
    unsigned long bytes;
};

static int comb_cmp (const void *a, const void *b);
static int usage (char *exename);
static struct options *parse_args(int argc, char *argv[]);

static double Wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)(t.tv_usec) / 1000000);
}

int main(
    int argc,
    char **argv)
{
    struct options *ig_opts = NULL;
    unsigned long total_byte_count = 0;
    unsigned long total_obj_count = 0;
    struct obj* total_objs = NULL;
    unsigned int i;
    double t1, t2;
    struct ch_placement_instance *instance;
    int fd;
    struct comb_stats *cs;
    uint64_t num_combs;
    unsigned long comb_tmp[CH_MAX_REPLICATION];
    int ret;

    ig_opts = parse_args(argc, argv);
    if(!ig_opts)
    {
        usage(argv[0]);
        return(-1);
    }

    if (ig_opts->comb_name){
        /* set up state to count replica combinations and store results */
        fd = open(ig_opts->comb_name, O_WRONLY|O_CREAT|O_EXCL, 
                S_IRUSR|S_IWUSR|S_IRGRP);
        if(fd < 0) {
            perror("open");
            return(-1);
        }
        
        num_combs = choose(ig_opts->num_servers, ig_opts->replication);
        cs = calloc(num_combs, sizeof(*cs));
        assert(cs);
        printf("# Total possible combinations for %u servers and %u replication: %lu\n",
                ig_opts->num_servers, ig_opts->replication, num_combs);
        printf("#  Combo state metrics consuming %lu MiB of memory\n", (num_combs*sizeof(*cs))/(1024*1024));

    }

    instance = ch_placement_initialize(ig_opts->placement, 
        ig_opts->num_servers,
        ig_opts->virt_factor);

    /* generate random set of objects for testing */
    printf("# Generating random object IDs...\n");
    oid_gen("random", instance, ig_opts->num_objs, ULONG_MAX,
        8675309, ig_opts->replication, ig_opts->num_servers,
        NULL,
        &total_byte_count, &total_obj_count, &total_objs);
    printf("# Done.\n");
    printf("#  Object population consuming approximately %lu MiB of memory.\n", (ig_opts->num_objs*sizeof(*total_objs))/(1024*1024));

    assert(total_obj_count == ig_opts->num_objs);

    sleep(1);

    printf("# Calculating placement for each object ID...\n");
    /* run placement benchmark */
    t1 = Wtime();
#pragma omp parallel for
    for(i=0; i<ig_opts->num_objs; i++)
    {
        ch_placement_find_closest(instance, total_objs[i].oid, ig_opts->replication, total_objs[i].server_idxs);
        /* compute the index corresponding to this combination of servers */
        if (ig_opts->comb_name){
            memcpy(comb_tmp, total_objs[i].server_idxs, 
                    ig_opts->replication*sizeof(*comb_tmp));
            rev_ins_sort(ig_opts->replication, comb_tmp);
            uint64_t idx = comb_index(ig_opts->replication, comb_tmp);
            cs[idx].count++;
            cs[idx].bytes += total_objs[i].size;
        }
    }
    t2 = Wtime();
    printf("# Done.\n");

    if(!ig_opts->comb_name)
    {
        printf("# <objects>\t<replication>\t<servers>\t<virt_factor>\t<algorithm>\t<time (s)>\t<rate oids/s>\n");
        printf("%u\t%d\t%u\t%u\t%s\t%f\t%f\n",
            ig_opts->num_objs,
            ig_opts->replication,
            ig_opts->num_servers,
            ig_opts->virt_factor,
            ig_opts->placement,
            t2-t1,
            (double)ig_opts->num_objs/(t2-t1));
    }
    else
    {
        printf("# NOTE: computational performance not shown.\n");
        printf("#  Calculating combinations and outputing to %s.\n", ig_opts->comb_name);
    }

    /* we don't need the global list any more */
    free(total_objs);
    total_obj_count = 0;
    total_byte_count = 0;

    /* print out the counts of used combinations */
    if (ig_opts->comb_name){
        int sz = 1<<20;
        char *buf = malloc(sz);
        int written = 0;
        uint64_t total = 0;
        uint64_t num_zeros;

        printf("Sorting/writing server combinations\n");
        qsort(cs, num_combs, sizeof(*cs), comb_cmp);

        /* find the number of 0 entries - we aren't printing them */
        for (num_zeros = 0;
             cs[num_zeros].count == 0 && num_zeros < num_combs;
             num_zeros++);

        /* print the header - the number of possible combinations and the
         * number of non-zero entries */
        written = snprintf(buf+written, sz, "%lu %lu\n", num_combs,
                num_combs-num_zeros);
        assert(written < sz);

        /* start the counter where we left off */
        total = num_zeros;
        while (total < num_combs){
            int w = snprintf(buf+written, sz-written, "%lu %lu\n", 
                    cs[total].count, cs[total].bytes);
            if (w >= sz-written){
                ret = write(fd, buf, written);
                assert(ret == written);
                written=0;
            }
            else{
                written += w;
                total++;
            }
        }
        if (written > 0){
            ret = write(fd, buf, written);
            assert(ret == written);
        }
    }

    close(fd);


    return(0);
}

static int usage (char *exename)
{
    fprintf(stderr, "Usage: %s [options]\n", exename);
    fprintf(stderr, "    -s <number of servers>\n");
    fprintf(stderr, "    -o <number of objects>\n");
    fprintf(stderr, "    -r <replication factor>\n");
    fprintf(stderr, "    -p <placement algorithm>\n");
    fprintf(stderr, "    -v <virtual nodes per physical node>\n");
    fprintf(stderr, "    -c <output file for combinatorial statistics>\n");

    exit(1);
}

static struct options *parse_args(int argc, char *argv[])
{
    struct options *opts = NULL;
    int ret = -1;
    int one_opt = 0;

    opts = (struct options*)malloc(sizeof(*opts));
    if(!opts)
        return(NULL);
    memset(opts, 0, sizeof(*opts));

    while((one_opt = getopt(argc, argv, "s:o:r:hp:v:c:")) != EOF)
    {
        switch(one_opt)
        {
            case 's':
                ret = sscanf(optarg, "%u", &opts->num_servers);
                if(ret != 1)
                    return(NULL);
                break;
            case 'o':
                ret = sscanf(optarg, "%u", &opts->num_objs);
                if(ret != 1)
                    return(NULL);
                break;
            case 'v':
                ret = sscanf(optarg, "%u", &opts->virt_factor);
                if(ret != 1)
                    return(NULL);
                break;
            case 'r':
                ret = sscanf(optarg, "%u", &opts->replication);
                if(ret != 1)
                    return(NULL);
                break;
            case 'p':
                opts->placement = strdup(optarg);
                if(!opts->placement)
                    return(NULL);
                break;
            case 'c':
                opts->comb_name = strdup(optarg);
                if(!opts->comb_name)
                    return(NULL);
                break;
            case '?':
                usage(argv[0]);
                exit(1);
        }
    }

    if(opts->replication < 2)
        return(NULL);
    if(opts->num_servers < (opts->replication+1))
        return(NULL);
    if(opts->num_objs < 1)
        return(NULL);
    if(opts->virt_factor < 1)
        return(NULL);
    if(!opts->placement)
        return(NULL);

    assert(opts->replication <= CH_MAX_REPLICATION);

    return(opts);
}

static int comb_cmp (const void *a, const void *b){
    unsigned long au = ((struct comb_stats*)a)->count;
    unsigned long bu = ((struct comb_stats*)b)->count; 
    int rtn;
    if (au < bu)       rtn = -1;
    else if (au == bu) rtn = 0;
    else               rtn = 1;
    return rtn;
}



/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
