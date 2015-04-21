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

struct options
{
    unsigned int num_servers;
    unsigned int num_objs;
    unsigned int replication;
    char* placement;
    unsigned int virt_factor;
    unsigned int kill_svr;
};

static int usage (char *exename);
static struct options *parse_args(int argc, char *argv[]);

int main(
    int argc,
    char **argv)
{
    struct options *ig_opts = NULL;
    unsigned long total_byte_count = 0;
    unsigned long total_obj_count = 0;
    struct obj* total_objs = NULL;
    unsigned int i;
    struct ch_placement_instance *instance;

    ig_opts = parse_args(argc, argv);
    if(!ig_opts)
    {
        usage(argv[0]);
        return(-1);
    }

    instance = ch_placement_initialize(ig_opts->placement, 
        ig_opts->num_servers,
        ig_opts->virt_factor);

    /* generate random set of objects for testing */
    printf("# Generating random object IDs...\n");
    oid_gen("random", instance, ig_opts->num_objs, ULONG_MAX,
        8675309, ig_opts->replication+1, ig_opts->num_servers,
        NULL,
        &total_byte_count, &total_obj_count, &total_objs);
    printf("# Done.\n");
    printf("#  Object population consuming approximately %lu MiB of memory.\n", (ig_opts->num_objs*sizeof(*total_objs))/(1024*1024));

    assert(total_obj_count == ig_opts->num_objs);

    printf("# Calculating placement for each object ID...\n");
#pragma omp parallel for
    for(i=0; i<ig_opts->num_objs; i++)
    {
        ch_placement_find_closest(instance, total_objs[i].oid, ig_opts->replication+1, total_objs[i].server_idxs);
    }
    printf("# Done.\n");

#if 0
    printf("# <objects>\t<replication>\t<servers>\t<algorithm>\t<time (s)>\t<rate oids/s>\n");
    printf("%u\t%d\t%u\t%s\t%f\t%f\n",
        ig_opts->num_objs,
        ig_opts->replication,
        ig_opts->num_servers,
        ig_opts->placement,
        t2-t1,
        (double)ig_opts->num_objs/(t2-t1));
#endif 

    /* we don't need the global list any more */
    free(total_objs);
    total_obj_count = 0;
    total_byte_count = 0;

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
    fprintf(stderr, "    -k <server to kill>\n");

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

    while((one_opt = getopt(argc, argv, "s:o:r:hp:v:k:")) != EOF)
    {
        switch(one_opt)
        {
            case 'k':
                ret = sscanf(optarg, "%u", &opts->kill_svr);
                if(ret != 1)
                    return(NULL);
                break;
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
    if(opts->kill_svr >= opts->num_servers)
        return(NULL);

    assert((opts->replication+1) <= CH_MAX_REPLICATION);

    return(opts);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
