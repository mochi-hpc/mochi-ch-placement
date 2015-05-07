/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>

#include "ch-placement.h"

/* This is a simple, one-shot test program for placement algorithms.  Given a
 * placement algorithm and system parameters (number of servers, and number
 * of virtual servers per server), it takes an object id and replication
 * factor and displays the servers that object would be mapped to.
 */

/* ch-placement-test <module> <n_svrs> <virt_factor> <replication_factor>
 */

int main(int argc, char **argv)
{
    int ret;
    unsigned n_svrs;
    unsigned virt_factor;
    unsigned replication_factor;
    struct ch_placement_instance *inst;
    unsigned long server_idxs[CH_MAX_REPLICATION];
    int i,j;
    unsigned long file_size = 1099511627776UL; /* 1 TB */
    unsigned int max_stripe_width = 0;
    unsigned int strip_size = 1048576UL; /* 1 MB */
    unsigned int num_objs;
    uint64_t *oids;
    unsigned long *sizes;

    /* argument parsing */
    /**************************/

    if(argc != 5)
    {
        fprintf(stderr, "Usage: %s <module> <n_svrs> <virt_factor> <replication_factor>\n", argv[0]);
        return(-1);
    }
    ret = sscanf(argv[2], "%u", &n_svrs);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: %s <module> <n_svrs> <virt_factor> <replication_factor>\n", argv[0]);
        return(-1);
    }
    ret = sscanf(argv[3], "%u", &virt_factor);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: %s <module> <n_svrs> <virt_factor> <replication_factor>\n", argv[0]);
        return(-1);
    }
    ret = sscanf(argv[4], "%u", &replication_factor);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: %s <module> <n_svrs> <virt_factor> <replication_factor>\n", argv[0]);
        return(-1);
    }

    if(replication_factor > CH_MAX_REPLICATION)
    {
        fprintf(stderr, "Error: max replication level is %u\n", CH_MAX_REPLICATION);
        return(-1);
    }

    /**************************/

    inst = ch_placement_initialize(argv[1], n_svrs, virt_factor, 0);

    if(!inst)
    {
        fprintf(stderr, "Error: failed to initialize %s\n", argv[1]);
        return(-1);
    }

    max_stripe_width = n_svrs/replication_factor;
    oids = malloc(max_stripe_width*sizeof(*oids));
    if(!oids)
    {
        perror("malloc");
        return(-1);
    }
    sizes = malloc(max_stripe_width*sizeof(*sizes));
    if(!sizes)
    {
        perror("malloc");
        return(-1);
    }

    ch_placement_create_striped(inst, file_size, replication_factor,
        max_stripe_width, strip_size, &num_objs, oids, sizes);
    printf("<oid> <server indices>\n========================\n");
    for(j=0; j<num_objs; j++)
    {
        ch_placement_find_closest(inst, oids[j], replication_factor, server_idxs);
        printf("%lu", oids[j]);
        for(i=0; i<replication_factor; i++)
        {
            printf("\t%lu", server_idxs[i]);
        }
        printf("\n");
    }

    free(oids);
    free(sizes);

    ch_placement_finalize(inst);

    return(0);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
