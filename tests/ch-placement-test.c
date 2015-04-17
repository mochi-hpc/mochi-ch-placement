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

/* ch-placement-test <module> <n_svrs> <virt_factor> <oid> <replication_factor>
 */

int main(int argc, char **argv)
{
    int ret;
    unsigned n_svrs;
    unsigned virt_factor;
    uint64_t oid;
    unsigned replication_factor;
    struct ch_placement_instance *inst;

    if(argc != 6)
    {
        fprintf(stderr, "Usage: %s <module> <n_svrs> <virt_factor> <oid> <replication_factor>\n", argv[0]);
        return(-1);
    }

    ret = sscanf(argv[2], "%u", &n_svrs);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: %s <module> <n_svrs> <virt_factor> <oid> <replication_factor>\n", argv[0]);
        return(-1);
    }
    ret = sscanf(argv[3], "%u", &virt_factor);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: %s <module> <n_svrs> <virt_factor> <oid> <replication_factor>\n", argv[0]);
        return(-1);
    }
    /* TODO: make 32bit portable */
    ret = sscanf(argv[4], "%lu", &oid);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: %s <module> <n_svrs> <virt_factor> <oid> <replication_factor>\n", argv[0]);
        return(-1);
    }
    ret = sscanf(argv[5], "%u", &replication_factor);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: %s <module> <n_svrs> <virt_factor> <oid> <replication_factor>\n", argv[0]);
        return(-1);
    }

    inst = ch_placement_initialize(argv[1], n_svrs, virt_factor);

    if(!inst)
    {
        fprintf(stderr, "Error: failed to initialize %s\n", argv[1]);
        return(-1);
    }

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
