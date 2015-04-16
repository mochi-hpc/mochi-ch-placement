/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <assert.h>

#include "placement-xor.h"
#include "ch-placement.h"

static uint64_t placement_distance_xor(uint64_t a, uint64_t b, 
    unsigned int num_servers);
static void placement_find_closest_xor(uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs, unsigned int num_servers);

struct placement_mod mod_xor = 
{
    "xor",
    placement_find_closest_xor,
    placement_create_striped_random
};

struct placement_mod* placement_mod_xor(void)
{
    return(&mod_xor);
}

static void placement_find_closest_xor(uint64_t obj, unsigned int replication, 
    unsigned long* server_idxs, unsigned int num_servers)
{
    unsigned int i, j;
    uint64_t svr, tmp_svr;
    uint64_t servers[CH_MAX_REPLICATION];

    for(i=0; i<replication; i++)
        servers[i] = UINT64_MAX;

    for(i=0; i<num_servers; i++)
    {
        svr = placement_index_to_id(i);
        for(j=0; j<replication; j++)
        {
            if(servers[j] == UINT64_MAX || (obj ^ svr) < (obj ^ servers[j]))
            {
                tmp_svr = servers[j];
                servers[j] = svr;
                svr = tmp_svr;
            }
        }
    }

    for(i=0; i<replication; i++)
    {
        server_idxs[i] = placement_id_to_index(servers[i]);
    }

    return;
}



/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
