/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "ch-placement.h"
#include "src/modules/placement-mod.h"


static struct placement_mod* placement_mod_xor(int n_svrs, int virt_factor);
static uint64_t placement_distance_xor(uint64_t a, uint64_t b, 
    unsigned int num_servers);
static void placement_find_closest_xor(uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs);

struct placement_mod_map xor_mod_map = 
{
    .type = "xor",
    .initiate = placement_mod_xor,
};

struct vnode
{
    uint64_t svr_idx;
    uint64_t svr_id;
};

struct xor_state
{
    int n_svrs;
    int virt_factor;
    struct vnode *virt_table;
};

struct placement_mod* placement_mod_xor(int n_svrs, int virt_factor)
{
    struct placement_mod *mod_xor;
    struct xor_state *mod_state;
    uint32_t h1, h2;
    uint64_t i, j;

    mod_xor = malloc(sizeof(*mod_xor));
    if(!mod_xor)
        return(NULL);

    mod_state = malloc(sizeof(*mod_state));
    if(!mod_state)
    {
        free(mod_xor);
        return(NULL);
    }

    mod_xor->data = mod_state;

    mod_state->virt_table = malloc(sizeof(*mod_state->virt_table)*n_svrs*virt_factor);
    if(!mod_state->virt_table)
    {
        free(mod_state);
        free(mod_xor);
        return(NULL);
    }

    mod_state->n_svrs = n_svrs;
    mod_state->virt_factor = virt_factor;

    /* create virt_factor virtual nodes for each server index by jenkins
     * hashing server index
     */
    for(i=0; i<n_svrs; i++)
    {
        for(j=0; j<n_svrs*virt_factor; j++)
        {
            h1 = j;
            h2 = 0;
            bj_hashlittle2(&i, sizeof(i), &h1, &h2);
            mod_state->virt_table[j*n_svrs+i].svr_idx = i;
            mod_state->virt_table[j*n_svrs+i].svr_id = h1 + (((uint64_t)h2)<<32);
        }
    }

    mod_xor->find_closest = placement_find_closest_xor;
    mod_xor->create_striped = placement_create_striped_random;

    return(mod_xor);
}

static void placement_find_closest_xor(uint64_t obj, unsigned int replication, 
    unsigned long* server_idxs)
{
#if 0
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

#endif
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
