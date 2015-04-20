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
#include "src/lookup3.h"

static struct placement_mod* placement_mod_xor(int n_svrs, int virt_factor);
static void placement_find_closest_xor(struct placement_mod *mod, uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs);
static void placement_finalize_xor(struct placement_mod *mod);

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
    unsigned int n_svrs;
    unsigned int virt_factor;
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
        for(j=0; j<virt_factor; j++)
        {
            h1 = j;
            h2 = 0;
            ch_bj_hashlittle2(&i, sizeof(i), &h1, &h2);
            mod_state->virt_table[j*n_svrs+i].svr_idx = i;
            mod_state->virt_table[j*n_svrs+i].svr_id = h1 + (((uint64_t)h2)<<32);
        }
    }

    mod_xor->find_closest = placement_find_closest_xor;
    mod_xor->create_striped = placement_create_striped_random;
    mod_xor->finalize = placement_finalize_xor;

    return(mod_xor);
}

/* TODO: implement a faster version of this; should not be O(n) */
static void placement_find_closest_xor(struct placement_mod *mod, uint64_t obj, unsigned int replication, 
    unsigned long* server_idxs)
{
    struct xor_state *mod_state = mod->data;
    struct vnode closest[CH_MAX_REPLICATION];
    struct vnode svr, tmp_svr;
    unsigned int i,j;

    for(i=0; i<replication; i++)
        closest[i].svr_idx = UINT64_MAX;

    for(i=0; i<(mod_state->n_svrs*mod_state->virt_factor); i++)
    {
        svr = mod_state->virt_table[i];
        for(j=0; j<replication; j++)
        {
            if(closest[j].svr_idx == UINT64_MAX || (obj ^ svr.svr_id) < (obj ^ closest[j].svr_id))
            {
                tmp_svr = closest[j];
                closest[j] = svr;
                svr = tmp_svr;
            }
        }
    }

    for(i=0; i<replication; i++)
    {
        server_idxs[i] = closest[i].svr_idx;
    }

    return;
}

static void placement_finalize_xor(struct placement_mod *mod)
{
    struct xor_state *mod_state = mod->data;

    free(mod_state->virt_table);
    free(mod_state);
    free(mod);

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
