/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>

#include "ch-placement.h"
#include "src/modules/placement-mod.h"
#include "src/lookup3.h"

static struct placement_mod* placement_mod_ring(int n_svrs, int virt_factor);
static void placement_find_closest_ring(struct placement_mod *mod, uint64_t obj, 
    unsigned int replication, unsigned long *server_idxs);
static void placement_finalize_ring(struct placement_mod *mod);

static int vnode_cmp(const void* a, const void *b);
static int vnode_nearest_cmp(const void* a, const void *b);

struct placement_mod_map ring_mod_map = 
{
    .type = "ring",
    .initiate = placement_mod_ring,
};

struct ring_state;

struct vnode
{
    uint64_t svr_idx;
    uint64_t svr_id;
    struct ring_state* mod_state;
    unsigned long array_idx;
};

struct ring_state
{
    unsigned int n_svrs;
    unsigned int virt_factor;
    struct vnode *virt_table;
};

struct placement_mod* placement_mod_ring(int n_svrs, int virt_factor)
{
    struct placement_mod *mod_ring;
    struct ring_state *mod_state;
    uint32_t h1, h2;
    uint64_t i, j;

    mod_ring = malloc(sizeof(*mod_ring));
    if(!mod_ring)
        return(NULL);

    mod_state = malloc(sizeof(*mod_state));
    if(!mod_state)
    {
        free(mod_ring);
        return(NULL);
    }

    mod_ring->data = mod_state;

    mod_state->virt_table = malloc(sizeof(*mod_state->virt_table)*n_svrs*virt_factor);
    if(!mod_state->virt_table)
    {
        free(mod_state);
        free(mod_ring);
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

    qsort(mod_state->virt_table, n_svrs*virt_factor, sizeof(*mod_state->virt_table), vnode_cmp);

    for(i=0; i<n_svrs*virt_factor; i++)
    {
        mod_state->virt_table[i].array_idx = i;
        mod_state->virt_table[i].mod_state = mod_state;
    }

    mod_ring->find_closest = placement_find_closest_ring;
    mod_ring->create_striped = placement_create_striped_random;
    mod_ring->finalize = placement_finalize_ring;

    return(mod_ring);
}

static int vnode_cmp(const void* a, const void *b)
{
    const struct vnode *v_a = a;
    const struct vnode *v_b = b;

    if(v_a->svr_id < v_b->svr_id)
        return(-1);
    else if(v_a->svr_id > v_b->svr_id)
        return(1);
    else
        return(0);
}

static void placement_find_closest_ring(struct placement_mod *mod, uint64_t obj, unsigned int replication, 
    unsigned long* server_idxs)
{
    struct ring_state *mod_state = mod->data;
    struct vnode* svr;
    int current_index;
    int dup;
    int i,j;

    /* binary search through ring to find the server with the greatest virtual ID less than 
     * the oid 
     */
    svr = bsearch(&obj, mod_state->virt_table, mod_state->n_svrs*mod_state->virt_factor, 
        sizeof(*mod_state->virt_table), vnode_nearest_cmp);

    /* if bsearch didn't find a match, then the object belongs to the last
     * server partition
     */
    if(!svr)
        svr = &mod_state->virt_table[(mod_state->n_svrs*mod_state->virt_factor)-1];

    /* walk through ring, clockwise, to find N closest servers. */
    current_index = svr->array_idx;
    for(i=0; i<replication; i++)
    {
        if(current_index == mod_state->n_svrs*mod_state->virt_factor)
            current_index = 0;
        /* we have to skip duplicates */
        do
        {
            dup = 0;
            for(j=0; j<i; j++)
            {
                if(mod_state->virt_table[current_index].svr_idx == server_idxs[j])
                {
                    dup = 1;
                    current_index++;
                    if(current_index == mod_state->n_svrs*mod_state->virt_factor)
                        current_index = 0;
                    break;
                }
            }
        }while(dup);

        server_idxs[i] = mod_state->virt_table[current_index].svr_idx;
        current_index++;
    }

    return;
}

static int vnode_nearest_cmp(const void* key, const void *member)
{
    const uint64_t* obj = key;
    const struct vnode *svr = member;

    if(*obj < svr->svr_id)
        return(-1);
    if(*obj > svr->svr_id)
    {
        /* are we on the last server already? */
        if(svr->array_idx == (svr->mod_state->n_svrs*svr->mod_state->virt_factor-1))
            return(0);
        /* is the oid also larger than the next server's id? */
        if(svr->mod_state->virt_table[svr->array_idx+1].svr_id < *obj)
            return(1);
    }

    /* otherwise it is in our range */
    return(0);
}

static void placement_finalize_ring(struct placement_mod *mod)
{
    struct ring_state *mod_state = mod->data;

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
