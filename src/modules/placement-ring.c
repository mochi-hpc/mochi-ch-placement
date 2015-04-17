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


#if 0
static uint64_t placement_distance_virt(uint64_t a, uint64_t b, 
    unsigned int n_svrs);
static void placement_find_closest_virt(uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs, unsigned int n_svrs);
static int virtual_cmp(const void* a, const void *b);
static int nearest_cmp(const void* a, const void *b);

static int partition_factor = 0;
static unsigned int n_svrs = 0;

struct server_id
{
    uint64_t virtual_id;
    unsigned long idx;
    struct server_id* first; /* pointer to first element in array */
    unsigned long array_idx;
};

static struct server_id* ring = NULL;

struct placement_mod mod_virt = 
{
    "virt",
    placement_distance_virt,
    placement_find_closest_virt,
    placement_create_striped_random
};

void placement_virt_set_factor(int factor, unsigned int set_n_svrs)
{
    unsigned int i, j;
    uint64_t server_increment;
    uint64_t server_id;
    uint32_t h1, h2;

    n_svrs = set_n_svrs;
    partition_factor = factor;
    assert(partition_factor > 0);
    assert(n_svrs > 0);
    
    server_increment = UINT64_MAX / n_svrs;

    ring = malloc(n_svrs*partition_factor*sizeof(*ring));
    assert(ring);

    for(i=0; i<n_svrs; i++)
    {
        for(j=0; j<partition_factor; j++)
        {
            ring[j+i*partition_factor].idx = i;
            server_id = 1+(server_increment*i);
            h1 = j;
            h2 = 0;
            bj_hashlittle2(&server_id, sizeof(server_id), &h1, &h2);
            ring[j+i*partition_factor].virtual_id = 
                h1 + (((uint64_t)h2)<<32);
        }
    }

    qsort(ring, n_svrs*partition_factor, sizeof(*ring), virtual_cmp);

    
    for(i=0; i<n_svrs*partition_factor; i++)
    {
        ring[i].array_idx = i;
        ring[i].first = ring;
        /* printf("FOO: server_idx:%lu, virtual_id:%lu\n", (long unsigned)ring[i].idx, (long unsigned)ring[i].virtual_id);  */
    }

    return;
}

struct placement_mod* placement_mod_virt(void)
{
    return(&mod_virt);
}

static uint64_t placement_distance_virt(uint64_t a, uint64_t b, 
    unsigned int n_svrs)
{
    /* not actually needed for this algorithm */
    assert(0);

    return(0);
}

static void placement_find_closest_virt(uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs, unsigned int arg_n_svrs)
{
    int i, j;
    struct server_id *svr;
    int current_index;
    int dup;

    assert(n_svrs == arg_n_svrs);

    /* binary search through ring to find the server with the greatest node ID
     * less than the OID
     */
    svr = bsearch(&obj, ring, n_svrs*partition_factor, sizeof(*ring), nearest_cmp);

    /* if bsearch didn't find a match, then the object belongs to the last
     * server partition
     */
    if(!svr)
        svr = &ring[(n_svrs*partition_factor)-1];

    current_index = svr->array_idx;

    for(i=0; i<replication; i++)
    {
        if(current_index == n_svrs*partition_factor)
            current_index = 0;
        /* we have to skip duplicates */
        do
        {
            dup = 0;
            for(j=0; j<i; j++)
            {
                if(ring[current_index].idx == server_idxs[j])
                {
                    dup = 1;
                    current_index++;
                    if(current_index == n_svrs*partition_factor)
                        current_index = 0;
                    break;
                }
            }
        }while(dup);

        server_idxs[i] = ring[current_index].idx;
        current_index++;
    }
}


static int nearest_cmp(const void* key, const void *member)
{
    const uint64_t* obj = key; 
    const struct server_id *svr = member;

    if(*obj < svr->virtual_id)
        return(-1);
    if(*obj > svr->virtual_id)
    {
        /* TODO: fix this.  Logic is wrong; need to stop at last entry in
         * ring, which is n_svrs * factor.
         */
        assert(0);
        /* are we on the last server already? */
        if(svr->array_idx == n_svrs-1)
            return(0);
        /* is the oid also larger than the next server's id? */
        if(svr->first[svr->array_idx+1].virtual_id < *obj)
            return(1);
    }

    /* otherwise it is in our range */
    return(0);
}

static int virtual_cmp(const void* a, const void *b)
{
    const struct server_id *s_a = a;
    const struct server_id *s_b = b;

    if(s_a->virtual_id < s_b->virtual_id)
        return(-1);
    else if(s_a->virtual_id > s_b->virtual_id)
        return(1);
    else
        return(0);

}

#endif

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
