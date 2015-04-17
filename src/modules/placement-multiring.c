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

static struct placement_mod* placement_mod_multiring(int n_svrs, int virt_factor);
static void placement_find_closest_multiring(struct placement_mod *mod, uint64_t obj, 
    unsigned int replication, unsigned long *server_idxs);
static void placement_finalize_multiring(struct placement_mod *mod);
static void placement_create_striped_multiring(
  struct placement_mod *mod,
  unsigned long file_size, 
  unsigned int replication, unsigned int max_stripe_width, 
  unsigned int strip_size,
  unsigned int* num_objects,
  uint64_t *oids, unsigned long *sizes);

static int vnode_cmp(const void* a, const void *b);
static int vnode_nearest_cmp(const void* key, const void *member);

struct placement_mod_map multiring_mod_map = 
{
    .type = "multiring",
    .initiate = placement_mod_multiring,
};

struct multiring_state;

struct vnode
{
    uint64_t svr_idx;
    uint64_t svr_id;
    struct multiring_state* mod_state;
    unsigned long array_idx;
};

struct multiring_state
{
    unsigned int n_svrs;
    unsigned int virt_factor;
    struct vnode **virt_table;
};

struct placement_mod* placement_mod_multiring(int n_svrs, int virt_factor)
{
    struct placement_mod *mod_multiring;
    struct multiring_state *mod_state;
    uint32_t h1, h2;
    uint64_t i, j;

    mod_multiring = malloc(sizeof(*mod_multiring));
    if(!mod_multiring)
        return(NULL);

    mod_state = malloc(sizeof(*mod_state));
    if(!mod_state)
    {
        free(mod_multiring);
        return(NULL);
    }

    mod_multiring->data = mod_state;

    mod_state->virt_table = malloc(sizeof(*mod_state->virt_table)*virt_factor);
    if(!mod_state->virt_table)
    {
        free(mod_state);
        free(mod_multiring);
        return(NULL);
    }
    for(i=0; i<virt_factor; i++)
    {
        mod_state->virt_table[i] = malloc(sizeof(*mod_state->virt_table[0])*n_svrs);
        if(!mod_state->virt_table[i])
        {
            free(mod_state->virt_table);
            free(mod_state);
            free(mod_multiring);
            return(NULL);
        }
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
            mod_state->virt_table[j][i].svr_idx = i;
            mod_state->virt_table[j][i].svr_id = h1 + (((uint64_t)h2)<<32);
        }
    }

    for(i=0; i<virt_factor; i++)
    {
        qsort(mod_state->virt_table[i], n_svrs, 
            sizeof(*mod_state->virt_table[0]), vnode_cmp);

        for(j=0; j<n_svrs; j++)
        {
            mod_state->virt_table[i][j].array_idx = j;
            mod_state->virt_table[i][j].mod_state = mod_state;
        }
    }

    mod_multiring->find_closest = placement_find_closest_multiring;
    mod_multiring->create_striped = placement_create_striped_multiring;
    mod_multiring->finalize = placement_finalize_multiring;

    return(mod_multiring);
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

static void placement_find_closest_multiring(struct placement_mod *mod, uint64_t obj, unsigned int replication, 
    unsigned long* server_idxs)
{
    struct multiring_state *mod_state = mod->data;
    struct vnode* svr;
    int current_index;
    int i;

    /* NOTE: there are other methods of partitioning objects across rings;
     * for now we assuming object IDs are randomly distributed and modulo
     * will work just fine.
     */
    int ring = obj % mod_state->virt_factor;

    /* binary search through multiring to find the server with the greatest 
     * virtual ID less than the oid 
     */
    svr = bsearch(&obj, mod_state->virt_table[ring], 
        mod_state->n_svrs, 
        sizeof(*mod_state->virt_table[0]), vnode_nearest_cmp);

    /* if bsearch didn't find a match, then the object belongs to the last
     * server partition
     */
    if(!svr)
        svr = &mod_state->virt_table[ring][mod_state->n_svrs-1];

    /* walk through ring, clockwise, to find N closest servers. */
    /* note: there are no duplicates on a given ring */
    current_index = svr->array_idx;
    for(i=0; i<replication; i++)
    {
        if(current_index == mod_state->n_svrs)
            current_index = 0;

        server_idxs[i] = mod_state->virt_table[ring][current_index].svr_idx;
        current_index++;
    }

    return;
}

static int vnode_nearest_cmp(const void* key, const void *member)
{
    const uint64_t* obj = key;
    const struct vnode *svr = member;
    int ring = (*obj) % svr->mod_state->virt_factor;

    if(*obj < svr->svr_id)
        return(-1);
    if(*obj > svr->svr_id)
    {
        /* are we on the last server already? */
        if(svr->array_idx == (svr->mod_state->n_svrs-1))
            return(0);
        /* is the oid also larger than the next server's id? */
        if(svr->mod_state->virt_table[ring][svr->array_idx+1].svr_id < *obj)
            return(1);
    }

    /* otherwise it is in our range */
    return(0);
}

static void placement_finalize_multiring(struct placement_mod *mod)
{
    struct multiring_state *mod_state = mod->data;
    int i;

    for(i=0; i<mod_state->virt_factor; i++)
        free(mod_state->virt_table[i]);
    free(mod_state->virt_table);
    free(mod_state);
    free(mod);

    return;
}

static void placement_create_striped_multiring(
  struct placement_mod *mod,
  unsigned long file_size, 
  unsigned int replication, unsigned int max_stripe_width, 
  unsigned int strip_size,
  unsigned int* num_objects,
  uint64_t *oids, unsigned long *sizes)
{
    struct multiring_state *mod_state = mod->data;
    int ring = random() % mod_state->virt_factor;
    int ring_idx = random() % mod_state->n_svrs;
    unsigned int stripe_width;
    int i;
    unsigned long size_left = file_size;
    unsigned long full_stripes;
    uint64_t range, oid_offset;
    unsigned long check_size = 0;

    /* how many objects to use */
    stripe_width = file_size / strip_size + 1;
    if(file_size % strip_size == 0)
        stripe_width--;
    if(stripe_width > max_stripe_width)
        stripe_width = max_stripe_width;
    *num_objects = stripe_width;

    /* size of each object */
    full_stripes = size_left/(stripe_width*strip_size);
    size_left -= full_stripes * stripe_width * strip_size;
    for(i=0; i<stripe_width; i++)
    {
        /* handle full stripes */
        sizes[i] = full_stripes*strip_size;
        /* handle remainder */
        if(size_left > 0)
        {
            if(size_left > strip_size)
            {
                sizes[i] += strip_size;
                size_left -= strip_size;
            }
            else
            {
                sizes[i] += size_left;
                size_left = 0;
            }
        }
        check_size += sizes[i];
    }
    assert(check_size == file_size);
    assert(size_left == 0);

    /* oid of each object */
    for(i=0; i<stripe_width; i++)
    {
        /* figure out size of object interval for this server on this ring */
        if(ring_idx < (mod_state->n_svrs-1))
            range = mod_state->virt_table[ring][ring_idx+1].svr_id - mod_state->virt_table[ring][ring_idx].svr_id;
        else
            range = UINT64_MAX - mod_state->virt_table[ring][ring_idx].svr_id
                + mod_state->virt_table[ring][0].svr_id;

        /* divide by mod_state->virt_factor to account for the fact that objects are
         * partitioned over each ring
         */
        range /= mod_state->virt_factor;
        /* conservatively reduce range to account for math skew below */
        range -= 3;

        /* pick oid offset within range as random number within range */
        oid_offset = ch_placement_random_u64() % range;
        /* calculate true oid based on offset */
        oids[i] = (mod_state->virt_table[ring][ring_idx].svr_id + (oid_offset+1)*mod_state->virt_factor);
        /* round down to an oid that falls in this ring */
        oids[i] -= oids[i]%mod_state->virt_factor;
        oids[i] += ring;

        ring_idx = (ring_idx + replication) % mod_state->n_svrs;
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
