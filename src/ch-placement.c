/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>

#include "ch-placement.h"
#include "src/modules/placement-mod.h"

/* externs pointing to api for each module */
extern struct placement_mod_map xor_mod_map;
extern struct placement_mod_map ring_mod_map;
extern struct placement_mod_map multiring_mod_map;
extern struct placement_mod_map hash_lookup3_mod_map;

/* table of available modules */
static struct placement_mod_map *table[] = 
{
    &xor_mod_map,
    &ring_mod_map,
    &multiring_mod_map,
    &hash_lookup3_mod_map,
    NULL,
};

struct ch_placement_instance
{
    struct placement_mod *mod;
};

#ifdef CH_ENABLE_CRUSH
#include "ch-placement-crush.h"
extern struct placement_mod* placement_mod_crush(struct crush_map *map, __u32 *weight, int n_weight)

struct ch_placement_instance* ch_placement_initialize_crush(struct crush_map *map, __u32 *weight, int n_weight)
{
    struct ch_placement_instance *instance = NULL;

    instance = malloc(sizeof(*instance));
    if(instance)
    {
        instance->mod = placement_mod_crush(map, weight, n_weight);
        if(!instance->mod)
        {
            free(instance);
            instance = NULL;
        }
    }

    return(instance);
}

#endif

struct ch_placement_instance* ch_placement_initialize(const char* name,
    int n_svrs, int virt_factor)
{
    struct ch_placement_instance *instance = NULL;
    int i;

    for(i=0; table[i]!= NULL; i++)
    {
        if(strcmp(name, table[i]->type) == 0)
        {
            instance = malloc(sizeof(*instance));
            if(instance)
            {
                instance->mod = table[i]->initiate(n_svrs, virt_factor);
                if(!instance->mod)
                {
                    free(instance);
                    instance = NULL;
                }
            }
            break;
        }
    }
    
    return(instance);
}

void placement_create_striped_random(struct placement_mod *mod, 
    unsigned long file_size, 
  unsigned int replication, unsigned int max_stripe_width, 
  unsigned int strip_size,
  unsigned int* num_objects,
  uint64_t *oids, unsigned long *sizes)
{
    unsigned int stripe_width;
    int i;
    unsigned long size_left = file_size;
    unsigned long full_stripes;

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
    }

    /* oid of each object */
    for(i=0; i<stripe_width; i++)
    {
        oids[i] = ch_placement_random_u64();
    }

    return;
}

/* TODO: optimize this */
uint64_t ch_placement_random_u64(void)
{
    uint64_t bigr = 0;
    uint64_t litr;
    int i;

    for(i=0; i<8; i++)
    {
        litr = random();
        litr = litr & 0xFF;
        bigr += (litr << (i*8));
    }

    return(bigr);
}

void ch_placement_finalize(struct ch_placement_instance *instance)
{
    instance->mod->finalize(instance->mod);
    free(instance);
    return;
}

void ch_placement_find_closest(
    struct ch_placement_instance *instance,
    uint64_t obj, 
    unsigned int replication, 
    unsigned long* server_idxs)
{
    instance->mod->find_closest(instance->mod, obj, replication, server_idxs);
    return;
}

void ch_placement_create_striped(
    struct ch_placement_instance *instance,
    unsigned long file_size, 
    unsigned int replication, 
    unsigned int max_stripe_width, 
    unsigned int strip_size,
    unsigned int* num_objects,
    uint64_t *oids, 
    unsigned long *sizes)
{
    instance->mod->create_striped(instance->mod, file_size,
        replication, max_stripe_width, strip_size, num_objects, oids,
        sizes);
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
