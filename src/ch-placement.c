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

/* table of available modules */
static struct placement_mod_map *table[] = 
{
    &xor_mod_map,
    NULL,
};

struct ch_placement_instance
{
    struct placement_mod *mod;
};

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

void placement_create_striped_random(unsigned long file_size, 
  unsigned int replication, unsigned int max_stripe_width, 
  unsigned int strip_size,
  unsigned int* num_objects,
  uint64_t *oids, unsigned long *sizes,
  struct placement_mod *mod)
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

#if 0
static unsigned int placement_num_servers = 0;
struct placement_mod* module = NULL;

/* set placement parameters: placement type and number of servers */
void placement_set(char* type, unsigned int num_servers)
{
    char* params;
    int num_rings;
    int partition_factor;
    int ret;

    placement_num_servers = num_servers;
    assert(placement_num_servers);

    if(strcmp(type, "1d") == 0)
    {
        module = placement_mod_euclid_1d();
        printf("# WARNING: 1d places objects on overall nearest server rather than nearest server to the left as is conventional in the literature.\n");
    }
    else if(strcmp(type, "xor") == 0)
    {
        printf("# WARNING: xor will not produced balanced utilization, because server IDs are distributed evenly in 1d space rather than xor space.\n");
        printf("# WARNING: simulations using xor don't account for computation overhead.\n");
        module = placement_mod_xor();
    }
    else if(strcmp(type, "2d") == 0)
    {
        printf("# WARNING: 2d will not produced balanced utilization, because server IDs are distributed evenly in 1d space rather than 2d space.\n");
        printf("# WARNING: simulations using 2d don't account for computation overhead.\n");
        module = placement_mod_euclid_2d();
    }
    else if(strcmp(type, "hash") == 0)
    {
        printf("# WARNING: simulations using hash don't account for computation overhead.\n");
        module = placement_mod_hash();
    }
    else if (strcmp(type, "hash-spooky") == 0){
        printf("# WARNING: simulations using hash-spooky don't account for computation overhead.\n");
        module = placement_mod_hash_spooky();
    }
    else if(strncmp(type, "multiring:", strlen("multiring:")) == 0)
    {
        module = placement_mod_multiring();

        /* parse out number of rings from type name */
        params = index(type, ':');
        params++;
        ret = sscanf(params, "%d", &num_rings);
        assert(ret == 1);
        placement_multiring_set_rings(num_rings, num_servers);
    }
    else if(strncmp(type, "virt:", strlen("virt:")) == 0)
    {
        module = placement_mod_virt();

        /* parse out number partition factor for virtual nodes */
        params = index(type, ':');
        params++;
        ret = sscanf(params, "%d", &partition_factor);
        assert(ret == 1);
        placement_virt_set_factor(partition_factor, num_servers);
    }


    assert(module);

    return;
}

/* distance between two object/server ids */
uint64_t placement_distance(uint64_t a, uint64_t b)
{
    return(module->placement_distance(a, b, placement_num_servers));
}

/* convert server id to index (from 0 to num_servers-1) */
unsigned int placement_id_to_index(uint64_t id)
{
    uint64_t server_increment = UINT64_MAX / placement_num_servers;
    unsigned int idx;

    idx = (id-1)/server_increment;

    assert(idx < placement_num_servers);
    return(idx);
}

/* convert server index (from 0 to num_servers-1) to an id */
uint64_t placement_index_to_id(unsigned int idx)
{
    uint64_t svr_id;
    uint64_t server_increment = UINT64_MAX / placement_num_servers;

    svr_id = idx;
    svr_id *= server_increment;
    svr_id++;
    return(svr_id);
}

/* calculate the N closest servers to a given object */
void placement_find_closest(uint64_t obj, unsigned int replication, 
    unsigned long* server_idxs)
{
    return(module->placement_find_closest(obj, replication, server_idxs, placement_num_servers));
}

void placement_create_striped(unsigned long file_size, 
  unsigned int replication, unsigned int max_stripe_width, 
  unsigned int strip_size,
  unsigned int* num_objects,
  uint64_t *oids, unsigned long *sizes)
{
    return(module->placement_create_striped(file_size, replication,
        max_stripe_width, strip_size, num_objects, oids, sizes));
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
