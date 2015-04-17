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
#include <mapper.h>


static void placement_find_closest_crush(struct placement_mod *mod, uint64_t obj, unsigned int replication, 
 unsigned long *server_idxs);
static void placement_finalize_crush(struct placement_mod *mod);

struct crush_state
{
    struct crush_map *map;
    __u32 *weight;
    int n_weight;
};

struct placement_mod* placement_mod_crush(struct crush_map *map, __u32 *weight, int n_weight)
{
    struct placement_mod *mod_crush;
    struct crush_state *mod_state;

    mod_crush = malloc(sizeof(*mod_crush));
    if(!mod_crush)
        return(NULL);

    mod_state = malloc(sizeof(*mod_state));
    if(!mod_state)
    {
        free(mod_crush);
        return(NULL);
    }

    mod_crush->data = mod_state;
    mod_state->map = map;
    mod_state->weight = weight;
    mod_state->n_weight = n_weight;

    mod_crush->find_closest = placement_find_closest_crush;
    mod_crush->create_striped = placement_create_striped_random;
    mod_crush->finalize = placement_finalize_crush;

    return(mod_crush);
}

static void placement_find_closest_crush(struct placement_mod *mod, uint64_t obj, unsigned int replication, 
    unsigned long* server_idxs)
{
    struct crush_state *mod_state = mod->data;
    int ret;
    int result[CH_MAX_REPLICATION];
    int scratch[CH_MAX_REPLICATION*3];
    int i;

    ret = crush_do_rule(mod_state->map, 0, obj, result, replication, 
        mod_state->weight, mod_state->n_weight, scratch);
    assert(ret == replication);

    for(i=0; i<replication; i++)
        server_idxs[i] = result[i];

    return;
}

static void placement_finalize_crush(struct placement_mod *mod)
{
    struct crush_state *mod_state = mod->data;

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
