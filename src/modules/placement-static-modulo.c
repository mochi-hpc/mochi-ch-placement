/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

/* NOTE NOTE NOTE NOTE
 * this is *NOT* a consistent hash
 *
 * It uses an API compatible to the other modules in this library, but does
 * not readily support adding or removing servers; the entire mapping would
 * need to be recalculated on any change in server membership
 */

#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "ch-placement.h"
#include "src/modules/placement-mod.h"
#include "src/lookup3.h"

static struct placement_mod* placement_mod_static_modulo(int n_svrs, int virt_factor, int seed);
static void placement_find_closest_static_modulo(struct placement_mod *mod, uint64_t obj, unsigned int replication, 
    unsigned long *server_idxs);
static void placement_finalize_static_modulo(struct placement_mod *mod);

struct placement_mod_map static_modulo_mod_map = 
{
    .type = "static_modulo",
    .initiate = placement_mod_static_modulo,
};

struct static_modulo_state
{
    unsigned int n_svrs;
};

struct placement_mod* placement_mod_static_modulo(int n_svrs, int virt_factor, int seed)
{
    struct placement_mod *mod_static_modulo;
    struct static_modulo_state *mod_state;

    /* NOTE: this placement algorithm will not benefit from virtual nodes;
     * ignore that parameter
     */
    (void)virt_factor;

    mod_static_modulo = malloc(sizeof(*mod_static_modulo));
    if(!mod_static_modulo)
        return(NULL);

    mod_state = malloc(sizeof(*mod_state));
    if(!mod_state)
    {
        free(mod_static_modulo);
        return(NULL);
    }

    mod_static_modulo->data = mod_state;

    mod_state->n_svrs = n_svrs;

    mod_static_modulo->find_closest = placement_find_closest_static_modulo;
    mod_static_modulo->create_striped = placement_create_striped_random;
    mod_static_modulo->finalize = placement_finalize_static_modulo;

    return(mod_static_modulo);
}

static void placement_find_closest_static_modulo(struct placement_mod *mod, uint64_t obj, unsigned int replication, 
    unsigned long* server_idxs)
{
    struct static_modulo_state *mod_state = mod->data;
    uint32_t h1 = 0;
    uint32_t h2 = 0;
    uint64_t hashed_obj;
    int i;

    /* hash incoming object id (this is like a pre conditioner so that we
     * balance load even if id space is not well distributed)
     */
    ch_bj_hashlittle2(&obj, sizeof(obj), &h1, &h2);
    hashed_obj  = h1 + (((uint64_t)h2)<<32);

    /* modulo to get first server, increment from there, with modulo to wrap
     * around
     */
    server_idxs[0] = hashed_obj % mod_state->n_svrs;
    for(i=1; i<replication; i++)
        server_idxs[i] = (server_idxs[i-1] + 1) % mod_state->n_svrs;

    return;
}

static void placement_finalize_static_modulo(struct placement_mod *mod)
{
    struct static_modulo_state *mod_state = mod->data;

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
