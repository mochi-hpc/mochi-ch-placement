/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef OID_GEN_H
#define OID_GEN_H

#include <stdint.h>
#include <ch-placement.h>

#ifdef __cplusplus
extern "C" {
#endif

/* describes an object */
struct obj
{
    uint64_t oid;          /* identifier */
    unsigned int replication;  /* replication factor */
    uint64_t size;         /* size of object */
    unsigned long server_idxs[CH_MAX_REPLICATION]; /* cached placement data */
};

/* generates a random array of object IDs for testing/evaluation */
void oid_gen(char* gen_name, 
    struct ch_placement_instance *instance,
    unsigned int max_objs, 
    long unsigned int max_bytes, 
    unsigned int random_seed, 
    unsigned int replication,
    unsigned int num_servers,
    const char* params,
    uint64_t* total_byte_count,
    unsigned long* total_obj_count,
    struct obj** total_objs);

void oid_sort(struct obj* objs, unsigned int objs_count);

void oid_randomize(struct obj* objs, unsigned int objs_count, unsigned int seed);

#ifdef __cplusplus
}
#endif

#endif /* OID_GEN_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
