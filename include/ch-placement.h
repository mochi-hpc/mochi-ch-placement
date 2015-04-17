/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef CH_PLACEMENT_H
#define CH_PLACEMENT_H

#include <stdint.h>

/* maximum replication factor allowed by library */
#define CH_MAX_REPLICATION 5

struct ch_placement_instance;

struct ch_placement_instance* ch_placement_initialize(const char* name, 
    int n_svrs, int virt_factor);

uint64_t ch_placement_random_u64(void);

#if 0
/* set placement parameters: placement type and number of servers */
void ch_placement_set(char* type, unsigned int num_servers);

/* calculate the N closest servers to a given object */
void ch_placement_find_closest(uint64_t obj, unsigned int replication, 
    unsigned long* server_idxs);

/* generate a set of OIDs for striping a file */
void ch_placement_create_striped(unsigned long file_size, 
  unsigned int replication, unsigned int max_stripe_width, 
  unsigned int strip_size,
  unsigned int* num_objects,
  uint64_t *oids, unsigned long *sizes);

#endif

#endif /* CH_PLACEMENT_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
