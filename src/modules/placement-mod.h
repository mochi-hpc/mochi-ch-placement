/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef PLACEMENT_MOD_H
#define PLACEMENT_MOD_H

#include <stdint.h>

struct placement_mod
{
    void (*find_closest)(uint64_t obj, unsigned int replication, 
        unsigned long* server_idxs);
    void (*create_striped)(unsigned long file_size, 
      unsigned int replication, unsigned int max_stripe_width, 
      unsigned int strip_size,
      unsigned int* num_objects,
      uint64_t *oids, unsigned long *sizes);
    void *data;
};

struct placement_mod_map
{
    char* type;
    struct placement_mod* (*initiate)(int n_svrs, int virt_factor);
};

/* generic striping function; just allocates random oids */
void placement_create_striped_random(unsigned long file_size, 
  unsigned int replication, unsigned int max_stripe_width, 
  unsigned int strip_size,
  unsigned int* num_objects,
  uint64_t *oids, unsigned long *sizes);


#endif /* PLACEMENT_MOD_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
