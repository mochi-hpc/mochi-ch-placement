/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef CH_PLACEMENT_CRUSH_H
#define CH_PLACEMENT_CRUSH_H

#include <stdint.h>
#include <ch-placement.h>
#include <builder.h>

/* special case initialization for using a crush map instead of one of the
 * build in modules.  Requires that the crush map (complete with rule 0) 
 * be constructed externally and passed in.
 */

struct ch_placement_instance* ch_placement_initialize_crush(struct crush_map *map, __u32 *weight, int n_weight);

#endif /* CH_PLACEMENT_CRUSH_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
