/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef SPOOKY_H

#define SPOOKY_H

#include <stdint.h>
#include <stddef.h>

uint64_t spooky_hash64(
        const void *message, 
        size_t length, 
        uint64_t seed);

#endif /* end of include guard: SPOOKY_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */

