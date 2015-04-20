/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <stdint.h>
#include "SpookyV2.h"

extern "C" {
#include "spooky.h"
}

extern "C"
uint64_t spooky_hash64(
        const void *message, 
        size_t length, 
        uint64_t seed){
    return SpookyHash::Hash64(message, length, seed);
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */

