/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifndef COMB_H
#define COMB_H

#include <limits.h>
#include <stdint.h>

static inline uint64_t choose(uint64_t n, uint64_t k){
    uint64_t res = 1, i;
    for (i = 1; i <= k; i++){
        res = (res*(n+1-i)) / i;
    }
    return res;
}

/* Returns canonical linear index in (N choose k) for arbitrary N
 * The ordering is descending with respect to the reverse
 * lexicographic order of the sets.
 * Example: 4 choose 2
 *          sets: {3,2}, {3,1}, {3,0}, {2,1}, {2,0}, {1,0}
 *       indexes:     5,     4,     3,     2,     1,     0
 * k = 3, comb = {4,2,1}
 * index = 4 choose 3 + 2 choose 2 + 1 choose 1 = 7
 * NOTE: Function expects inputs in descending order */
static inline uint64_t comb_index(unsigned long k, unsigned long *vals){
    unsigned long i, res=0;
    /* step in reverse order */
    for (i = 0; i < k; i++){
        res += choose(vals[i],k-i);
    }
    return res;
}

/* sorts in descending order the set of numbers needed for the combination 
 * calculation */
static inline void rev_ins_sort(unsigned int n, uint64_t *vals){
    unsigned int i, j;
    for (i = 1; i < n; i++){
        uint64_t val = vals[i];
        for (j = i; j > 0 && val > vals[j-1]; j--){
            vals[j]=vals[j-1];
        }
        vals[j] = val;
    }
}

#endif /* end of include guard: COMB_H */

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */
