/*
 * Copyright (C) 2013 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <limits.h>

#include "ch-placement-oid-gen.h"
#include "ch-placement.h"

#define PRINT_PROGRESS 1

static int bin_cmp(const void* key, const void *member);

static void oid_gen_random(
    struct ch_placement_instance *instance,
    unsigned int max_objs, 
    long unsigned int max_bytes, 
    unsigned int random_seed, 
    unsigned int replication,
    unsigned int num_servers,
    const char* params,
    uint64_t* total_byte_count,
    unsigned long* total_objs_count,
    struct obj** total_objs);

static void oid_gen_hist_stripe(
    struct ch_placement_instance *instance,
    unsigned int max_objs, 
    long unsigned int max_bytes, 
    unsigned int random_seed, 
    unsigned int replication,
    unsigned int num_servers,
    const char* params,
    uint64_t* total_byte_count,
    unsigned long* total_objs_count,
    struct obj** total_objs);

static void oid_gen_hist_hadoop(
    struct ch_placement_instance *instance,
    unsigned int max_objs, 
    long unsigned int max_bytes, 
    unsigned int random_seed, 
    unsigned int replication,
    unsigned int num_servers,
    const char* params,
    uint64_t* total_byte_count,
    unsigned long* total_objs_count,
    struct obj** total_objs);

static void oid_gen_basic(
    struct ch_placement_instance *instance,
    unsigned int max_objs, 
    long unsigned int max_bytes, 
    unsigned int random_seed, 
    unsigned int replication,
    unsigned int num_servers,
    const char* params,
    uint64_t* total_byte_count,
    unsigned long* total_objs_count,
    struct obj** total_objs);
static int obj_cmp(const void* a, const void *b);

void oid_sort(struct obj* objs, unsigned int objs_count)
{
    qsort(objs, objs_count, sizeof(*objs), obj_cmp);

    return;
}

void oid_randomize(struct obj* objs, unsigned int objs_count, unsigned int seed)
{
    unsigned int i;
    unsigned int j;
    struct obj tmp_obj;

    srandom(seed);

    if (objs_count > 1) {
        for (i = 0; i < objs_count - 1; i++) {
            j = i + rand() / (RAND_MAX / (objs_count - i) + 1);
            tmp_obj = objs[j];
            objs[j] = objs[i];
            objs[i] = tmp_obj;
        }
    }

    return;
}


/* generates an array of objects to populate the system */
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
    struct obj** total_objs)
{
    if(strcmp(gen_name, "random") == 0)
        oid_gen_random(instance, max_objs, max_bytes, random_seed, replication,
            num_servers, params,
            total_byte_count, total_obj_count, total_objs);
    else if(strcmp(gen_name, "basic") == 0)
        oid_gen_basic(instance, max_objs, max_bytes, random_seed, replication,
            num_servers, params,
            total_byte_count, total_obj_count, total_objs);
    else if(strcmp(gen_name, "hist_stripe") == 0)
        oid_gen_hist_stripe(instance, max_objs, max_bytes, random_seed, replication,
            num_servers, params,
            total_byte_count, total_obj_count, total_objs);
    else if(strcmp(gen_name, "hist_hadoop") == 0)
        oid_gen_hist_hadoop(instance, max_objs, max_bytes, random_seed, replication,
            num_servers, params,
            total_byte_count, total_obj_count, total_objs);
    else
        assert(0);

    return;
}

static void oid_gen_random(
    struct ch_placement_instance *instance,
    unsigned int max_objs, 
    long unsigned int max_bytes, 
    unsigned int random_seed, 
    unsigned int replication,
    unsigned int num_servers,
    const char* params,
    uint64_t* total_byte_count,
    unsigned long* total_objs_count,
    struct obj** total_objs)
{
    uint64_t byte_count = 0;
    unsigned int i;
    unsigned r;
#if PRINT_PROGRESS
    unsigned long progress = 0;
    unsigned long prog_cnt = max_objs / 10ul + (max_objs%10ul > 0);
    int percentage = 0;
#endif

    /* generate random object with sizes from 1 byte to 16 MiB until we hit 
     * either the limit on the number of objects or the limit on size
     * (not counting replicas)
     */

    /* allocate an array large enough to hold every object id; after we are
     * done then we can realloc it down to a smaller size.
     */
    *total_objs = malloc(max_objs*sizeof(**total_objs));
    assert(*total_objs);
    *total_objs_count = 0;

    srand(random_seed);

#if PRINT_PROGRESS
    printf("# Progress: 0%%");
    fflush(stdout);
#endif
    while(*total_objs_count < max_objs && byte_count < max_bytes)
    {
#if PRINT_PROGRESS
        if (*total_objs_count - progress >= prog_cnt){
            progress += prog_cnt;
            percentage += 10;
            printf("...%d%%", percentage);
            fflush(stdout);
        }
#endif
        (*total_objs)[*total_objs_count].oid = ch_placement_random_u64();
        r = random();
        (*total_objs)[*total_objs_count].size = r % (1024*1024*16);
        (*total_objs)[*total_objs_count].replication = replication;
        byte_count += (*total_objs)[*total_objs_count].size;
        (*total_objs_count)++;
    }
#if PRINT_PROGRESS
    printf("...100%%\n");
    fflush(stdout);
#endif

    /* sort and filter out duplicate OIDs */
    oid_sort(*total_objs, *total_objs_count);

    for(i=0; i<((*total_objs_count)-1); i++)
    {
        while((*total_objs)[i].oid == (*total_objs)[i+1].oid)
        {
            byte_count -= (*total_objs)[i].size;
            memmove(&((*total_objs)[i]), &((*total_objs)[i+1]), ((*total_objs_count)-i-1)*sizeof(**total_objs));
            (*total_objs_count)--;
        }
    }

    *total_byte_count = byte_count;

    return;
}

static int obj_cmp(const void* a, const void *b)
{
    const struct obj *o_a = a;
    const struct obj *o_b = b;

    if(o_a->oid < o_b->oid)
        return(-1);
    else if(o_a->oid > o_b->oid)
        return(1);
    else
        return(0);
}

struct bin
{
    unsigned long min;
    unsigned long max;
    unsigned long count;
    double cumu_percentage;
};

/* generates objects in striped sets based on a file size histogram.
 */
/* TODO: refactor to share code with hist_hadoop */
static void oid_gen_hist_stripe(
    struct ch_placement_instance *instance,
    unsigned int max_objs, 
    long unsigned int max_bytes, 
    unsigned int random_seed, 
    unsigned int replication,
    unsigned int num_servers,
    const char* params,
    uint64_t* total_byte_count,
    unsigned long* total_objs_count,
    struct obj** total_objs)
{
    unsigned long strip_size = 0;
    char hist_file[PATH_MAX] = {0};
    char* param;
    int ret;
    char* dup_params;
    char* parse_params;
    FILE* hist;
    char buffer[512];
    struct bin *bin_array;
    int nbins = 0;
    int i;
    unsigned long total_count = 0;
    unsigned long running_count = 0;
    uint64_t r;
    double r_float;
    struct bin *r_bin;
    uint64_t size;
    uint64_t *oids;
    unsigned long *sizes;
    unsigned int num_oids;
#if 0
    int *server_overlaps;
    int max_overlap;
    int j;
#endif
    unsigned long *server_idxs;
    unsigned long file_count = 0;

#if PRINT_PROGRESS
    unsigned long progress = 0;
    unsigned long prog_cnt = max_objs / 10ul + (max_objs%10ul > 0);
    int percentage = 0;
#endif

    *total_objs = malloc(max_objs*sizeof(**total_objs));
    assert(*total_objs);
    *total_objs_count = 0;
    *total_byte_count = 0;

    bin_array = malloc(50*sizeof(*bin_array));
    assert(bin_array);

    oids = malloc((num_servers/replication)*sizeof(*oids));
    assert(oids);
    sizes = malloc((num_servers/replication)*sizeof(*sizes));
    assert(sizes);
#if 0
    /* used to track how many servers overlap objects after striping */
    server_overlaps = malloc(num_servers*sizeof(*server_overlaps));
    assert(server_overlaps);
#endif
    server_idxs = malloc(replication*sizeof(*server_idxs));
    assert(server_idxs);


    /* parameters are mandatory for this function; expected in the format:
     * "strip_size:4194304,hist_file:mira-histo.dat"
     */
    assert(params);

    printf("# hist_stripe() params: %s\n", params);
    
    dup_params = strdup(params);
    assert(dup_params);
    parse_params = dup_params;

    /* look through comma delimited list of parameters */
    param = strtok(parse_params, ",");
    while(param)
    {
        ret = 0;
        /* try each possibly parameter format; expect only one to succeed
         * each round.
         */
        ret += sscanf(param, "strip_size:%lu", &strip_size);
        ret += sscanf(param, "hist_file:%s", hist_file);

        assert(ret == 1);

        param = strtok(NULL, ",");
    }
    free(dup_params);

    assert(strip_size > 0);

    hist = fopen(hist_file, "r");
    if(!hist)
    {
        perror("fopen");
        assert(0);
    }

    while(fgets(buffer, 512, hist))
    {
        sscanf(buffer, "# total_count: %lu", &total_count);

        if(buffer[0] == '#' || buffer[0] == '\n')
            continue;

        assert(total_count > 0);

        ret = sscanf(buffer, "%lu %lu %lu", &bin_array[nbins].min,
            &bin_array[nbins].max,
            &bin_array[nbins].count);
        if(ret == 3)
        {
            /* generate cumulative percentage for each bin */
            running_count += bin_array[nbins].count;
            bin_array[nbins].cumu_percentage = 
                (double)running_count/(double)total_count;
            nbins++;
        }
        else
        {
            assert(0);
        }

        assert(nbins < 50);
    }
    fclose(hist);
    printf("# read in %d bins from %s\n", nbins, hist_file);
    for(i=0; i<nbins; i++)
    {
        printf("%lu\t%lu\t%lu\t%f\n", bin_array[i].min, bin_array[i].max, bin_array[i].count, bin_array[i].cumu_percentage);
    }

#if PRINT_PROGRESS
    printf("# Progress: 0%%");
    fflush(stdout);
#endif
    //printf("# ol:\t<file size>\t<stripe width>\t<max overlap>\n");
    while(*total_objs_count < max_objs && *total_byte_count < max_bytes)
    {
#if PRINT_PROGRESS
        if (*total_objs_count - progress >= prog_cnt){            
            progress += prog_cnt;
            percentage += 10;
            printf("...%d%%", percentage);
            fflush(stdout);
        }
#endif
        /* generate random floating point number between 0 and 1 */
        r = ch_placement_random_u64();
        r_float = (double)r/(double)UINT64_MAX;

        /* find matching bin in weighted histogram */
        r_bin = bsearch(&r_float, bin_array, nbins, sizeof(*bin_array), bin_cmp);
        /* randomize size within bin */
        r = ch_placement_random_u64();
        r = r % ((uint64_t)r_bin->max - (uint64_t)r_bin->min);
        size = r_bin->min + r;
        if(size == 0)
            size = 1;

        //printf("FOO: r_float: %f, size: %lu\n", r_float, size);

        /* did we hit the byte limit? */
        if(*total_byte_count + size > max_bytes)
            break;

        /* create striped set */
        ch_placement_create_striped(instance, size, replication, (num_servers/replication),
            strip_size, &num_oids, oids, sizes);
        
        /* did we hit the object count limit? */
        if(*total_objs_count + num_oids > max_objs)
            break;

        file_count++;
        /* add objects to list */
        for(i=0; i<num_oids; i++)
        {
            (*total_objs)[*total_objs_count].oid = oids[i];
            (*total_objs)[*total_objs_count].replication = replication;
            assert(sizes[i] > 0);
            (*total_objs)[*total_objs_count].size = sizes[i];
            (*total_objs_count)++;
        }
        (*total_byte_count) += size;

#if 0
        /* calculate placement for each object to detect overlaps */
        memset(server_overlaps, 0, num_servers*sizeof(*server_overlaps));
        for(i=0; i<num_oids; i++)
        {
           placement_find_closest(oids[i], replication, server_idxs);
           for(j=0; j<replication; j++)
           {
              server_overlaps[server_idxs[j]]++;
           }
        }
        max_overlap = 0;
        /* find maximum overlap */
        for(i=0; i<num_servers; i++)
        {
           if(server_overlaps[i] > max_overlap)
              max_overlap = server_overlaps[i];
        }
        //printf("ol:\t%lu\t%u\t%d\n", size, num_oids, max_overlap);
#endif
    }
#if PRINT_PROGRESS
    printf("...100%%\n");
    fflush(stdout);
#endif
    printf("# Produced %lu files.\n", file_count);

    free(bin_array);
    free(oids);
    free(sizes);
#if 0
    free(server_overlaps);
#endif
    free(server_idxs);

    return;
}

/* generates objects in sets according to default hdfs chunking method
 * (divide into 64 MiB objects)
 */
/* TODO: refactor to share code with hist_stripe */
static void oid_gen_hist_hadoop(
    struct ch_placement_instance *instance,
    unsigned int max_objs, 
    long unsigned int max_bytes, 
    unsigned int random_seed, 
    unsigned int replication,
    unsigned int num_servers,
    const char* params,
    uint64_t* total_byte_count,
    unsigned long* total_objs_count,
    struct obj** total_objs)
{
    unsigned long strip_size = 0;
    char hist_file[PATH_MAX] = {0};
    char* param;
    int ret;
    char* dup_params;
    char* parse_params;
    FILE* hist;
    char buffer[512];
    struct bin *bin_array;
    int nbins = 0;
    int i;
    unsigned long total_count = 0;
    unsigned long running_count = 0;
    uint64_t r;
    double r_float;
    struct bin *r_bin;
    uint64_t size;
    unsigned int num_oids;
    unsigned long file_count = 0;

#if PRINT_PROGRESS
    unsigned long progress = 0;
    unsigned long prog_cnt = max_objs / 10ul + (max_objs%10ul > 0);
    int percentage = 0;
#endif

    *total_objs = malloc(max_objs*sizeof(**total_objs));
    assert(*total_objs);
    *total_objs_count = 0;
    *total_byte_count = 0;

    bin_array = malloc(50*sizeof(*bin_array));
    assert(bin_array);

    /* parameters are mandatory for this function; expected in the format:
     * "strip_size:4194304,hist_file:mira-histo.dat"
     */
    assert(params);

    printf("# hist_hadoop() params: %s\n", params);
    
    dup_params = strdup(params);
    assert(dup_params);
    parse_params = dup_params;

    /* look through comma delimited list of parameters */
    param = strtok(parse_params, ",");
    while(param)
    {
        ret = 0;
        /* try each possibly parameter format; expect only one to succeed
         * each round.
         */
        ret += sscanf(param, "strip_size:%lu", &strip_size);
        ret += sscanf(param, "hist_file:%s", hist_file);

        assert(ret == 1);

        param = strtok(NULL, ",");
    }
    free(dup_params);

    assert(strip_size > 0);

    hist = fopen(hist_file, "r");
    if(!hist)
    {
        perror("fopen");
        assert(0);
    }

    while(fgets(buffer, 512, hist))
    {
        sscanf(buffer, "# total_count: %lu", &total_count);

        if(buffer[0] == '#' || buffer[0] == '\n')
            continue;

        assert(total_count > 0);

        ret = sscanf(buffer, "%lu %lu %lu", &bin_array[nbins].min,
            &bin_array[nbins].max,
            &bin_array[nbins].count);
        if(ret == 3)
        {
            /* generate cumulative percentage for each bin */
            running_count += bin_array[nbins].count;
            bin_array[nbins].cumu_percentage = 
                (double)running_count/(double)total_count;
            nbins++;
        }
        else
        {
            assert(0);
        }

        assert(nbins < 50);
    }
    fclose(hist);
    printf("# read in %d bins from %s\n", nbins, hist_file);
    for(i=0; i<nbins; i++)
    {
        printf("%lu\t%lu\t%lu\t%f\n", bin_array[i].min, bin_array[i].max, bin_array[i].count, bin_array[i].cumu_percentage);
    }

#if PRINT_PROGRESS
    printf("# Progress: 0%%");
    fflush(stdout);
#endif
    while(*total_objs_count < max_objs && *total_byte_count < max_bytes)
    {
#if PRINT_PROGRESS
        if (*total_objs_count - progress >= prog_cnt){            
            progress += prog_cnt;
            percentage += 10;
            printf("...%d%%", percentage);
            fflush(stdout);
        }
#endif
        /* generate random floating point number between 0 and 1 */
        r = ch_placement_random_u64();
        r_float = (double)r/(double)UINT64_MAX;

        /* find matching bin in weighted histogram */
        r_bin = bsearch(&r_float, bin_array, nbins, sizeof(*bin_array), bin_cmp);
        assert(r_bin);

        /* randomize size within bin */
        r = ch_placement_random_u64();
        r = r % ((uint64_t)r_bin->max - (uint64_t)r_bin->min);
        size = r_bin->min + r;
        if(size == 0)
            size = 1;

        //printf("FOO: r_float: %f, size: %lu\n", r_float, size);

        /* did we hit the byte limit? */
        if(*total_byte_count + size > max_bytes)
            break;

        /* divide into randomly generated objects of at most strip_size
         * each.
         */
        num_oids = size/strip_size;
        if(size % strip_size)
            num_oids++;
  
        /* did we hit the object count limit? */
        if(*total_objs_count + num_oids > max_objs)
            break;

        file_count++;
       
        /* add objects to list */
        for(i=0; i<num_oids; i++)
        {
            (*total_objs)[*total_objs_count].oid = ch_placement_random_u64();
            (*total_objs)[*total_objs_count].replication = replication;
            if(i == (num_oids-1) && size % strip_size)
                (*total_objs)[*total_objs_count].size = size % strip_size;
            else
                (*total_objs)[*total_objs_count].size = strip_size;
            //printf("FOO: file size %lu, obj size: %lu\n", size, (*total_objs)[*total_objs_count].size);
            (*total_objs_count)++;
        }
        (*total_byte_count) += size;
    }
#if PRINT_PROGRESS
    printf("...100%%\n");
    fflush(stdout);
#endif

    printf("# Produced %lu files.\n", file_count);

    free(bin_array);

    return;
}

/* find the bin with the lowest cumulative percentage that is greater than
 * the key 
 */
static int bin_cmp(const void* key, const void *member)
{
    const double *r_float = key;
    const struct bin *mem_bin = member;
    const struct bin *lower_bin;
    
    if(*r_float > mem_bin->cumu_percentage)
        return(1);

    /* hit bottom */
    if(mem_bin->min == 0)
        return(0);
    
    /* this bin is a candidate, need to check bin below  it */
    lower_bin = mem_bin - 1;
    if(*r_float < lower_bin->cumu_percentage)
        return(-1);

    /* got it */
    return(0);
}


static void oid_gen_basic(
    struct ch_placement_instance *instance,
    unsigned int max_objs, 
    long unsigned int max_bytes, 
    unsigned int random_seed, 
    unsigned int replication,
    unsigned int num_servers,
    const char* params,
    uint64_t* total_byte_count,
    unsigned long* total_objs_count,
    struct obj** total_objs)
{
    unsigned int i;

    /* Simple case for testing.  Three objs with replication level 2, total
     * volume of data (not counting replication) slightly over 400 GIB */

    *total_objs = malloc(max_objs*sizeof(**total_objs));
    assert(*total_objs);
    *total_objs_count = max_objs;
    *total_byte_count = 0;

    for(i=0; i<max_objs; i++)
    {
        (*total_objs)[i].oid = i+1;
        (*total_objs)[i].replication = replication;
        (*total_objs)[i].size = max_bytes / max_objs;
        (*total_byte_count) += max_bytes / max_objs;
    }
    
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
