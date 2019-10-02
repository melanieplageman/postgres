/*-------------------------------------------------------------------------
 *
 * sharedtuplestore.h
 *	  Simple mechanism for sharing tuples between backends.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/sharedtuplestore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHAREDTUPLESTORE_H
#define SHAREDTUPLESTORE_H

#include "access/htup.h"
#include "nodes/execnodes.h"
#include "storage/fd.h"
#include "storage/sharedfileset.h"

struct SharedTuplestore;
typedef struct SharedTuplestore SharedTuplestore;

struct SharedTuplestoreAccessor;
typedef struct SharedTuplestoreAccessor SharedTuplestoreAccessor;
struct tupleMetadata;
typedef struct tupleMetadata tupleMetadata;
struct tupleMetadata
{
	uint32 hashvalue;
	int tuplenum;
} __attribute__((packed));

/*
 * A flag indicating that the tuplestore will only be scanned once, so backing
 * files can be unlinked early.
 */
#define SHARED_TUPLESTORE_SINGLE_PASS 0x01

extern size_t sts_estimate(int participants);

extern SharedTuplestoreAccessor *sts_initialize(SharedTuplestore *sts,
												int participants,
												int my_participant_number,
												size_t meta_data_size,
												int flags,
												SharedFileSet *fileset,
												const char *name);

extern SharedTuplestoreAccessor *sts_attach(SharedTuplestore *sts,
											int my_participant_number,
											SharedFileSet *fileset);

extern void sts_end_write(SharedTuplestoreAccessor *accessor);

extern void sts_reinitialize(SharedTuplestoreAccessor *accessor);

extern void sts_begin_parallel_scan(SharedTuplestoreAccessor *accessor);

extern void sts_end_parallel_scan(SharedTuplestoreAccessor *accessor);

// TODO: this API is not great -- passing count_tuples flag. However, I don't want to count tuples for the inner side
extern void sts_puttuple(SharedTuplestoreAccessor *accessor, void *meta_data, MinimalTuple tuple, bool count_tuples);

extern uint32 sts_gettuplenum(SharedTuplestoreAccessor *accessor);

extern int sts_get_my_participant_number(SharedTuplestoreAccessor *accessor);
extern void sts_make_STA_outerMatchStatuses(SharedTuplestoreAccessor *accessor, int batchno, char *name);
extern BufFile *sts_get_my_STA_outerMatchStatuses(SharedTuplestoreAccessor *accessor);

extern MinimalTuple sts_parallel_scan_next(SharedTuplestoreAccessor *accessor, void *meta_data, bool is_outer);

extern void sts_cleanup_STA_outer_match_status_files(SharedTuplestoreAccessor *accessor);

extern void sts_bitmap_filename(char *name, SharedTuplestoreAccessor *accessor, int participant);
extern SharedFileSet *sts_get_fileset(SharedTuplestoreAccessor *accessor);

extern void
print_tuplenums(SharedTuplestoreAccessor *accessor, int batchno, BufFile *combined_bitmap_file,
				BufFile **outer_read_files, int num_outer_read_files);
extern void
combine_outer_match_statuses(SharedTuplestoreAccessor *accessor, BufFile *outer_match_statuses[], int length, size_t num_bytes, int batchno, BufFile **combined_bitmap_file);
extern void
populate_outer_match_statuses(SharedTuplestoreAccessor *accessor, BufFile *outer_match_statuses[], char **outer_match_status_filenames);
extern void
close_outer_match_statuses(SharedTuplestoreAccessor *accessor, BufFile *outer_match_statuses[], int length);

extern
void sts_rewind_all_outer_files(SharedTuplestoreAccessor *accessor, BufFile **outer_read_files, int length);

#endif							/* SHAREDTUPLESTORE_H */
