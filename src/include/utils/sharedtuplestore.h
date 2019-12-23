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
// TODO: can I use something less wasteful?
struct tupleMetadata;
typedef struct tupleMetadata tupleMetadata;
struct tupleMetadata
{
	uint32 hashvalue;
	int tupleid; /* tuple id on outer side and chunk number for inner side */
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

extern void sts_puttuple(SharedTuplestoreAccessor *accessor,
						 void *meta_data,
						 MinimalTuple tuple);

extern MinimalTuple sts_parallel_scan_next(SharedTuplestoreAccessor *accessor,
										   void *meta_data);






extern int sts_increment_tuplenum(SharedTuplestoreAccessor *accessor);
extern void sts_set_outer_match_status(SharedTuplestoreAccessor *accessor, uint32 tuplenum);
extern void sts_make_outer_match_status_file(SharedTuplestoreAccessor *accessor, char *name);
extern void sts_close_outer_match_status_file(SharedTuplestoreAccessor *accessor);
extern BufFile *sts_combine_outer_match_status_files(SharedTuplestoreAccessor *accessor);


#endif							/* SHAREDTUPLESTORE_H */
