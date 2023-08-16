/*-------------------------------------------------------------------------
 *
 * streaming_read.h
 *	  streaming_read
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/streaming_read.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STREAMING_READ_H
#define STREAMING_READ_H

#include "storage/bufmgr.h"

/*
 * Helper for efficient reads (using readahead).
 */
struct PgAioInProgress;

typedef struct PgStreamingRead PgStreamingRead;
typedef enum PgStreamingReadNextStatus
{
	PGSR_NEXT_END,
	PGSR_NEXT_NO_IO,
	PGSR_NEXT_IO
} PgStreamingReadNextStatus;

typedef PgStreamingReadNextStatus (*PgStreamingReadDetermineNextCB)
			(PgStreamingRead *pgsr, uintptr_t pgsr_private,
			 struct PgAioInProgress *aio, void *read_private);
typedef void (*PgStreamingReadRelease) (uintptr_t pgsr_private, void *read_private);
extern PgStreamingRead *pg_streaming_read_alloc(uint32 iodepth,
												uint32 per_io_private_size,
												uintptr_t pgsr_private,
												PgStreamingReadDetermineNextCB determine_next_cb,
												PgStreamingReadRelease release_cb);
extern void pg_streaming_read_free(PgStreamingRead *pgsr);
extern void *pg_streaming_read_get_next(PgStreamingRead *pgsr);

/*
 * A layer ontop a base PgStreamingRead that makes it easier to work with
 * buffers. The callback just returns a block number and sets the relation /
 * fork output parameters, which is used to do a buffer lookup and start IO if
 * necessary. If InvalidBlockNumber is returned, the streaming read ends.
 */
struct RelationData;
typedef BlockNumber (*PgStreamingReadBufferDetermineNextCB)
			(PgStreamingRead *pgsr,
			 uintptr_t pgsr_private,
			 void *io_private,
			 struct RelationData **rel, ForkNumber *forkNum, ReadBufferMode *mode);
extern PgStreamingRead *pg_streaming_read_buffer_alloc(uint32 iodepth,
													   uint32 per_io_private_size,
													   uintptr_t pgsr_private,
													   BufferAccessStrategy strategy,
													   PgStreamingReadBufferDetermineNextCB determine_next_cb);
extern Buffer pg_streaming_read_buffer_get_next(PgStreamingRead *pgsr, void **io_private);

#endif							/* STREAMING_READ_H */
