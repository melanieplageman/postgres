/*-------------------------------------------------------------------------
 *
 * streaming_write.h
 *	  Helper to efficiently perform bulk writes.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/streaming_write.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STREAMING_WRITE_H
#define STREAMING_WRITE_H

struct PgAioInProgress;

typedef struct PgStreamingWrite PgStreamingWrite;
typedef bool (*PgStreamingWriteRetry) (PgStreamingWrite *pgsw, void *pgsw_private, struct PgAioInProgress *io, void *write_private);
typedef void (*PgStreamingWriteCompleted) (PgStreamingWrite *pgsw, void *pgsw_private, int result, void *write_private);

extern PgStreamingWrite *pg_streaming_write_alloc(uint32 iodepth, void *private_data);
extern struct PgAioInProgress *pg_streaming_write_get_io(PgStreamingWrite *pgsw);
extern void pg_streaming_write_release_io(PgStreamingWrite *pgsw, struct PgAioInProgress *io);
extern uint32 pg_streaming_write_inflight(PgStreamingWrite *pgsw);
extern void pg_streaming_write_write(PgStreamingWrite *pgsw, struct PgAioInProgress *io,
									 PgStreamingWriteCompleted on_completion,
									 PgStreamingWriteRetry on_failure,
									 void *private_data);
extern void pg_streaming_write_wait_all(PgStreamingWrite *pgsw);
extern void pg_streaming_write_free(PgStreamingWrite *pgsw);


#endif							/* STREAMING_WRITE_H */
